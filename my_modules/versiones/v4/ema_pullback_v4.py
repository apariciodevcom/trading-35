"""
===========================================================================
 Estrategia: EMA Pullback - v4 (tuning integrado + debug) - LeanTech Trading
===========================================================================

Descripcion:
------------
Detecta rebotes al alza sobre la EMA20 en contexto de tendencia alcista.
La lógica incluye parámetros de tuning integrados y filtros configurables
basados en análisis de grid search (junio 2025).

Mejoras v4:
-----------
✅ Integra 4 parámetros clave:
   - usar_filtro_volatilidad
   - atr_threshold
   - rebote_required
   - tendencia_required
✅ Todos los filtros aplicados internamente, sin necesidad de lógica externa
✅ Soporta modo debug para inspección detallada

Mejor configuración validada (score 0.60):
------------------------------------------
- usar_filtro_volatilidad = True
- rebote_required = True
- tendencia_required = True
- atr_threshold = 0.010

Requiere:
---------
- Columnas: 'fecha', 'close', 'high', 'low'
- Librerías: pandas, ta

Salida:
-------
DataFrame con ['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("ema_pullback_v4")

def generar_senales(df: pd.DataFrame,
                    usar_filtro_volatilidad: bool = True,
                    atr_threshold: float = 0.010,
                    rebote_required: bool = True,
                    tendencia_required: bool = True,
                    debug: bool = True) -> pd.DataFrame:
    try:
        df = df.copy()

        # 1. Validar columnas necesarias
        req_cols = {"fecha", "close", "high", "low"}
        if not req_cols.issubset(df.columns):
            logger.warning(f"Columnas faltantes: {req_cols - set(df.columns)}")
            return df_as_hold(df, razon="faltan columnas")

        # 2. Ordenar por fecha
        df = df.sort_values("fecha").reset_index(drop=True)

        # 3. Validar longitud
        if len(df) < 25:
            return df_as_hold(df, razon="datos insuficientes")

        # 4. Indicadores base
        df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["tendencia_alcista"] = df["ema_20"] > df["ema_20"].shift(1)
        df["pullback"] = df["close"] < df["ema_20"] * 1.01
        df["rebote"] = df["close"] >= df["close"].shift(1)

        # 5. Condición inicial BUY
        df["buy_cond"] = df["pullback"]

        if tendencia_required:
            df["buy_cond"] &= df["tendencia_alcista"]
        if rebote_required:
            df["buy_cond"] &= df["rebote"]

        # 6. Filtro de volatilidad si se activa
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["buy_cond"] &= df["atr_ratio"] > atr_threshold

        # 7. Generar señales
        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df["prev_buy"] = pd.Series(df["buy_cond"].shift(1)).fillna(False).astype(bool)
        df["end_buy"] = df["prev_buy"] & ~df["buy_cond"]
        df.loc[df["end_buy"], "signal"] = "sell"

        # 8. Etiqueta
        df["estrategia"] = "ema_pullback_v4"

        # 9. Log resumen
        logger.info(f"EMA_Pullback_v4: filtro_vol={usar_filtro_volatilidad}, atr>{atr_threshold}, rebote={rebote_required}, tendencia={tendencia_required}, buy={sum(df['signal']=='buy')}, sell={sum(df['signal']=='sell')}")

        # 10. Columnas finales
        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["ema_20", "tendencia_alcista", "pullback", "rebote", "buy_cond"]
            if usar_filtro_volatilidad:
                columnas += ["atr", "atr_ratio"]
        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, razon="exception")

def df_as_hold(df: pd.DataFrame, razon: str) -> pd.DataFrame:
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "ema_pullback_v4"
    return df[["fecha", "signal", "estrategia"]]
