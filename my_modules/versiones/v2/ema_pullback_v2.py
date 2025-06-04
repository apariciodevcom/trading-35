"""
===========================================================================
 Estrategia: EMA Pullback - v3 (parametrizable + debug) - LeanTech Trading
===========================================================================

Descripcion:
------------
Detecta rebotes al alza sobre la EMA20 en contexto de tendencia alcista,
con opciones configurables y salida mejorada.

Mejoras v3:
-----------
✅ Umbral de volatilidad (`atr_threshold`) parametrizable
✅ Salida por ruptura de condicion (como v2)
✅ Modo debug para retornar columnas auxiliares

Requiere:
---------
- Columnas: 'fecha', 'close', 'high', 'low'
- Librerias: pandas, ta

Salida:
-------
DataFrame con ['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("ema_pullback_v3")

def generar_senales(df: pd.DataFrame, usar_filtro_volatilidad: bool = True,
                     atr_threshold: float = 0.008, debug: bool = False) -> pd.DataFrame:
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

        # 4. Calculo de indicadores
        df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["tendencia_alcista"] = df["ema_20"] > df["ema_20"].shift(1)
        df["pullback"] = df["close"] < df["ema_20"] * 1.01
        df["rebote"] = df["close"] >= df["close"].shift(1)

        df["buy_cond"] = df["tendencia_alcista"] & df["pullback"] & df["rebote"]

        # 5. Volatilidad opcional
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["buy_cond"] &= df["atr_ratio"] > atr_threshold

        # 6. Señales BUY/SELL
        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df["prev_buy"] = df["buy_cond"].shift(1).fillna(False)
        df["end_buy"] = df["prev_buy"] & ~df["buy_cond"]
        df.loc[df["end_buy"], "signal"] = "sell"

        # 7. Etiqueta de estrategia
        df["estrategia"] = "ema_pullback_v3"

        # 8. Log resumen
        logger.info(f"Volatilidad>={atr_threshold}, buy={sum(df['signal']=='buy')}, sell={sum(df['signal']=='sell')}")

        # 9. Salida
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
    df["estrategia"] = "ema_pullback_v3"
    return df[["fecha", "signal", "estrategia"]]
