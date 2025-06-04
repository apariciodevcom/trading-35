"""
===========================================================================
 Estrategia: Cruce de Medias Móviles - v4 (afinada + debug)
===========================================================================

📌 Descripción:
Cruce entre EMA 10 y EMA 30 con validación de contexto para reducir falsos positivos.

✅ Confirmación al día siguiente
✅ Filtro por volatilidad (ATR Ratio)
✅ Validación de sesgo direccional con EMA200
✅ Depuración opcional (modo debug)

🎯 Parámetros optimizados:
Estos parámetros fueron seleccionados tras una grilla de afinamiento de 8 combinaciones
(posibles valores True/False para 3 booleanos), midiendo el rendimiento en:

- Winrate (tasa de acierto)
- Avg Profit (ganancia media por operación)
- Score (Winrate * Avg Profit)

🧪 Resultados del tuning:
- Winrate: ~51%
- Avg Profit: ~0.20
- Score: ~0.106
- Mejor combinación: ✅ todos los filtros activados

🛠️ Parámetros:
- usar_filtro_volatilidad: bool = True
- confirmar_al_dia_siguiente: bool = True
- usar_sesgo_tendencial: bool = True
- debug: bool = False

📤 Salida:
DataFrame con ['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("cruce_medias_v4")

def generar_senales(df: pd.DataFrame,
                    usar_filtro_volatilidad: bool = True,
                    confirmar_al_dia_siguiente: bool = True,
                    usar_sesgo_tendencial: bool = True,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        cols = {"fecha", "close", "high", "low"}
        if not cols.issubset(df.columns):
            logger.warning(f"Faltan columnas: {cols - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 50:
            return df_as_hold(df, "datos insuficientes")

        # EMAs
        df["ema_10"] = df["close"].ewm(span=10, adjust=False).mean()
        df["ema_30"] = df["close"].ewm(span=30, adjust=False).mean()
        df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()

        # Cruces básicos
        df["cruce_alcista"] = (df["ema_10"] > df["ema_30"]) & (df["ema_10"].shift(1) <= df["ema_30"].shift(1))
        df["cruce_bajista"] = (df["ema_10"] < df["ema_30"]) & (df["ema_10"].shift(1) >= df["ema_30"].shift(1))

        # Confirmación al día siguiente
        if confirmar_al_dia_siguiente:
            df["cruce_alcista"] &= df["ema_10"].shift(-1) > df["ema_30"].shift(-1)
            df["cruce_bajista"] &= df["ema_10"].shift(-1) < df["ema_30"].shift(-1)

        # Filtro de volatilidad
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > 0.01
            df["cruce_alcista"] &= df["vol_ok"]
            df["cruce_bajista"] &= df["vol_ok"]

        # Filtro de sesgo direccional
        if usar_sesgo_tendencial:
            df["cruce_alcista"] &= df["close"] > df["ema_200"]
            df["cruce_bajista"] &= df["close"] < df["ema_200"]

        # Señales finales
        df["signal"] = "hold"
        df.loc[df["cruce_alcista"], "signal"] = "buy"
        df.loc[df["cruce_bajista"], "signal"] = "sell"
        df["estrategia"] = "cruce_medias_v4"

        logger.info(f"Cruce Medias v4 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["ema_10", "ema_30", "ema_200", "cruce_alcista", "cruce_bajista"]
            if usar_filtro_volatilidad:
                columnas += ["atr", "atr_ratio", "vol_ok"]

        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, "exception")

def df_as_hold(df: pd.DataFrame, razon: str) -> pd.DataFrame:
    logger.info(f"HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "cruce_medias_v4"
    return df[["fecha", "signal", "estrategia"]]