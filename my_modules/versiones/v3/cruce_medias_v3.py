"""
===========================================================================
 Estrategia: Cruce de Medias Móviles - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Cruce EMA 10/30 con opción de filtro por volatilidad y sesgo de tendencia.
✅ Confirmación al día siguiente opcional
✅ Filtro por ATR ratio
✅ Sesgo EMA200 opcional
✅ Modo debug

Parámetros:
-----------
- usar_filtro_volatilidad: bool
- confirmar_al_dia_siguiente: bool
- usar_sesgo_tendencial: bool
- debug: bool

Salida:
--------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("cruce_medias_v3")

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

        # Cruces
        df["cruce_alcista"] = (df["ema_10"] > df["ema_30"]) & (df["ema_10"].shift(1) <= df["ema_30"].shift(1))
        df["cruce_bajista"] = (df["ema_10"] < df["ema_30"]) & (df["ema_10"].shift(1) >= df["ema_30"].shift(1))

        # Confirmación día siguiente
        if confirmar_al_dia_siguiente:
            df["cruce_alcista"] &= df["ema_10"].shift(-1) > df["ema_30"].shift(-1)
            df["cruce_bajista"] &= df["ema_10"].shift(-1) < df["ema_30"].shift(-1)

        # ATR ratio
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > 0.01
            df["cruce_alcista"] &= df["vol_ok"]
            df["cruce_bajista"] &= df["vol_ok"]

        # Sesgo EMA200
        if usar_sesgo_tendencial:
            df["cruce_alcista"] &= df["close"] > df["ema_200"]
            df["cruce_bajista"] &= df["close"] < df["ema_200"]

        df["signal"] = "hold"
        df.loc[df["cruce_alcista"], "signal"] = "buy"
        df.loc[df["cruce_bajista"], "signal"] = "sell"
        df["estrategia"] = "cruce_medias_v3"

        logger.info(f"Cruce Medias v3 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

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
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "cruce_medias_v3"
    return df[["fecha", "signal", "estrategia"]]
