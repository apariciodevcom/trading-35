"""
===========================================================================
 Estrategia: Movimiento Promedios - v3 (cruce SMA 5/20, parametrizable + debug)
===========================================================================

Descripción:
------------
Genera señales de cruce entre SMA 5 y SMA 20:
✅ Confirmación futura opcional
✅ Filtro de volatilidad (ATR ratio)
✅ Modo debug

Parámetros:
-----------
- confirmar_al_dia_siguiente: bool
- atr_threshold: float
- debug: bool

Salida:
--------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("mov_avg_v3")

def generar_senales(df: pd.DataFrame,
                    confirmar_al_dia_siguiente: bool = True,
                    atr_threshold: float = 0.008,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low"}
        if not req.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 30:
            return df_as_hold(df, "datos insuficientes")

        # SMA
        df["sma_5"] = df["close"].rolling(5).mean()
        df["sma_20"] = df["close"].rolling(20).mean()

        # ATR ratio
        df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
        df["atr_ratio"] = df["atr"] / df["close"]
        df["vol_ok"] = df["atr_ratio"] > atr_threshold

        # Cruce SMA
        df["cruce_alcista"] = (df["sma_5"] > df["sma_20"]) & (df["sma_5"].shift(1) <= df["sma_20"].shift(1))
        df["cruce_bajista"] = (df["sma_5"] < df["sma_20"]) & (df["sma_5"].shift(1) >= df["sma_20"].shift(1))

        if confirmar_al_dia_siguiente:
            df["cruce_alcista"] &= df["sma_5"].shift(-1) > df["sma_20"].shift(-1)
            df["cruce_bajista"] &= df["sma_5"].shift(-1) < df["sma_20"].shift(-1)

        # Filtro volatilidad
        df["cruce_alcista"] &= df["vol_ok"]
        df["cruce_bajista"] &= df["vol_ok"]

        df["signal"] = "hold"
        df.loc[df["cruce_alcista"], "signal"] = "buy"
        df.loc[df["cruce_bajista"], "signal"] = "sell"
        df["estrategia"] = "mov_avg_v3"

        logger.info(f"MovAvg v3 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["sma_5", "sma_20", "cruce_alcista", "cruce_bajista", "atr", "atr_ratio", "vol_ok"]

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
    df["estrategia"] = "mov_avg_v3"
    return df[["fecha", "signal", "estrategia"]]
