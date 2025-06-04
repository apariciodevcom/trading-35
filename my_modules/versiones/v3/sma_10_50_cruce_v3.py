"""
===========================================================================
 Estrategia: Cruce SMA 10/50 - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Detecta señales de cruce entre SMA 10 y SMA 50:
✅ Confirmación futura opcional
✅ Filtro de volatilidad (ATR ratio)
✅ Modo debug para inspección técnica

Parámetros:
-----------
- confirmar_al_dia_siguiente: bool
- usar_filtro_volatilidad: bool
- atr_threshold: float
- debug: bool

Salida:
--------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("sma_10_50_cruce_v3")

def generar_senales(df: pd.DataFrame,
                    confirmar_al_dia_siguiente: bool = True,
                    usar_filtro_volatilidad: bool = True,
                    atr_threshold: float = 0.008,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req_cols = {"fecha", "close", "high", "low"}
        if not req_cols.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req_cols - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 60:
            return df_as_hold(df, "datos insuficientes")

        # SMA
        df["sma_10"] = df["close"].rolling(10).mean()
        df["sma_50"] = df["close"].rolling(50).mean()

        # Cruces
        df["cruce_alcista"] = (df["sma_10"] > df["sma_50"]) & (df["sma_10"].shift(1) <= df["sma_50"].shift(1))
        df["cruce_bajista"] = (df["sma_10"] < df["sma_50"]) & (df["sma_10"].shift(1) >= df["sma_50"].shift(1))

        # Confirmación futura opcional
        if confirmar_al_dia_siguiente:
            df["cruce_alcista"] &= df["sma_10"].shift(-1) > df["sma_50"].shift(-1)
            df["cruce_bajista"] &= df["sma_10"].shift(-1) < df["sma_50"].shift(-1)

        # Volatilidad
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > atr_threshold
            df["cruce_alcista"] &= df["vol_ok"]
            df["cruce_bajista"] &= df["vol_ok"]

        df["signal"] = "hold"
        df.loc[df["cruce_alcista"], "signal"] = "buy"
        df.loc[df["cruce_bajista"], "signal"] = "sell"
        df["estrategia"] = "sma_10_50_cruce_v3"

        logger.info(f"SMA 10/50 v3 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["sma_10", "sma_50", "cruce_alcista", "cruce_bajista"]
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
    df["estrategia"] = "sma_10_50_cruce_v3"
    return df[["fecha", "signal", "estrategia"]]
