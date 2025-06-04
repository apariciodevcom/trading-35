"""
===========================================================================
 Estrategia: Cruce de MACD - v3 (parametrizable + debug) - LeanTech Trading
===========================================================================

Descripción:
------------
Genera señales BUY/SELL en cruces de MACD con su línea de señal.
Mejoras:
✅ Confirmación opcional en días futuros
✅ Filtro de volatilidad parametrizable
✅ Modo debug para inspección técnica

Requiere:
---------
- Columnas: 'fecha', 'close', 'high', 'low'

Salida:
--------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("macd_cruce_v3")

def generar_senales(df: pd.DataFrame,
                    usar_confirmacion_cruce: bool = False,
                    usar_filtro_volatilidad: bool = False,
                    atr_threshold: float = 0.008,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low"}
        if not req.issubset(df.columns):
            logger.warning(f"Columnas faltantes: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 35:
            return df_as_hold(df, "datos insuficientes")

        # 1. Calculo MACD y señal
        df["macd"] = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
        df["signal_line"] = df["macd"].ewm(span=9, adjust=False).mean()

        # 2. Detectar cruces
        df["cruce_alcista"] = (df["macd"] > df["signal_line"]) & (df["macd"].shift(1) <= df["signal_line"].shift(1))
        df["cruce_bajista"] = (df["macd"] < df["signal_line"]) & (df["macd"].shift(1) >= df["signal_line"].shift(1))

        # 3. Confirmación al día siguiente
        if usar_confirmacion_cruce:
            df["cruce_alcista"] &= df["macd"].shift(-1) > df["signal_line"].shift(-1)
            df["cruce_bajista"] &= df["macd"].shift(-1) < df["signal_line"].shift(-1)

        # 4. Filtro de volatilidad
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > atr_threshold
            df["cruce_alcista"] &= df["vol_ok"]
            df["cruce_bajista"] &= df["vol_ok"]

        # 5. Asignar señales
        df["signal"] = "hold"
        df.loc[df["cruce_alcista"], "signal"] = "buy"
        df.loc[df["cruce_bajista"], "signal"] = "sell"

        df["estrategia"] = "macd_cruce_v3"
        logger.info(f"MACD v3 | BUY={sum(df['signal']=='buy')} | SELL={sum(df['signal']=='sell')}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["macd", "signal_line", "cruce_alcista", "cruce_bajista"]
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
    df["estrategia"] = "macd_cruce_v3"
    return df[["fecha", "signal", "estrategia"]]
