"""
===========================================================================
 Estrategia: Cruce de EMAs 9/21 - v3 (parametrizable + debug) - LeanTech Trading
===========================================================================

Descripción:
------------
Genera señales de compra o venta cuando el cruce de EMA9/EMA21 persiste
al menos 'n' días consecutivos. Incluye:
✅ Filtro de volatilidad parametrizable (ATR ratio)
✅ Persistencia de cruce configurable
✅ Modo debug para análisis gráfico

Requiere:
---------
- Columnas: 'fecha', 'close', 'high', 'low'

Salida:
-------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("ema_9_21_cruce_v3")

def generar_senales(df: pd.DataFrame, usar_filtro_volatilidad: bool = True,
                     atr_threshold: float = 0.008,
                     dias_cruce: int = 2,
                     debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low"}
        if not req.issubset(df.columns):
            logger.warning(f"Columnas faltantes: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)

        if len(df) < 30:
            return df_as_hold(df, "datos insuficientes")

        # Calculo EMAs
        df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
        df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()

        # Cruces EMA
        df["ema_up"] = df["ema_9"] > df["ema_21"]
        df["ema_down"] = df["ema_9"] < df["ema_21"]

        df["ema_up_persist"] = df["ema_up"].rolling(dias_cruce).apply(lambda x: all(x), raw=True).fillna(0).astype(bool)
        df["ema_down_persist"] = df["ema_down"].rolling(dias_cruce).apply(lambda x: all(x), raw=True).fillna(0).astype(bool)

        # Filtro de volatilidad
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > atr_threshold
            df["ema_up_persist"] &= df["vol_ok"]
            df["ema_down_persist"] &= df["vol_ok"]

        # Señales
        df["signal"] = "hold"
        df.loc[df["ema_up_persist"], "signal"] = "buy"
        df.loc[df["ema_down_persist"], "signal"] = "sell"

        df["estrategia"] = "ema_9_21_cruce_v3"
        logger.info(f"Cruce {dias_cruce}d | ATR>{atr_threshold} | BUY={sum(df['signal']=='buy')} | SELL={sum(df['signal']=='sell')}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["ema_9", "ema_21", "ema_up", "ema_down", "ema_up_persist", "ema_down_persist"]
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
    df["estrategia"] = "ema_9_21_cruce_v3"
    return df[["fecha", "signal", "estrategia"]]
