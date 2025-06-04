"""
===========================================================================
 Estrategia: Reversión RSI - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Genera señales de reversión basada en condiciones de sobreventa/sobrecompra
con pendiente favorable y filtro de volatilidad.

Parámetros:
-----------
- sobreventa: int
- sobrecompra: int
- confirmar_direccion_rsi: bool
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

logger = configurar_logger("rsi_reversion_v3")

def generar_senales(df: pd.DataFrame,
                    sobreventa: int = 30,
                    sobrecompra: int = 70,
                    confirmar_direccion_rsi: bool = True,
                    usar_filtro_volatilidad: bool = True,
                    atr_threshold: float = 0.008,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low"}
        if not req.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 15:
            return df_as_hold(df, "datos insuficientes")

        df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=14).rsi()

        df["buy_cond"] = df["rsi"] < sobreventa
        df["sell_cond"] = df["rsi"] > sobrecompra

        if confirmar_direccion_rsi:
            df["rsi_up"] = df["rsi"].diff(1) > 0
            df["rsi_down"] = df["rsi"].diff(1) < 0
            df["buy_cond"] &= df["rsi_up"]
            df["sell_cond"] &= df["rsi_down"]

        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > atr_threshold
            df["buy_cond"] &= df["vol_ok"]
            df["sell_cond"] &= df["vol_ok"]

        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df.loc[df["sell_cond"], "signal"] = "sell"
        df["estrategia"] = "rsi_reversion_v3"

        logger.info(f"RSI v3 | BUY={df['buy_cond'].sum()} | SELL={df['sell_cond'].sum()} | TOTAL={len(df)}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["rsi", "buy_cond", "sell_cond"]
            if confirmar_direccion_rsi:
                columnas += ["rsi_up", "rsi_down"]
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
    df["estrategia"] = "rsi_reversion_v3"
    return df[["fecha", "signal", "estrategia"]]
