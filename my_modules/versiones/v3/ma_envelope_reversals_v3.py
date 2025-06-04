"""
===========================================================================
 Estrategia: Reversión MA Envelope - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Detecta reversiones desde extremos de un canal tipo envelope
basado en una media móvil simple.
✅ Filtro de volatilidad opcional (ATR)
✅ Confirmación por vela de reversión
✅ Modo debug

Parámetros:
-----------
- window: int (SMA y envelope)
- envelope_pct: float (ancho del canal)
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

logger = configurar_logger("ma_envelope_reversals_v3")

def generar_senales(df: pd.DataFrame,
                    window: int = 20,
                    envelope_pct: float = 0.03,
                    usar_filtro_volatilidad: bool = True,
                    atr_threshold: float = 0.008,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req_cols = {"fecha", "close", "high", "low", "open"}
        if not req_cols.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req_cols - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < window + 1:
            return df_as_hold(df, "datos insuficientes")

        # Envelope
        df["ma"] = df["close"].rolling(window).mean()
        df["upper"] = df["ma"] * (1 + envelope_pct)
        df["lower"] = df["ma"] * (1 - envelope_pct)

        # Confirmación de vela
        df["cuerpo"] = df["close"] - df["open"]
        df["vela_alcista"] = df["cuerpo"] > 0
        df["vela_bajista"] = df["cuerpo"] < 0

        df["buy_cond"] = (df["close"] < df["lower"]) & df["vela_alcista"]
        df["sell_cond"] = (df["close"] > df["upper"]) & df["vela_bajista"]

        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > atr_threshold
            df["buy_cond"] &= df["vol_ok"]
            df["sell_cond"] &= df["vol_ok"]

        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df.loc[df["sell_cond"], "signal"] = "sell"
        df["estrategia"] = "ma_envelope_reversals_v3"

        logger.info(f"Envelope v3 | BUY={df['buy_cond'].sum()} | SELL={df['sell_cond'].sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["ma", "upper", "lower", "vela_alcista", "vela_bajista", "buy_cond", "sell_cond"]
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
    df["estrategia"] = "ma_envelope_reversals_v3"
    return df[["fecha", "signal", "estrategia"]]
