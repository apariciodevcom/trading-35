"""
===========================================================================
 Estrategia: Divergencias RSI - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Detecta condiciones extremas del RSI y rebotes del precio
para generar señales de reversión. Mejoras v3:
✅ Confirmación RSI con pendiente positiva > 1 día
✅ Cuerpo de vela significativo para rebote
✅ Filtro de volatilidad con umbral parametrizable
✅ Modo debug para análisis técnico

Parámetros:
-----------
- window: int (RSI)
- sobreventa, sobrecompra: int
- confirmar_direccion_rsi: bool
- confirmar_rebote_cuerpo: bool
- usar_filtro_volatilidad: bool
- vol_threshold: float
- debug: bool

Salida:
-------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("rsi_divergencia_v3")

def generar_senales(df: pd.DataFrame,
                    window: int = 14,
                    sobreventa: int = 30,
                    sobrecompra: int = 70,
                    confirmar_direccion_rsi: bool = False,
                    confirmar_rebote_cuerpo: bool = False,
                    usar_filtro_volatilidad: bool = False,
                    vol_threshold: float = 0.008,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low", "open"}
        if not req.issubset(df.columns):
            logger.warning(f"Columnas faltantes: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < window + 2:
            return df_as_hold(df, "datos insuficientes")

        # 1. RSI y extremos
        df["rsi"] = ta.momentum.RSIIndicator(df["close"], window=window).rsi()
        df["min_rolling"] = df["close"].rolling(window).min()
        df["max_rolling"] = df["close"].rolling(window).max()

        df["buy_cond"] = (df["rsi"] < sobreventa) & (df["close"] > df["min_rolling"])
        df["sell_cond"] = (df["rsi"] > sobrecompra) & (df["close"] < df["max_rolling"])

        # 2. Confirmar pendiente RSI (> 1 día positiva)
        if confirmar_direccion_rsi:
            df["rsi_up"] = (df["rsi"].diff(1) > 0) & (df["rsi"].diff(2) > 0)
            df["rsi_down"] = (df["rsi"].diff(1) < 0) & (df["rsi"].diff(2) < 0)
            df["buy_cond"] &= df["rsi_up"]
            df["sell_cond"] &= df["rsi_down"]

        # 3. Confirmar rebote con vela significativa
        if confirmar_rebote_cuerpo:
            cuerpo = (df["close"] - df["open"]).abs()
            rango = (df["high"] - df["low"]).replace(0, 1e-5)  # evitar div/0
            df["cuerpo_ratio"] = cuerpo / rango
            df["vela_buena"] = df["cuerpo_ratio"] > 0.5  # mínimo 50% del rango
            df["buy_cond"] &= (df["close"] > df["open"]) & df["vela_buena"]
            df["sell_cond"] &= (df["close"] < df["open"]) & df["vela_buena"]

        # 4. Filtro de volatilidad
        if usar_filtro_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > vol_threshold
            df["buy_cond"] &= df["vol_ok"]
            df["sell_cond"] &= df["vol_ok"]

        # 5. Señales
        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df.loc[df["sell_cond"], "signal"] = "sell"
        df["estrategia"] = "rsi_divergencia_v3"

        logger.info(f"RSI v3 señales: buy={sum(df['signal']=='buy')}, sell={sum(df['signal']=='sell')}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["rsi", "min_rolling", "max_rolling", "buy_cond", "sell_cond"]
            if confirmar_direccion_rsi:
                columnas += ["rsi_up", "rsi_down"]
            if confirmar_rebote_cuerpo:
                columnas += ["cuerpo_ratio", "vela_buena"]
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
    df["estrategia"] = "rsi_divergencia_v3"
    return df[["fecha", "signal", "estrategia"]]
