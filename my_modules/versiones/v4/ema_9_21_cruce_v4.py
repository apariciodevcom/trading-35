"""
===========================================================================
 Estrategia: Cruce EMA 9/21 - v4 (parametrizable + debug) - LeanTech Trading
===========================================================================

📌 Descripción:
--------------
Detecta señales de compra y venta basadas en el cruce entre dos medias móviles exponenciales:
- EMA 9 (rápida)
- EMA 21 (lenta)

Incorpora:
✅ Filtro opcional de volatilidad (basado en ATR)
✅ Confirmación de cruce durante varios días consecutivos
✅ Modo debug para análisis y trazabilidad

🧪 Mejores parámetros encontrados (Junio 2025):
-----------------------------------------------
- usar_vol = True
- atr_threshold = 0.0095
- dias_cruce = 2

📈 Señales:
----------
- BUY: EMA9 cruza por encima de EMA21 y se mantiene ≥ `dias_cruce` días.
- SELL: EMA9 cruza por debajo de EMA21 y se mantiene ≥ `dias_cruce` días.
- Filtro adicional: ATR_ratio > `atr_threshold` (si se activa)

📤 Salida:
----------
DataFrame con columnas: ['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("ema_9_21_cruce_v4")

def generar_senales(df: pd.DataFrame,
                    usar_vol: bool = True,
                    atr_threshold: float = 0.0095,
                    dias_cruce: int = 2,
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

        # EMAs
        df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
        df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()

        # Condiciones cruce
        df["cond_buy"] = df["ema_9"] > df["ema_21"]
        df["cond_sell"] = df["ema_9"] < df["ema_21"]

        # Confirmación por días consecutivos
        df["cond_buy"] = df["cond_buy"].rolling(dias_cruce).apply(lambda x: all(x), raw=True).fillna(False).astype(bool)
        df["cond_sell"] = df["cond_sell"].rolling(dias_cruce).apply(lambda x: all(x), raw=True).fillna(False).astype(bool)

        # Volatilidad
        if usar_vol:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            df["vol_ok"] = df["atr_ratio"] > atr_threshold
            df["cond_buy"] &= df["vol_ok"]
            df["cond_sell"] &= df["vol_ok"]

        # Señales
        df["signal"] = "hold"
        df.loc[df["cond_buy"], "signal"] = "buy"
        df.loc[df["cond_sell"], "signal"] = "sell"
        df["estrategia"] = "ema_9_21_cruce_v4"

        logger.info(f"EMA 9/21 v4 | BUY={df['cond_buy'].sum()} | SELL={df['cond_sell'].sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["ema_9", "ema_21", "cond_buy", "cond_sell"]
            if usar_vol:
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
    df["estrategia"] = "ema_9_21_cruce_v4"
    return df[["fecha", "signal", "estrategia"]]