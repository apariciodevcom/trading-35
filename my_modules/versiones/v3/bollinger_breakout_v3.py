"""
===========================================================================
 Estrategia: Bollinger Breakout - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Genera señales de ruptura sobre bandas de Bollinger con múltiples filtros:
✅ Confirmación con cuerpo y volumen opcional
✅ Ajuste de bandas con ATR dinámico
✅ Modo debug para análisis técnico

Parámetros:
-----------
- window: int
- s: float (desviaciones std)
- ajuste_volatilidad: bool
- usar_filtro_cuerpo, usar_filtro_volumen: bool
- atr_threshold: float
- vol_multiplier: float
- debug: bool

Salida:
--------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("bollinger_breakout_v3")

def generar_senales(df: pd.DataFrame,
                    window: int = 20,
                    s: float = 2,
                    ajuste_volatilidad: bool = True,
                    usar_filtro_cuerpo: bool = True,
                    usar_filtro_volumen: bool = True,
                    atr_threshold: float = 0.01,
                    vol_multiplier: float = 1.1,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low", "open", "volume"}
        if not req.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < window + 1:
            return df_as_hold(df, "datos insuficientes")

        # 1. Bollinger Bands
        df["ma"] = df["close"].rolling(window).mean()
        df["std"] = df["close"].rolling(window).std()

        # 2. ATR para ajuste de desviación
        if ajuste_volatilidad:
            df["atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=14)
            df["atr_ratio"] = df["atr"] / df["close"]
            s_dyn = s * df["atr_ratio"]
        else:
            s_dyn = s

        df["upper"] = df["ma"] + s_dyn * df["std"]
        df["lower"] = df["ma"] - s_dyn * df["std"]

        # 3. Filtros opcionales
        df["cuerpo"] = abs(df["close"] - df["open"])
        df["cuerpo_prom"] = df["cuerpo"].rolling(window).mean()
        df["cuerpo_ok"] = df["cuerpo"] > df["cuerpo_prom"]

        df["vol_sma"] = df["volume"].rolling(window).mean()
        df["vol_ok"] = df["volume"] > df["vol_sma"] * vol_multiplier

        if usar_filtro_volumen:
            filtro_vol = df["vol_ok"]
        else:
            filtro_vol = True

        if usar_filtro_cuerpo:
            filtro_cuerpo = df["cuerpo_ok"]
        else:
            filtro_cuerpo = True

        if ajuste_volatilidad:
            filtro_volat = df["atr_ratio"] > atr_threshold
        else:
            filtro_volat = True

        # 4. Condiciones de ruptura
        df["buy_cond"] = (df["close"] > df["upper"]) & filtro_vol & filtro_cuerpo & filtro_volat
        df["sell_cond"] = (df["close"] < df["lower"]) & filtro_vol & filtro_cuerpo & filtro_volat

        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df.loc[df["sell_cond"], "signal"] = "sell"

        df["estrategia"] = "bollinger_breakout_v3"

        logger.info(f"BB v3 | BUY={df['buy_cond'].sum()} | SELL={df['sell_cond'].sum()} | TOTAL={len(df)}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["close", "upper", "lower", "ma", "std", "cuerpo", "cuerpo_prom", "cuerpo_ok",
                         "volume", "vol_sma", "vol_ok", "buy_cond", "sell_cond"]
            if ajuste_volatilidad:
                columnas += ["atr", "atr_ratio"]

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
    df["estrategia"] = "bollinger_breakout_v3"
    return df[["fecha", "signal", "estrategia"]]
