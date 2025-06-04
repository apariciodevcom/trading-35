"""
===========================================================================
 Estrategia: Breakout por Volumen - v3 (parametrizable + debug)
===========================================================================

Descripción:
------------
Detecta rupturas (breakouts) confirmadas por:
- Precio: cierre supera max/min previos (shifted)
- Volumen: opcional, superior a promedio previo
- Cuerpo de vela: opcional, significativo vs rolling

Parámetros:
-----------
- ventana: int (default=20)
- usar_volumen: bool
- usar_cuerpo: bool
- min_ratio_cuerpo: float (e.g. 0.5 → cuerpo > promedio)
- debug: bool

Salida:
--------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("breakout_volumen_v3")

def generar_senales(df: pd.DataFrame,
                    ventana: int = 20,
                    usar_volumen: bool = True,
                    usar_cuerpo: bool = True,
                    min_ratio_cuerpo: float = 0.5,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "high", "low", "open", "volume"}
        if not req.issubset(df.columns):
            logger.warning(f"Faltan columnas: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < ventana + 1:
            return df_as_hold(df, "datos insuficientes")

        # Indicadores shifted
        df["max_20"] = df["high"].shift(1).rolling(ventana).max()
        df["min_20"] = df["low"].shift(1).rolling(ventana).min()

        # Cuerpo de vela
        df["cuerpo"] = abs(df["close"] - df["open"])
        df["cuerpo_avg"] = df["cuerpo"].shift(1).rolling(ventana).mean()

        # Volumen
        df["vol_avg"] = df["volume"].shift(1).rolling(ventana).mean()

        # Condiciones
        df["cond_price_buy"] = df["close"] > df["max_20"]
        df["cond_price_sell"] = df["close"] < df["min_20"]

        df["cond_vol"] = df["volume"] > df["vol_avg"] if usar_volumen else True
        df["cond_cuerpo"] = df["cuerpo"] > (df["cuerpo_avg"] * min_ratio_cuerpo) if usar_cuerpo else True

        # Señales
        df["buy_cond"] = df["cond_price_buy"] & df["cond_vol"] & df["cond_cuerpo"]
        df["sell_cond"] = df["cond_price_sell"] & df["cond_vol"] & df["cond_cuerpo"]

        df["signal"] = "hold"
        df.loc[df["buy_cond"], "signal"] = "buy"
        df.loc[df["sell_cond"], "signal"] = "sell"
        df["estrategia"] = "breakout_volumen_v3"

        logger.info(f"Breakout v3 | BUY={df['buy_cond'].sum()} | SELL={df['sell_cond'].sum()} | Total={len(df)}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += [
                "close", "max_20", "min_20",
                "cuerpo", "cuerpo_avg", "cond_cuerpo",
                "volume", "vol_avg", "cond_vol",
                "cond_price_buy", "cond_price_sell",
                "buy_cond", "sell_cond"
            ]

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
    df["estrategia"] = "breakout_volumen_v3"
    return df[["fecha", "signal", "estrategia"]]
