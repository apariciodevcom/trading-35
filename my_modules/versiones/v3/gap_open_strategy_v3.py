"""
===========================================================================
 Estrategia: Gap de Apertura - v3 (con filtro absoluto y debug)
===========================================================================

Descripción:
------------
Detecta gaps de apertura significativos entre el cierre anterior y la
apertura del dia. Incluye:
✅ Confirmación opcional con vela
✅ Umbral de gap relativo configurable
✅ Umbral de gap absoluto mínimo (filtro de ruido)
✅ Modo debug para análisis

Requiere:
---------
- Columnas: 'fecha', 'close', 'open'

Salida:
-------
['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("gap_open_strategy_v3")

def generar_senales(df: pd.DataFrame,
                    umbral_gap: float = 0.03,
                    gap_min_abs_pct: float = 0.01,
                    usar_confirmacion_cuerpo: bool = False,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()
        req = {"fecha", "close", "open"}
        if not req.issubset(df.columns):
            logger.warning(f"Columnas faltantes: {req - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < 3:
            return df_as_hold(df, "datos insuficientes")

        # Calculo del GAP
        df["prev_close"] = df["close"].shift(1)
        df["gap_pct"] = (df["open"] - df["prev_close"]) / df["prev_close"]
        df["gap_abs"] = df["gap_pct"].abs()

        df["signal"] = "hold"

        # Aplicar umbrales
        df["gap_up"] = (df["gap_pct"] > umbral_gap) & (df["gap_abs"] > gap_min_abs_pct)
        df["gap_down"] = (df["gap_pct"] < -umbral_gap) & (df["gap_abs"] > gap_min_abs_pct)

        # Confirmacion con cuerpo de vela si se activa
        if usar_confirmacion_cuerpo:
            df["vela_alcista"] = df["close"] > df["open"]
            df["vela_bajista"] = df["close"] < df["open"]
            df["buy_cond"] = df["gap_up"] & df["vela_alcista"]
            df["sell_cond"] = df["gap_down"] & df["vela_bajista"]
        else:
            df["buy_cond"] = df["gap_up"]
            df["sell_cond"] = df["gap_down"]

        df.loc[df["buy_cond"], "signal"] = "buy"
        df.loc[df["sell_cond"], "signal"] = "sell"

        df["estrategia"] = "gap_open_strategy_v3"

        logger.info(f"gap>={umbral_gap}, abs>={gap_min_abs_pct}, buy={sum(df['signal']=='buy')}, sell={sum(df['signal']=='sell')}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["gap_pct", "gap_abs", "gap_up", "gap_down", "buy_cond", "sell_cond"]
            if usar_confirmacion_cuerpo:
                columnas += ["vela_alcista", "vela_bajista"]

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
    df["estrategia"] = "gap_open_strategy_v3"
    return df[["fecha", "signal", "estrategia"]]
