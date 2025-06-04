# ============================================================================
# Estrategia: Precio Bajo con Filtros ADX y Volumen - v1
# ============================================================================
# Intención:
# ----------
# Esta estrategia busca identificar oportunidades de compra cuando el precio
# se encuentra en una zona baja relativa dentro de una ventana de tiempo,
# lo cual puede representar un soporte técnico o una oportunidad de reversión.
# Se agregan filtros de tendencia (ADX) y volumen para evitar operar en
# situaciones de debilidad o sin interés institucional.
# 
# Estructura modular y parametrizable, compatible con el sistema de señal.
# ============================================================================


import pandas as pd
import ta
from my_modules.logger_estrategia import configurar_logger

logger = configurar_logger("precio_bajo_filtros_v1")

def generar_senales(df: pd.DataFrame,
                    window: int = 100,
                    adx_threshold: float = 20,
                    usar_adx: bool = True,
                    usar_volumen: bool = True,
                    debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()

        # Validar columnas necesarias
        columnas_necesarias = {"fecha", "close", "high", "low", "volume"}
        if not columnas_necesarias.issubset(df.columns):
            logger.warning(f"Faltan columnas: {columnas_necesarias - set(df.columns)}")
            return df_as_hold(df, "faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)
        if len(df) < window:
            return df_as_hold(df, "datos insuficientes")

        # Posición relativa en la ventana de N días
        df["min_win"] = df["close"].rolling(window).min()
        df["max_win"] = df["close"].rolling(window).max()
        df["rango_win"] = df["max_win"] - df["min_win"]
        df["pos_relativa"] = (df["close"] - df["min_win"]) / df["rango_win"]

        # ADX (opcional)
        if usar_adx:
            df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)

        # Volumen (opcional)
        if usar_volumen:
            df["vol_media"] = df["volume"].rolling(20).mean()

        # Condición base
        condiciones = (df["pos_relativa"] < 0.1)

        if usar_adx:
            condiciones &= df["adx"] > adx_threshold
        if usar_volumen:
            condiciones &= df["volume"] > df["vol_media"]

        # Señales
        df["signal"] = "hold"
        df.loc[condiciones, "signal"] = "buy"
        df["estrategia"] = "precio_bajo_filtros_v1"

        logger.info(f"WIN={window} | ADX>{adx_threshold} | BUY={df['signal'].eq('buy').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["min_win", "max_win", "pos_relativa"]
            if usar_adx:
                columnas += ["adx"]
            if usar_volumen:
                columnas += ["volume", "vol_media"]

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
    df["estrategia"] = "precio_bajo_filtros_v1"
    return df[["fecha", "signal", "estrategia"]]
