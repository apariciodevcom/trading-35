"""
===========================================================================
 Estrategia: ADX Filter Trend (v4 extendida y parametrizable) - LeanTech Trading
===========================================================================

Descripcion:
------------
Detecta BUY/SELL segun:
- BUY: close > sma_20 y adx > threshold
- SELL: close < sma_20 y adx > threshold

Novedades v4:
-------------
✅ Umbral ADX parametrizable (default=10)
✅ Columnas auxiliares opcionales para debug: 'adx', 'sma_20'
✅ Comentarios detallados paso a paso

Requiere:
---------
- Columnas: 'fecha', 'close', 'high', 'low'
- Librerias: pandas, ta

Salida:
-------
DataFrame con ['fecha', 'signal', 'estrategia', ...]
"""

import pandas as pd
from my_modules.logger_estrategia import configurar_logger
import ta

logger = configurar_logger("adx_filter_trend_v3")

def generar_senales(df: pd.DataFrame, adx_threshold: float = 10, debug: bool = False) -> pd.DataFrame:
    try:
        df = df.copy()

        # 1. Validar columnas necesarias
        columnas_necesarias = {"fecha", "close", "high", "low"}
        if not columnas_necesarias.issubset(df.columns):
            faltantes = columnas_necesarias - set(df.columns)
            logger.warning(f"Columnas faltantes: {faltantes}")
            return df_as_hold(df, estrategia="adx_filter_trend_v3", razon="faltan columnas")

        # 2. Ordenar por fecha
        df = df.sort_values("fecha").reset_index(drop=True)

        # 3. Validar mínimo de datos para sma_20
        if len(df) < 20:
            logger.warning("Datos insuficientes para calcular SMA20")
            return df_as_hold(df, estrategia="adx_filter_trend_v3", razon="datos insuficientes")

        # 4. Calcular indicadores
        df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)
        df["sma_20"] = df["close"].rolling(20).mean()

        # 5. Inicializar señal
        df["signal"] = "hold"

        # 6. Aplicar condiciones de señal
        df.loc[(df["close"] > df["sma_20"]) & (df["adx"] > adx_threshold), "signal"] = "buy"
        df.loc[(df["close"] < df["sma_20"]) & (df["adx"] > adx_threshold), "signal"] = "sell"

        # 7. Etiqueta de estrategia
        df["estrategia"] = "adx_filter_trend_v3"

        # 8. Log resumen
        logger.info(f"TH={adx_threshold} | BUY={sum(df['signal']=='buy')} | SELL={sum(df['signal']=='sell')} | TOTAL={len(df)}")

        # 9. Salida
        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["adx", "sma_20"]
        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, estrategia="adx_filter_trend_v3", razon="exception")

def df_as_hold(df: pd.DataFrame, estrategia: str, razon: str) -> pd.DataFrame:
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    df["signal"] = "hold"
    df["estrategia"] = estrategia
    return df[["fecha", "signal", "estrategia"]] if "fecha" in df.columns else pd.DataFrame(columns=["fecha", "signal", "estrategia"])
