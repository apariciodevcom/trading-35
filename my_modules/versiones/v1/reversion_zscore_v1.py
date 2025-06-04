import pandas as pd
from my_modules.logger_estrategia import configurar_logger

# Logger con nombre específico
logger = configurar_logger("reversion_zscore_v1")

def generar_senales(
    df: pd.DataFrame,
    ventana: int = 20,            # ventana para la media y desvío estándar
    z_buy: float = -2.5,          # umbral inferior para compra
    z_sell: float = 2.5,          # umbral superior para venta
    debug: bool = False
) -> pd.DataFrame:
    try:
        df = df.copy()

        # Verifica columnas requeridas
        if not {"fecha", "close"}.issubset(df.columns):
            return df_as_hold(df, razon="faltan columnas")

        df = df.sort_values("fecha").reset_index(drop=True)

        # Calculo de media móvil y desvío estándar
        df["ma"] = df["close"].rolling(ventana).mean()
        df["std"] = df["close"].rolling(ventana).std()

        # Cálculo del z-score
        df["zscore"] = (df["close"] - df["ma"]) / df["std"]

        # Señal según desviación extrema
        df["signal"] = "hold"
        df.loc[df["zscore"] < z_buy, "signal"] = "buy"
        df.loc[df["zscore"] > z_sell, "signal"] = "sell"
        df["estrategia"] = "reversion_zscore_v1"

        logger.info(f"RZ v1 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["zscore", "ma", "std"]

        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, razon="exception")

# Fallback a HOLD si hay errores
def df_as_hold(df: pd.DataFrame, razon: str) -> pd.DataFrame:
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "reversion_zscore_v1"
    return df[["fecha", "signal", "estrategia"]]

