import pandas as pd
from my_modules.logger_estrategia import configurar_logger

# Configura logger para registrar actividad de la estrategia
logger = configurar_logger("ruptura_volumen_v1")

def generar_senales(
    df: pd.DataFrame,
    umbral_roc: float = 0.02,      # cambio mínimo en % para considerar ruptura
    zscore_vol: float = 1.6,       # cuán anómalo debe ser el volumen
    debug: bool = False
) -> pd.DataFrame:
    try:
        df = df.copy()

        # Validación de columnas necesarias
        if not {"fecha", "close", "volume"}.issubset(df.columns):
            return df_as_hold(df, razon="faltan columnas")

        # Orden temporal
        df = df.sort_values("fecha").reset_index(drop=True)

        # Cálculo de ROC (Rate of Change) de 1 día
        df["roc_1d"] = df["close"].pct_change()

        # Volumen promedio y z-score en ventana de 3 días
        df["vol_ma_3"] = df["volume"].rolling(3).mean()
        df["vol_z"] = (df["volume"] - df["vol_ma_3"]) / df["vol_ma_3"]

        # Condiciones para señales
        df["cond_buy"] = (df["roc_1d"] > umbral_roc) & (df["vol_z"] > zscore_vol)
        df["cond_sell"] = (df["roc_1d"] < -umbral_roc) & (df["vol_z"] > zscore_vol)

        # Inicialización
        df["signal"] = "hold"
        df.loc[df["cond_buy"], "signal"] = "buy"
        df.loc[df["cond_sell"], "signal"] = "sell"
        df["estrategia"] = "ruptura_volumen_v1"

        logger.info(f"RV v1 | BUY={df['signal'].eq('buy').sum()} | SELL={df['signal'].eq('sell').sum()}")

        columnas = ["fecha", "signal", "estrategia"]
        if debug:
            columnas += ["roc_1d", "vol_z", "cond_buy", "cond_sell"]

        return df[columnas]

    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return df_as_hold(df, razon="exception")

# Función auxiliar para fallback a HOLD en errores
def df_as_hold(df: pd.DataFrame, razon: str) -> pd.DataFrame:
    logger.info(f"Retornando HOLD por: {razon}")
    df = df.copy()
    if "fecha" not in df.columns:
        return pd.DataFrame(columns=["fecha", "signal", "estrategia"])
    df["signal"] = "hold"
    df["estrategia"] = "ruptura_volumen_v1"
    return df[["fecha", "signal", "estrategia"]]
