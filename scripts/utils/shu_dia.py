import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from importlib import import_module
import traceback
import sys

sys.path.append("/home/ubuntu/tr")

# === CONFIG ===
CONFIG_PATH = Path("/home/ubuntu/tr/config/symbol_groups.json")
HISTORIC_PATH = Path("/home/ubuntu/tr/data/historic_reciente")
OUTPUT_PATH = Path("/home/ubuntu/tr/reports/senales_heuristicas/diarias")
LOG_PATH = Path(f"/home/ubuntu/tr/logs/utils/shu_diario_{datetime.now().date()}.csv")
STATUS_PATH = Path("/home/ubuntu/tr/config/system_status.json")
ESTRATEGIAS_DIR = "my_modules.estrategias"
ESTRATEGIAS_PATH = "/home/ubuntu/tr/my_modules/estrategias"

# === CARGAR SIMBOLOS ===
with open(CONFIG_PATH, "r") as f:
    grupos = json.load(f)
SIMBOLOS = sorted(set(sum(grupos.values(), [])))

# === CARGAR ESTRATEGIAS ===
estrategias = {}
estrategias_cargadas = []
for archivo in os.listdir(ESTRATEGIAS_PATH):
    if archivo.endswith(".py"):
        try:
            mod = import_module(f"{ESTRATEGIAS_DIR}.{archivo[:-3]}")
            estrategias[archivo[:-3]] = mod.generar_senales
            estrategias_cargadas.append(archivo[:-3])
        except Exception as e:
            print(f"[ERROR] No se pudo cargar {archivo}: {e}")

# === LOG ===
def log_event(modulo, status, mensaje, inicio):
    fin = datetime.now()
    dur = round((fin - inicio).total_seconds(), 2)
    ts = fin.strftime("%Y-%m-%d %H:%M:%S")
    linea = f"{ts},{modulo},{status},{mensaje},{dur}s\n"
    with open(LOG_PATH, "a") as f:
        f.write(linea)
    print(f"[{modulo}] {status}: {mensaje} ({dur}s)")

log_event("loader", "OK", f"Estrategias cargadas: {', '.join(estrategias_cargadas)}", datetime.now())

# === PREPARAR CARPETA OUTPUT ===
if OUTPUT_PATH.exists():
    for archivo in OUTPUT_PATH.glob("*"):
        archivo.unlink()
else:
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# === PROCESAMIENTO ===
errores = []
inicio_total = datetime.now()

for simbolo in SIMBOLOS:
    inicio = datetime.now()
    estrategias_activas = []
    try:
        archivo = HISTORIC_PATH / f"{simbolo}.parquet"
        if not archivo.exists():
            raise FileNotFoundError(f"{archivo} no encontrado")

        df = pd.read_parquet(archivo).reset_index(drop=True)
        if df.empty or "fecha" not in df.columns:
            raise ValueError("Histórico vacío o sin columna 'fecha'")

        ultima_fecha = df["fecha"].max()
        log_event(simbolo, "INFO", f"Última fecha en histórico: {ultima_fecha}", inicio)

        resultados = []
        for nombre_est, funcion in estrategias.items():
            try:
                df_out = funcion(df.copy())
                if df_out is not None and not df_out.empty:
                    df_out = df_out[df_out["fecha"] == ultima_fecha]
                    if not df_out.empty:
                        df_out["simbolo"] = simbolo
                        df_out["estrategia"] = nombre_est
                        resultados.append(df_out)
                        estrategias_activas.append(nombre_est)
            except Exception as e:
                log_event(nombre_est, "ERROR", f"{simbolo} fallo interno: {str(e)}", inicio)
                traceback.print_exc()

        if resultados:
            df_result = pd.concat(resultados)
            df_result["fecha"] = pd.to_datetime(df_result["fecha"]).dt.strftime("%Y-%m-%d")
            archivo_salida = OUTPUT_PATH / f"{simbolo}_senales_diarias.csv"
            df_result.to_csv(archivo_salida, index=False)
            log_event(simbolo, "OK", f"{simbolo} procesado - estrategias: {', '.join(estrategias_activas)}", inicio)
        else:
            log_event(simbolo, "SKIP", f"{simbolo} sin señales para {ultima_fecha}", inicio)

    except Exception as e:
        errores.append(simbolo)
        log_event(simbolo, "ERROR", f"{simbolo} fallo: {str(e)}", inicio)
        traceback.print_exc()

log_event("shu_diario", "RESUMEN", f"{len(SIMBOLOS)-len(errores)} de {len(SIMBOLOS)} procesados", inicio_total)

# === ACTUALIZAR STATUS ===
estado = {
    "fecha": datetime.now().strftime("%Y-%m-%d"),
    "status": "OK" if not errores else "ERROR",
    "mensaje": f"{len(SIMBOLOS)-len(errores)} de {len(SIMBOLOS)} procesados"
}
with open(STATUS_PATH, "r") as f:
    status_json = json.load(f)
status_json["senales_heuristicas_diarias"] = estado
with open(STATUS_PATH, "w") as f:
    json.dump(status_json, f, indent=2)
