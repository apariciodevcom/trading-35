# Sistema de Trading Local - trading-34

Sistema local de analisis, generacion de senales y evaluacion de estrategias heuristicas. Integrado con scripts de ingesta, procesamiento, backtesting y alertas.

Uso privado para investigacion y despliegue manual en VM/Linux o entorno Windows local.

---

## Estructura del proyecto

```
trading-34/
├── config/                   # Configuraciones y symbol groups
├── data/                     # Datos historicos en .parquet o CSV
├── logs/                     # Registros de ejecucion
├── my_modules/               # Modulos internos como estrategias y email
├── notebooks/                # Jupyter notebooks de analisis
├── reports/                  # Ignorado en Git (.gitignore)
├── respaldo/                 # Backups locales manuales
├── scripts/                  # Scripts de ingesta, senales, limpieza, etc.
├── trading_env/              # Entorno virtual (ignorado)
├── .gitignore
└── README.md
```

---

## Requisitos

- Python 3.10+
- Entorno virtual trading_env activo
- Dependencias principales:

```
pip install pandas numpy matplotlib jupyter ta scikit-learn
```

---

## Uso basico

Activar entorno:

```
D:\trading_env\Scripts\activate.bat
```

Lanzar Jupyter:

```
cd notebooks
jupyter notebook
```

Ejecutar scripts:

```
python scripts/utils/shu_cro.py
python scripts/utils/alc_v1.py
```

---

## Seguridad

- .keys.sh y archivos .env no deben subirse al repo
- Carpeta reports/ excluida por contener archivos grandes
- Archivos .parquet y .ipynb_checkpoints/ ignorados por .gitignore

---

## Notebooks recomendados

Organizados en notebooks/:

- exploracion_datos.ipynb
- generar_senales.ipynb
- ejecutar_backtest.ipynb
- analisis_resultados.ipynb

---

## Autor

Luis Figueroa  
GitHub: https://github.com/apariciodevcom

---