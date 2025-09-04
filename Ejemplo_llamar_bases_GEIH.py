import pandas as pd
import sys
sys.path.append(r"\\sassystem\GEIH-TEMATICA\Python\Scripts")
from MDO_18_PRUEBAS import geih_pega_parquet

BASE_DIR = r"\\sassystem\GEIH-TEMATICA\Python\Bases_parquet"
nal_2024_2025 = geih_pega_parquet(
    base_dir=BASE_DIR,
    years=(2021, 2025),                  
    months=(1, 1),                       
    vars_keep=["P6800","OCI","DSI","FFT","FEX_C18","sexo"],
    chapters=None,                       
    engine="pyarrow",
    include_cap01=False,
    verbose=True
)
