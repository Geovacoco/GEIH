# -*- coding: utf-8 -*-
"""
Created on Wed Sep  3 09:52:46 2025

@author: ADMayorP
"""

import pandas as pd
import pyreadstat
import os

nombres_bases = [
    "sas01nal2412", "sas10nal2412", "sas50nal2412", "sas60nal2412",
    "sas70nal2412", "sas80nal2412", "sas90nal2412", "sas94nal2412"
]

ruta_base = r"//Systema44/GEIH_CENSO_2018/PARALELA/BASES_ANONI_DUO_FEX/BASES_ANONI_SAS/2024/12/nacional/dat/reg"

ruta_salida = r"//sassystem/GEIH-TEMATICA/Python/Bases_parquet/2024/12/nacional/dat/reg"
os.makedirs(ruta_salida, exist_ok=True)

for nombre in nombres_bases:
    ruta_archivo = os.path.join(ruta_base, f"{nombre}.sas7bdat")
    print(f"📥 Cargando {nombre}...")
    
    df, meta = pyreadstat.read_sas7bdat(ruta_archivo, encoding="latin1")
    
    globals()[nombre] = df

    print(f"✅ {nombre} cargado correctamente.")

for nombre_original in nombres_bases:
    print(f"💾 Exportando {nombre_original}...")
    
    df = globals()[nombre_original]
    
    nombre_exportado = nombre_original.replace("sas", "", 1) + ".parquet"
   
    ruta_parquet = os.path.join(ruta_salida, nombre_exportado)
    
    df.to_parquet(ruta_parquet, engine="pyarrow", index=False)

    print(f"✅ Exportado como {nombre_exportado}")

print("\n🎉 Todo el proceso se completó correctamente con nombres sin el prefijo 'sas'.")
