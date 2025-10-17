# -*- coding: utf-8 -*-
"""
Created on  Oct 8 2025

@author: HSofan con base en programa de ADMayorP
"""

import pandas as pd
import pyreadstat
import os

nombres_bases = [
    "for2401nal","for2402nal","for2403nal","for2404nal", "for2405nal", "for2406nal",
    "for2407nal","for2408nal","for2409nal","for2410nal", "for2411nal", "for2412nal",
]

ruta_base = r"\\systema44\GEIH_CENSO_2018\PARALELA\GEIH_BOL_PARA\MODULOS\FORMACION_T\2024\nacional\dat"

ruta_salida = r"//sassystem\GEIH-TEMATICA\Python\Bases_parquet\Modulos_especiales\FPT\2024"
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
    
    nombre_exportado = nombre_original + ".parquet"
   
    ruta_parquet = os.path.join(ruta_salida, nombre_exportado)
    
    df.to_parquet(ruta_parquet, engine="pyarrow", index=False)

    print(f"✅ Exportado como {nombre_exportado}")

print("\n🎉 Todo el proceso se completó correctamente con nombres sin el prefijo 'sas'.")

