# -*- coding: utf-8 -*-
"""
Creado: Miércoles - Septiembre: 3 17:54:27 2025
Autor:  Daniel Mayor Pachon
"""

# MDO_18.py
from pathlib  import Path
from typing   import List, Tuple, Union, Optional
from datatime import datetime #NOTE(): Toca verificar que datatime esté instalado.

import pandas as pd
import re
import time

# ---------- Parámetros globales ----------
CLAVES        = ['DIRECTORIO', 'SECUENCIA_P', 'ORDEN']
CAPITULOS_DEF = ['10', '50', '60', '70', '80', '90', '94']
_QMAP         = {1: (1,3), 2:(4,6), 3:(7,9), 4:(10,12)}
# ----------------------------------------
def usage():
    return """ USO: Para leer `years` y `months` se debe espeficar:
        <years>:
                int:   2001, 2002 ... 2025                            -- Para leer un solo año
                tuple: (2001, 2002), (2021, 2024), ... (2022, 2025)   -- Para leer varios años: `int`, pero con un año minimo y maximo
                list:  [2021, 2025, 2020], [2021, 2025, 2020, 2021]   -- Para leer varios años: `int`, independientes sin necesidad de estar ordenados
        <months>:
                int:  1, 2, 3, ..., 12                               -- Para leer un solo mes
                str: 'all', 'Q1', 'Q2', 'Q3', 'Q4'                   -- Para leer todos los doce meses o leer trimestre especificos
                tuple: (1, 2), (1, 3), (1, 4), (1, 12)               -- Para leer varios meses: `int`, pero con un mes minimo y maximo
                list:   [1, 2, 3, ..., 12]                           -- Para leer varios meses: `int`, independientes sin necesidad de estar ordenados
        """

def _build_periods(years, months):
    """ Verifica y crea `años` y `meses` con el formato deseado para posterior procesamiento """
    if isinstance(years, int)
        years = [years]
    elif isinstance(years, tuple) and len(years) == 2:
        years = list(range(years[0], years[1] + 1))
    else:
        years = list(years)

    current_year = datatime.now().year
    if min(years) < 2001:
        raise ValueError("El año debe ser mayor o igual a 2001.")
    if max(years) > current_year:
        raise ValueError("El año debe ser menor o igual al año actual.")

    if isinstance(months, str):
        if months.lower() == "all":
            months = list(range(1, 13))
        elif months.upper().startswith("Q"):
            if not re.fullmatch(r"Q[1-4]", months.upper()):
                raise ValueError("Para leer trimestres debe especificar: `Q1, Q2, Q3 o Q4`")
            q = int(months[1]); a, b = _QMAP[q]; months = list(range(a, b + 1))
        else:
            raise ValueError("Formato de 'months' no reconocido.")
    elif isinstance(months, tuple) and len(months) == 2:
        months = list(range(months[0], months[1] + 1))
    elif isinstance(months, list):
        out = []
        for m in months:
            if isinstance(m, int):
                out.append(m)
            elif isinstance(m, str) and m.upper().startswith("Q"):
                q = int(m[1]); a, b = _QMAP[q]; out.extend(range(a, b + 1))
            else:
                raise ValueError("Elemento de 'months' no reconocido.")
        months = sorted(set(out))
    else:
        raise ValueError("Tipo de 'months' no reconocido.")

    return [(y, m) for y in years for m in months]

def geih_pega_parquet(
    base_dir,
    years,
    months,
    vars_keep=None,
    chapters=None,
    engine="pyarrow",
    include_cap01=False,
    verbose=True
):
    """
    Lee capítulos Parquet por periodo y registro 
    """
    t0 = time.time()
    base_dir = Path(base_dir)
    chapters = list(chapters) if chapters else CAPITULOS_DEF.copy()
    if include_cap01 and '01' not in chapters:
        chapters = ['01'] + chapters

    # Derivadas que NO deben disparar alerta si se piden en vars_keep
    derived_ok = {'año', 'mes', 'time', 'trim', 'sexo'}

    derived_deps = {}
    if vars_keep:
        vars_keep = list(vars_keep)
        if 'sexo' in vars_keep:
            derived_deps['sexo'] = ['P3271']  
        # hay que ralizar esto para crear nuevas variables:
        

    periodos = _build_periods(years, months)
    data_por_periodo = []

    for anio, mes in sorted(periodos):
        mes_str = f"{mes:02d}"
        sufijo = f"{str(anio)[2:]}{mes_str}"
        carpeta = base_dir / str(anio) / mes_str / "nacional" / "dat" / "reg"
        if verbose:
            print(f"\n🔄 Procesando {anio}-{mes_str} ...")

        data_capitulos = []
        columnas_vistas_periodo = set(CLAVES)

        for cod in chapters:
            archivo = carpeta / f"{cod}nal{sufijo}.parquet"
            if archivo.exists():
                try:
                    df = pd.read_parquet(archivo, engine=engine)

                    if all(c in df.columns for c in CLAVES):
                        columnas_vistas_periodo.update(df.columns.tolist())

                        if vars_keep:                            
                            cols_presentes = [c for c in vars_keep if c in df.columns]                            
                            for der, deps in (derived_deps or {}).items():
                                if der in vars_keep:
                                    cols_presentes += [d for d in deps if d in df.columns]
                            cols = CLAVES + cols_presentes
                            cols = list(dict.fromkeys([c for c in cols if c in df.columns]))
                            df = df.loc[:, cols]
                        else:
                            df = df.loc[:, ~df.columns.duplicated()]

                        data_capitulos.append(df)
                        if verbose:
                            print(f"  ✔ {archivo.name}  ({df.shape[0]:,} filas, {df.shape[1]} cols)")
                    else:
                        falt = [c for c in CLAVES if c not in df.columns]
                        if verbose:
                            print(f"  ⚠ {archivo.name} omitido (faltan llaves: {falt})")
                except Exception as e:
                    if verbose:
                        print(f"  ⚠ Error al leer {archivo.name}: {e}")
            else:
                if verbose:
                    print(f"  ❌ No encontrado: {archivo.name}")

        # Alerta de faltantes (ignora derivadas(variables creadas))
        if vars_keep:
            faltantes_mes = sorted([
                v for v in vars_keep
                if v not in columnas_vistas_periodo and v not in CLAVES and v not in derived_ok
            ])
            if faltantes_mes and verbose:
                print(f"  💩 Mes {mes_str} (Año {anio}): variables faltantes 💩 -> {faltantes_mes}")

        if data_capitulos:
            df_merge = data_capitulos[0]
            for df_next in data_capitulos[1:]:
                comunes = set(df_merge.columns).intersection(df_next.columns) - set(CLAVES)
                if comunes:
                    df_next = df_next.drop(columns=list(comunes))
                df_merge = pd.merge(df_merge, df_next, on=CLAVES, how='outer')

            df_merge["año"] = anio
            df_merge["mes"] = mes
            # derivadas por periodo (fuera de filtros)
            df_merge["time"] = df_merge["año"] * 100 + df_merge["mes"]
            if "P3271" in df_merge.columns:
                df_merge["sexo"] = df_merge["P3271"].map({1: "Hombre", 2: "Mujer"})

            data_por_periodo.append(df_merge)
            if verbose:
                print(f"  😀 Pegue de resgistros {anio}-{mes_str}: {len(df_merge):,} registros")
        else:
            if verbose:
                print(f"  ⚠ Sin capítulos combinables para {anio}-{mes_str}")

    if data_por_periodo:
        texto = pd.concat(data_por_periodo, ignore_index=True)

        if vars_keep:            
            if 'sexo' in vars_keep and 'sexo' not in texto.columns and 'P3271' in texto.columns:
                texto['sexo'] = texto['P3271'].map({1: 'Hombre', 2: 'Mujer'})

            orden = [c for c in CLAVES if c in texto.columns] \
                  + [c for c in vars_keep if c in texto.columns] \
                  + [c for c in ['año', 'mes', 'time'] if c in texto.columns]
            orden += [c for c in texto.columns if c not in orden]
            texto = texto.loc[:, orden]

        if verbose:
            print(f"\n✅ Base final: {len(texto):,} filas, {texto.shape[1]} columnas")
            resumen = texto.groupby(['año', 'mes']).size().reset_index(name='registros')
            print("\n📋 Registros por año y mes:")
            print(resumen)
    else:
        texto = pd.DataFrame()
        if verbose:
            print("\n❌ No se cargó ningún periodo válido.")

    if verbose:
        print(f"\n⏱ Tiempo total: {time.time() - t0:.2f} s")

    return texto











