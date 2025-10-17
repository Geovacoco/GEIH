# -*- coding: utf-8 -*-
"""
Created on Fri Oct  3  2025

@author: HSofan

"""
###################################################################################
#Cargando la base de la GEIH para pegarla con la base de formacion para el trabajo#
###################################################################################

import pandas as pd
import sys
sys.path.append(r"\\sassystem\GEIH-TEMATICA\Python\Scripts")
from MDO_18_PRUEBAS import geih_pega_parquet


BASE_DIR = r"\\sassystem\GEIH-TEMATICA\Python\Bases_parquet"
nal_2023 = geih_pega_parquet(
    base_dir=BASE_DIR,
    years=(2023),                  
    months=(4, 6),                       
    vars_keep=["OCI","DSI","FFT","FEX_C18","P6430","P6040","RAMA2D_R4","P3042S1","P3042","P3043","sexo","MPIO","CLASE"],
    chapters=None,                       
    engine="pyarrow",
    include_cap01=False,
    verbose=True
)
           

###################################################################################                  
#### Cargando las bases del formación para el trabajo II trimestre de cada año####
###################################################################################

import pandas as pd
import os


ruta_base = "//sassystem/GEIH-TEMATICA/Python/Bases_parquet/Modulos_especiales/FPT/2023/"

nombres_bases = [
    "for2304nal.parquet", "for2305nal.parquet", "for2306nal.parquet"
]

dataframes = []

for nombre in nombres_bases:
    archivos_parquet = os.path.join(ruta_base, nombre)
    try:

        inicial = pd.read_parquet(archivos_parquet)
        dataframes.append(inicial)
        print(f"Archivo '{nombre}' cargado exitosamente.")
    except FileNotFoundError:
        print(f"Error: El archivo '{archivos_parquet}' no fue encontrado.")
    except Exception as e:
        print(f"Error al cargar el archivo '{nombre}': {e}")  
        

 
######### Generando la base de Formación para el trabajo inicial ########
    
fpt2023 = pd.concat(dataframes, ignore_index=True)


######### Eliminando algunas variables de la base fpt porque ya aparecen en las bases GEIH #########

fpt2023_final = fpt2023.drop(columns=['P3271', 'P6040','CLASE'])

######### Generando el pegue de la la base de Formación para el trabajo con la de GEIH ########

base_total = pd.merge(nal_2023, fpt2023_final, on=['DIRECTORIO', 'SECUENCIA_P', 'ORDEN'], how='outer')

######### Eliminando Nan ########

base_total_f= base_total.fillna('')

##################################################################################
########################## Generando la base final de fpt ########################
##################################################################################

formacion = pd.DataFrame(base_total_f)




##################################################################################
############### Generando las variables para los cálculos ########################
##################################################################################

formacion['asiste'] = ((formacion['P455'] == 1) | (formacion['P456'] == 1)).astype(int)

formacion['pt']=1

formacion['FEX_T'] = formacion['FEX_C18'] / 3000

formacion['PET'] = (formacion['P6040']  >= 15).astype(int)

formacion['OCU'] = (formacion['PET'] != 1).astype(int)
formacion['DES'] = (formacion['PET'] != 1).astype(int)
formacion['INA'] = (formacion['PET'] != 1).astype(int)

formacion['OCU'] = (formacion['OCI'] == 1).astype(int)
formacion['DES'] = (formacion['DSI'] == 1).astype(int)
formacion['INA'] = (formacion['FFT'] == 1).astype(int)

formacion['PEA'] = (formacion['PET'] != 1).astype(int)

formacion['PEA'] = ((formacion['OCU'] == 1) | (formacion['DES'] == 1)).astype(int)


formacion['edad'] = 'Menores de 15 años'
formacion.loc[(formacion['P6040'] >= 15)  & (formacion['P6040'] <= 24), 'edad'] = '15 a 24 años'
formacion.loc[(formacion['P6040'] >= 25)  & (formacion['P6040'] <= 40), 'edad'] = '25 a 40 años'
formacion.loc[(formacion['P6040'] >= 41)  & (formacion['P6040'] <= 54), 'edad'] = '41 a 54 años'
formacion.loc[(formacion['P6040'] >= 55)  , 'edad'] = '55 años y más'


formacion['sexo'] = 'Hombres'
formacion.loc[(formacion['P3271'] ==2)  , 'sexo'] = 'Mujeres'

formacion['clase_f'] = '.'
formacion.loc[(formacion['CLASE'] =='1')  , 'clase_f'] = 'Cabeceras'
formacion.loc[(formacion['CLASE'] =='2')  , 'clase_f'] = 'Centros poblados y rural disperso'



formacion['horas'] = '.'
formacion.loc[(formacion['P457'] ==1)  , 'horas'] = 'Hasta 40 horas'
formacion.loc[(formacion['P457'] ==2)  , 'horas'] = 'Entre 41 y 100 horas'
formacion.loc[(formacion['P457'] ==3)  , 'horas'] = 'Entre 101 y 600 horas'
formacion.loc[(formacion['P457'] ==4)  , 'horas'] = 'Entre 601 y 1800 horas'


formacion['modalidad'] = '.'
formacion.loc[(formacion['P458'] ==1)  , 'modalidad'] = 'Presencial'
formacion.loc[(formacion['P458'] ==2)  , 'modalidad'] = 'A distancia'

formacion['institucion'] = '.'
formacion.loc[(formacion['P459'] ==1)  , 'institucion'] = 'Públical'
formacion.loc[(formacion['P459'] ==2)  , 'institucion'] = 'Privada'


formacion['tematica'] = '.'
formacion.loc[(formacion['P457'] ==1)  , 'tematica'] = 'Educación'
formacion.loc[(formacion['P457'] ==2)  , 'tematica'] = 'Humanidades y artes'
formacion.loc[(formacion['P457'] ==3)  , 'tematica'] = 'Ciencias sociales y del comportamiento'
formacion.loc[(formacion['P457'] ==4)  , 'tematica'] = 'Periodismo e información'
formacion.loc[(formacion['P457'] ==5)  , 'tematica'] = 'Educación comercial y administración'
formacion.loc[(formacion['P457'] ==6)  , 'tematica'] = 'Derecho'
formacion.loc[(formacion['P457'] ==7)  , 'tematica'] = 'Ciencias de la vida y ciencias físicas'
formacion.loc[(formacion['P457'] ==8)  , 'tematica'] = 'Matemáticas y estadística'
formacion.loc[(formacion['P457'] ==9)  , 'tematica'] = 'Informática'
formacion.loc[(formacion['P457'] ==10)  , 'tematica'] = 'Ingeniería y profesiones afines'
formacion.loc[(formacion['P457'] ==11)  , 'tematica'] = 'Industria y producción'
formacion.loc[(formacion['P457'] ==12)  , 'tematica'] = 'Arquitectura y construcción'
formacion.loc[(formacion['P457'] ==13)  , 'tematica'] = 'Agricultura y veterinaria'
formacion.loc[(formacion['P457'] ==14)  , 'tematica'] = 'Salud y Servicios sociales'
formacion.loc[(formacion['P457'] ==15)  , 'tematica'] = 'Servicios personales'
formacion.loc[(formacion['P457'] ==16)  , 'tematica'] = 'Servicios de transporte'
formacion.loc[(formacion['P457'] ==17)  , 'tematica'] = 'Protección del medio ambiente'
formacion.loc[(formacion['P457'] ==18)  , 'tematica'] = 'Servicios de seguridad'
formacion.loc[(formacion['P457'] ==19)  , 'tematica'] = 'Sectores desconocidos o no especificados'


formacion['financiacion'] = '.'
formacion.loc[(formacion['P758'] ==1)  , 'financiacion'] = 'Pagó todo'
formacion.loc[(formacion['P758'] ==2)  , 'financiacion'] = 'Pagó una parte'
formacion.loc[(formacion['P758'] ==3)  , 'financiacion'] = 'Fue gratuito'
formacion.loc[(formacion['P758'] ==4)  , 'financiacion'] = 'Pagó la totalidad otra persona o institución'

formacion['objetivo'] = '.'
formacion.loc[(formacion['P464'] ==1)  , 'objetivo'] = 'Conseguir empleo'
formacion.loc[(formacion['P464'] ==3)  , 'objetivo'] = 'Crear su propia empresa o mejorar el manejo de ella'
formacion.loc[(formacion['P464'] ==4)  , 'objetivo'] = 'Mejorar su desempeño laboral'
formacion.loc[(formacion['P464'] ==5)  , 'objetivo'] = 'Continuar con sus estudios'
formacion.loc[(formacion['P464'] ==6)  , 'objetivo'] = 'Fue exigencia de la empresa'
formacion.loc[((formacion['P464'] ==2) | (formacion['P464'] ==7)) , 'objetivo'] = 'Otra^^'

formacion['resultado'] = '.'
formacion.loc[(formacion['P465'] ==1)  , 'resultado'] = 'Conseguir empleo'
formacion.loc[(formacion['P465'] ==3)  , 'resultado'] = 'Crear su propia empresa o mejorar el manejo de ella'
formacion.loc[(formacion['P465'] ==4)  , 'resultado'] = 'Mejorar su desempeño laboral'
formacion.loc[(formacion['P465'] ==6)  , 'resultado'] = 'Ningún resultado'
formacion.loc[((formacion['P465'] ==2) | (formacion['P465'] ==5)) , 'resultado'] = 'Otra^^'

formacion['razon'] = '.'
formacion.loc[(formacion['P467'] ==1)  , 'razon'] = 'No reporta ningún beneficio'
formacion.loc[(formacion['P467'] ==2)  , 'razon'] = 'La oferta actual de cursos no le parece interesante'
formacion.loc[(formacion['P467'] ==3)  , 'razon'] = 'Falta de recursos'
formacion.loc[(formacion['P467'] ==4)  , 'razon'] = 'Falta de cupos'
formacion.loc[(formacion['P467'] ==5)  , 'razon'] = 'Desconoce la oferta de cursos'
formacion.loc[(formacion['P467'] ==6)  , 'razon'] = 'Se considera muy joven o muy viejo'
formacion.loc[(formacion['P467'] ==7)  , 'razon'] = 'Asiste a colegio o universidad'
formacion.loc[(formacion['P467'] ==8)  , 'razon'] = 'Otra'


formacion['planes'] = '.'
formacion.loc[(formacion['P468'] ==1)  , 'planes'] = 'Planea tomar cursos'
formacion.loc[(formacion['P468'] ==2)  , 'planes'] = 'No planea tomar cursos'

formacion['certificacion'] = '.'
formacion.loc[(formacion['P469'] ==1)  , 'certificacion'] = 'Con certificación'
formacion.loc[(formacion['P469'] ==2)  , 'certificacion'] = 'Sin certificación'
formacion.loc[(formacion['P469'] ==3)  , 'certificacion'] = 'No conoce el proceso'


formacion['trim'] = '.'
formacion.loc[((formacion['time'] ==202304) | (formacion['time'] ==202305) | (formacion['time'] ==202306)) , 'trim'] = 'Abril-Junio 2023'








