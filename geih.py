
from pandas import DataFrame
from pandas import read_csv

class ErrorGEIH(Exception):
    """ Clase base para excepciones personalizadas """
    pass

def _csv_name_from_url(mnt_url: str) -> str:
    """ Extrae el nombre del archivo CSV de una URL """
    return mnt_url.split('/')[-1]

def csv_to_df(mnt_url : str, delim : str = ';') -> DataFrame:
    """ Importa un CSV y lo devuelve como DataFrame """
    if isinstance(mnt_url, str) and '/' in mnt_url:
        if mnt_url.endswith('.csv'):
            df = read_csv(mnt_url, sep = delim)
            print(f'Se ha importado el archivo `{_csv_name_from_url(mnt_url)}` como DataFrame.')
            return df 
        else: 
            raise ErrorGEIH("El archivo debe tener extensión `.csv`")
    else:
        raise ErrorGEIH("El argumento debe ser una cadena de texto que represente la ruta del archivo `csv`")
    
def df_to_parquet(df: DataFrame, output_path: str) -> None:
    """ Convierte un DataFrame a formato Parquet y lo guarda en la ruta especificada """
    if isinstance(df,DataFrame): 
        df.to_parquet(output_path)
        print('El DataFrame se ha guardado en formato Parquet en la ruta especificada.')
    else : 
        raise ErrorGEIH("El argumento debe ser un DataFrame de pandas.")

