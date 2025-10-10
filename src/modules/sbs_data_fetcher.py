# src/modules/sbs_data_fetcher.py

import os
import sys
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from datetime import datetime
from itertools import product

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

import src.utils as utils

def _expected_dates(start_year: int = 2002, period: str = 'M') -> set[str]:
    # --- Construye la lista de fechas esperadas en formato 'AAAAMM' ---
    if period not in ['M', 'Q']:
        raise ValueError("El parámetro 'period' debe ser 'M' (mensual) o 'Q' (trimestral).")
    if start_year > datetime.now().year:
        raise ValueError(f"El 'start_year' no debe ser mayor al año actual.")
    current_year = datetime.now().year
    range_years = list(range(start_year, current_year + 1))
    range_months = list(range(1, 13)) if period == 'M' else [3, 6, 9, 12]
    expected_dates = set(
        [str(year)+str(month).zfill(2) for year in range_years for month in range_months]
        )
    return expected_dates

def _existing_dates(df: pd.DataFrame, doc_type: str, type_col: str = 'TIPO', 
                    date_col: str = 'DATE') -> set[str]:
    # --- Extrae las fechas existentes en el DataFrame en formato 'AAAAMM' ---
    df = df[df[type_col] == doc_type]
    df[date_col] = df[date_col].astype(str)
    existing_dates = set(df[date_col])
    return existing_dates

def _missing_dates(df: pd.DataFrame|None, doc_type: str, type_col: str = 'TIPO', date_col: str = 'DATE',
                    period: str = 'M', start_year: int = 2002) -> list[str]:
    # --- Identifica las fechas faltantes en el dataset ---
    if df is None or df.empty:
        missing_dates = sorted(list(_expected_dates(start_year, period)))
    else:
        expected_dates = _expected_dates(start_year, period)
        existing_dates = _existing_dates(df, doc_type, type_col, date_col)
        if not existing_dates.issubset(expected_dates):
            raise ValueError("El DataFrame contiene fechas fuera del rango esperado.")
        missing_dates = sorted(list(expected_dates - existing_dates))
    return missing_dates
    
def _tuples_dates(df: pd.DataFrame, doc_type: str, type_col: str = 'TIPO', date_col: str = 'DATE', period: str = 'M', 
                   start_year: int = 2002) -> tuple:
    # --- Construye la lista de combinaciones de año y mes para formar las url ---
    months_map = [
        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
        'Julio', 'Agosto', 'Setiembre', 'Octubre', 'Noviembre', 'Diciembre'
    ]
    months_short_map = [
        'en', 'fe', 'ma', 'ab', 'my', 'jn',
        'jl', 'ag', 'se', 'oc', 'no', 'di'
    ]
    missing_dates = _missing_dates(df, doc_type, type_col, date_col, period, start_year)
    tuples_dates = tuple()
    for date in missing_dates:
        year = date[:4]
        month = int(date[4:])
        tuples_dates += (
            (year, (str(month).zfill(2),months_map[month-1], months_short_map[month-1])),
            )
    return tuples_dates

def _build_dic_dataset_urls(df: pd.DataFrame, type_col: str = 'TIPO', date_col: str = 'DATE', 
                            start_year: int = 2002) -> dict:
    # --- Construye las URLs de los datasets faltantes ---
    # Plantillas para nombres y URLs
    urls_templates = {
        'Banca_Multiple_EEFF': 'B-2201',
        'Banca_Multiple_Ratios': 'B-2401',
        'Empresas_Financieras_EEFF': 'B-3101',
        'Empresas_Financieras_Ratios': 'B-3301',
        'Cajas_Municipales_EEFF': 'C-1101',
        'Cajas_Municipales_Ratios': 'C-1301',
        'Cajas_Rurales_EEFF': 'C-2101',
        'Cajas_Rurales_Ratios': 'C-2301',
        'Empresas_Crediticias_EEFF': 'C-4103',
        'Empresas_Crediticias_Ratios': 'C-4301',
        'Cooperativas_Nivel3_EEFF': 'SC-0002',
        'Cooperativas_Nivel2b_EEFF': 'SC-0003',
        'Cooperativas_Nivel2a_EEFF': 'SC-0004',
        'Cooperativas_Nivel1_EEFF': 'SC-0005'
    }
    dic_datasets_urls = {}
    for key, value in urls_templates.items():
        period = 'Q' if key in ['Cooperativas_Nivel2a_EEFF','Cooperativas_Nivel1_EEFF'] else 'M'
        start_year = 2023 if key.startswith('Cooperativas') else 2002
        doc_type = ' '.join(key.split('_')[:-1])
        tuples_dates = _tuples_dates(df, doc_type, type_col, date_col, period, start_year= start_year)
        for (year, (month, month_long, month_short)), (name_prefix, code) in product(tuples_dates, [(key, value)]):
            key = f'{name_prefix}_{year}{month}'
            url = f'https://intranet2.sbs.gob.pe/estadistica/financiera/{year}/{month_long}/{code}-{month_short}{year}.XLS'
            dic_datasets_urls[key] = url
    return dic_datasets_urls

def download_dataset(df: pd.DataFrame | None, type_col: str = 'TIPO', date_col: str = 'DATE', 
                     start_year: int = 2002) -> dict[str, BytesIO]:
    """
    Descarga datasets y los mantiene en memoria (BytesIO) sin guardarlos localmente.
    
    Returns:
        Tuple con:
        - bool: Si hubo descargas
        - Dict[str, BytesIO]: Diccionario con nombre_archivo -> contenido en memoria
    """
    logger = utils.get_logger('SBS')
    logger.info(">>> Realizando el proceso de descarga de datasets en memoria...")
    build_dic_dataset_urls = _build_dic_dataset_urls(df, type_col, date_col, start_year)
    # Diccionario para almacenar archivos en memoria
    files_in_memory = {}
    for file_name, url in build_dic_dataset_urls.items():
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Crear objeto BytesIO en memoria
                file_memory = BytesIO(response.content)
                files_in_memory[file_name] = file_memory
                logger.info(f"Archivo cargado en memoria: {file_name}.xls desde {url}")
            else:
                logger.warning(f"Archivo {file_name} no encontrado en la URL: {url} (Status code: {response.status_code})")
        except requests.RequestException as e:
            logger.error(f"Error al descargar el archivo desde {url}: {e}")
    
    if bool(files_in_memory):
        logger.info(f"Descarga finalizada. Se cargaron {len(files_in_memory)} archivos en memoria.")
    else:
        logger.info("No se encontraron archivos para descargar.")
    
    logger.info(f"<<< Proceso de descarga finalizado. Hubo descargas en memoria?: {bool(files_in_memory)}.")
    return files_in_memory

if __name__ == "__main__":
    bucket_name = 'opendataanalyzer_datas'
    path_file = 'SBS_EEFF_PROCESSED.csv'
    gcs_manager = utils.GCSManager()
    sbs_eeff_analyzed = gcs_manager.download_csv_as_df(bucket_name, path_file)
    if sbs_eeff_analyzed is not None:
        was_downloaded, files_in_memory = download_dataset(sbs_eeff_analyzed)
        if was_downloaded:
            print(f"Se cargaron {len(files_in_memory)} archivos en memoria.")
        else:
            print("No se encontraron archivos para descargar.")
    else:
        print(f"Error: No se pudo cargar el archivo {path_file} desde GCS.")
