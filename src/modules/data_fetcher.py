# src/modules/data_fetcher.py

import os
import requests
import warnings
from tqdm import tqdm
from datetime import datetime
from itertools import product
from typing import Tuple, List
import src.utils as utils

# Deshabilitar advertencias de requests para conexiones no verificadas
warnings.filterwarnings('ignore', message='Unverified HTTPS request')


def _build_dataset_urls() -> dict:
    # --- Construye el diccionario de URLs para todos los datasets ---
    current_year = datetime.now().year
    years = list(map(str, range(2002, current_year + 1)))
    months1 = [
        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
        'Julio', 'Agosto', 'Setiembre', 'Octubre', 'Noviembre', 'Diciembre'
    ]
    months2 = [
        'en', 'fe', 'ma', 'ab', 'my', 'jn',
        'jl', 'ag', 'se', 'oc', 'no', 'di'
    ]
    months = list(zip(months1, months2))
    years_months = list(product(years, months))

    # Plantillas para nombres y URLs
    templates = {
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
    for (year, (month_long, month_short)), (name_prefix, code) in product(years_months, templates.items()):
        month_num = months1.index(month_long) + 1
        key = f'{name_prefix}_{year}{month_num:02d}'
        url = f'https://intranet2.sbs.gob.pe/estadistica/financiera/{year}/{month_long}/{code}-{month_short}{year}.XLS'
        dic_datasets_urls[key] = url

    return dic_datasets_urls


def download_dataset() -> Tuple[bool, dict[str, str]]:
    '''
    Verifica y descarga datasets que no existen localmente.

    DEVUELVE:
        Un tuple con (True/False si hubo descargas, diccionario completo de URLs verificadas).
    '''
    # --- 1. Setup inicial ---
    logger = utils.get_logger()
    logger.info(">>> Iniciando el proceso de descarga de datasets...")

    project_root = utils.get_project_root()
    output_raw_dir = os.path.join(project_root, 'data', 'raw')
    os.makedirs(output_raw_dir, exist_ok=True)
    logger.info(f"Directorio de datos crudos verificado: '{output_raw_dir}'")

    # --- 2. Construcción de URLs ---
    urls_to_check = _build_dataset_urls()
    logger.info(f"Se construyeron {len(urls_to_check)} URLs para verificar (desde 2002 hasta la fecha).")

    was_downloaded = False
    new_files_downloaded = []

    # --- 3. Bucle de verificación y descarga ---
    logger.info("Iniciando la verificación de archivos existentes y descarga de faltantes...")
    try:
        for key, url in tqdm(urls_to_check.items(), desc='Verificando datasets'):
            file_path = os.path.join(output_raw_dir, f'{key}.xls')

            # Comprueba si el archivo ya existe
            if not os.path.exists(file_path):
                # Si no existe, intenta descargarlo
                try:
                    logger.info(f"Archivo '{key}.xls' no encontrado. Intentando descarga...")
                    response = requests.get(url, verify=False, timeout=10)
                    response.raise_for_status()  # Lanza un error si el status no es 200 (OK)

                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    # Si la descarga es exitosa, se actualiza la bandera y se registra en el log
                    was_downloaded = True
                    new_files_downloaded.append(key)
                    logger.info(f"ÉXITO: '{key}.xls' descargado correctamente.")

                except requests.exceptions.RequestException as e:
                    # Si el archivo no existe en el servidor o hay un error de conexión, se registra como una advertencia
                    logger.warning(
                        f"AVISO: No se pudo descargar '{key}.xls'. El archivo podría no existir en el servidor. URL: {url}")
                    pass  # Continúa con el siguiente archivo

    except Exception as e:
        # Captura cualquier otro error inesperado durante el proceso
        logger.error(f"Error inesperado durante el bucle de descarga: {e}", exc_info=True)
        pass

    # --- 4. Resumen y finalización ---
    if was_downloaded:
        logger.info(f"Descarga finalizada. Se descargaron {len(new_files_downloaded)} nuevos archivos.")
        logger.info(f"Archivos nuevos: {', '.join(new_files_downloaded)}")
    else:
        logger.info("No se encontraron nuevos archivos para descargar. Los datos locales están actualizados.")

    logger.info(f"<<< Proceso de descarga finalizado. Hubo descargas: {was_downloaded}.")

    # Devuelve la bandera y la lista completa de URLs para que el siguiente módulo sepa qué procesar
    return was_downloaded, urls_to_check
