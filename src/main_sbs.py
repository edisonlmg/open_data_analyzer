# src/main_sbs.py

import sys
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

from modules.sbs_data_fetcher import download_dataset
from modules.sbs_data_processing import process_dataset_eeff, process_dataset_tc
from modules.gcs_manager import GCSManager
from utils import get_logger


def download_base_datasets(gcs_manager: GCSManager, bucket_name: str, path_eeff: str, path_tc: str, logger) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Descarga los datasets base de EEFF y TC desde GCS."""
    logger.info(f"🔄 Descargando datasets base desde el bucket '{bucket_name}'...")
    sbs_eeff_processed = gcs_manager.download_csv_as_df(bucket_name, path_eeff)
    sbs_tc_processed = gcs_manager.download_csv_as_df(bucket_name, path_tc)
    return sbs_eeff_processed, sbs_tc_processed


def process_and_upload_eeff(files_in_memory: dict, sbs_eeff_processed: pd.DataFrame, gcs_manager: GCSManager, bucket_name: str, path_file_eeff: str, logger) -> pd.DataFrame:
    """Procesa, concatena y sube los datos de EEFF."""
    FINANCIAL_INCOME_TERMS = "INGRESOS FINANCIEROS"
    SERVICE_INCOME_TERMS = "INGRESOS POR SERVICIOS FINANCIEROS"
    NET_RESULT_TERMS = [
        "RESULTADO NETO DEL EJERCICIO",
        "UTILIDAD ( PÉRDIDA ) NETA",
        "UTILIDAD (PÉRDIDA) NETA"
    ]

    sbs_eeff_actualyzed = process_dataset_eeff(
        files_in_memory, FINANCIAL_INCOME_TERMS,
        SERVICE_INCOME_TERMS, NET_RESULT_TERMS,
        logger
    )

    if not sbs_eeff_actualyzed.empty:
        sbs_eeff_processed = pd.concat(
            [sbs_eeff_processed, sbs_eeff_actualyzed],
            axis=0, ignore_index=True
        )
        logger.info(f"💾 Guardando dataset de EEFF procesado en '{path_file_eeff}'...")
        gcs_manager.upload_df_as_csv(sbs_eeff_processed, bucket_name, path_file_eeff)

        sbs_eeff_analyzed = (
            sbs_eeff_processed
            .drop_duplicates(['PERIODO', 'ENTIDAD', 'MONEDA'], keep='last', ignore_index=True)
        )
        logger.info("💾 Guardando dataset de EEFF analizado en 'SBS_EEFF_ANALYZED.csv'...")
        gcs_manager.upload_df_as_csv(sbs_eeff_analyzed, bucket_name, 'SBS_EEFF_ANALYZED.csv')

    return sbs_eeff_processed


def process_and_upload_tc(files_in_memory: dict, sbs_tc_processed: pd.DataFrame | None, gcs_manager: GCSManager, bucket_name: str, path_file_tc: str, logger):
    """Procesa, concatena y sube los datos de Tipo de Cambio."""
    TC_TERMS = "TIPO DE CAMBIO"
    sbs_tc_actualyzed = process_dataset_tc(files_in_memory, TC_TERMS, logger)
    
    if not sbs_tc_actualyzed.empty:
        if sbs_tc_processed is not None and not sbs_tc_processed.empty:
            sbs_tc_processed = pd.concat(
                [sbs_tc_processed, sbs_tc_actualyzed],
                axis=0, ignore_index=True
            )
        else:
            sbs_tc_processed = sbs_tc_actualyzed
            
        logger.info(f"💾 Guardando dataset de TC procesado en '{path_file_tc}'...")
        gcs_manager.upload_df_as_csv(sbs_tc_processed, bucket_name, path_file_tc)


def main():
    """Función principal que orquesta la descarga, procesamiento y almacenamiento de datos de la SBS."""
    logger = get_logger('sbs')
    logger.info("--- 🚀 Iniciando el proceso principal de SBS ---")

    # --- 1. Configuración y Conexión a GCS ---
    bucket_name = 'opendataanalyzer_datas'
    path_file_eeff = 'SBS_EEFF_PROCESSED.csv'
    path_file_tc = 'SBS_TC_PROCESSED.csv'
    gcs_manager = GCSManager()

    # --- 2. Descarga de Datasets Base desde GCS ---
    sbs_eeff_processed, sbs_tc_processed = download_base_datasets(
        gcs_manager, bucket_name, path_file_eeff, path_file_tc, logger
    )

    # Si el archivo base no existe, se asume que es la primera ejecución.
    if sbs_eeff_processed is None or sbs_eeff_processed.empty:
        logger.warning(f"⚠️ No se encontró el archivo base '{path_file_eeff}' o está vacío. Se intentará descargar todos los datos históricos.")
        sbs_eeff_processed = pd.DataFrame() # Se crea un DF vacío para que el flujo continúe

    # Si el archivo de TC no existe, se asume que es la primera ejecución.
    if sbs_tc_processed is None:
        logger.warning(f"⚠️ No se encontró el archivo base '{path_file_tc}'. Se creará uno nuevo si se encuentran datos de TC.")
        sbs_tc_processed = pd.DataFrame() # Se crea un DF vacío para que el flujo continúe

    # --- 3. Detección y Descarga de Nuevos Archivos ---
    files_in_memory = download_dataset(df=sbs_eeff_processed)
    if not files_in_memory:
        logger.info("✅ No se encontraron nuevos archivos para procesar. El dataset está actualizado. Finalizando.")
        return

    # --- 4. Procesamiento de Estados Financieros (EEFF) ---
    sbs_eeff_processed = process_and_upload_eeff(
        files_in_memory, sbs_eeff_processed, gcs_manager, bucket_name, path_file_eeff, logger
    )

    # --- 5. Procesamiento de Tipo de Cambio (TC) ---
    process_and_upload_tc(
        files_in_memory, sbs_tc_processed, gcs_manager, bucket_name, path_file_tc, logger
    )

    logger.info("--- ✅ Proceso principal de SBS finalizado exitosamente. ---")

if __name__ == "__main__":
    main()
    