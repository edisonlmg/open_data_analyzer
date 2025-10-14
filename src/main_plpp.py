# src/main_plpp.py

import sys
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

from modules.mef_data_fetcher import download_any_url
from modules.plpp_data_processing import process_dataset_plpp
from modules.plpp_data_queries import queries_dataset_plpp
from modules.gcs_manager import GCSManager
from utils import get_logger

def main():
    """Función principal que orquesta la descarga, procesamiento y almacenamiento de datos del MEF."""
    logger_name = 'mef_plpp'
    logger = get_logger(logger_name)
    bucket_name = 'opendataanalyzer_datas'
    gcs_manager = GCSManager(logger_name)
    logger.info("--- 🚀 Iniciando el proceso principal de SBS ---")

    # Descargar datasets

    list_urls = [
        'https://fs.datosabiertos.mef.gob.pe/datastorefiles/Ley_Presupuesto_Publico_{year}.csv',
        'https://fs.datosabiertos.mef.gob.pe/datastorefiles/Proyecto_de_Ley_Presupuesto_Publico_{year}.csv'
    ]
    
    dict_plpp = download_any_url(
        'PROYECTO_LEY_PRESUPUESTO_PUBLICO',
        2021,
        list_urls
        )
    df_plpp = process_dataset_plpp(dict_plpp)
    dict_queries = queries_dataset_plpp(df_plpp)
    for name, df in dict_queries.items():
        file_name = f'{name}.csv'
        gcs_manager.upload_df_as_csv(
                df,
                bucket_name,
                file_name
            )

if __name__ == "__main__":
    main()
