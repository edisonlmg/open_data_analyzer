# src/main_sbs.py

import sys
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

from modules.mef_data_fetcher import download_dataset
from modules.inv_data_processing import process_dataset_inv
# from modules.gcs_manager import GCSManager
from utils import get_logger


def main():
    """Función principal que orquesta la descarga, procesamiento y almacenamiento de datos del MEF."""
    logger = get_logger('mef')
    logger.info("--- 🚀 Iniciando el proceso principal de SBS ---")

    # Descargar datasets

    dic_urls = {
        'DETALLE_INVERSIONES': 'https://fs.datosabiertos.mef.gob.pe/datastorefiles/DETALLE_INVERSIONES.csv',
        'CIERRE_INVERSIONES': 'https://fs.datosabiertos.mef.gob.pe/datastorefiles/CIERRE_INVERSIONES.csv',
        'INVERSIONES_DESACTIVADAS': 'https://fs.datosabiertos.mef.gob.pe/datastorefiles/INVERSIONES_DESACTIVADAS.csv'
    }

    inv_datasets = download_dataset(dic_urls)
    inv_datasets = process_dataset_inv(inv_datasets)
    logger.info("✅ Datasets de inversiones descargados correctamente.")
    return inv_datasets

if __name__ == "__main__":
    main()
