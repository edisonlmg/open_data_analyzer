# src/main.py

import sys
import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

from modules.sbs_data_fetcher import download_dataset
from modules.sbs_data_processing import process_dataset_eeff, process_dataset_tc
import src.utils as utils

def main():
    # --- Funcion principal que orquesta la descarga y el procesamiento de datos ---
    FINANCIAL_INCOME_TERMS = "INGRESOS FINANCIEROS"
    SERVICE_INCOME_TERMS = "INGRESOS POR SERVICIOS FINANCIEROS"
    NET_RESULT_TERMS = [
        "RESULTADO NETO DEL EJERCICIO",
        "UTILIDAD ( PÉRDIDA ) NETA",
        "UTILIDAD (PÉRDIDA) NETA"
    ]
    TC_TERMS = "TIPO DE CAMBIO"
    bucket_name = 'opendataanalyzer_datas'
    path_file_eeff = 'SBS_EEFF_PROCESSED.csv'
    path_file_tc = 'SBS_TC_PROCESSED.csv'
    gcs_manager = utils.GCSManager()
    sbs_eeff_processed = gcs_manager.download_csv_as_df(bucket_name, path_file_eeff)
    sbs_tc_processed = gcs_manager.download_csv_as_df(bucket_name, path_file_tc)
    if sbs_eeff_processed is not None:
        files_in_memory = download_dataset(sbs_eeff_processed)
        if bool(files_in_memory):
            sbs_eeff_actualyzed = process_dataset_eeff(
                files_in_memory, FINANCIAL_INCOME_TERMS,
                SERVICE_INCOME_TERMS, NET_RESULT_TERMS,
                utils.get_logger('SBS')
                )
            sbs_eeff_processed = pd.concat(
                [sbs_eeff_processed, sbs_eeff_actualyzed], 
                axis=0, ignore_index=True
                )
            gcs_manager.upload_df_as_csv(
                sbs_eeff_processed, bucket_name, path_file_eeff
                )
            sbs_eeff_analyzed = (
                sbs_eeff_processed
                .drop_duplicates(
                    ['PERIODO','ENTIDAD','MONEDA'],
                    keep='last',
                    ignore_index=True
                    )
                )
            gcs_manager.upload_df_as_csv(
                sbs_eeff_analyzed, bucket_name, 'SBS_EEFF_ANALYZED.csv'
                )
            sbs_tc_actualyzed = process_dataset_tc(
                files_in_memory, TC_TERMS,
                utils.get_logger('SBS')
                )
            sbs_tc_processed = pd.concat(
                [sbs_tc_processed, sbs_tc_actualyzed], 
                axis=0, ignore_index=True
                )
            gcs_manager.upload_df_as_csv(
                sbs_tc_processed, bucket_name, path_file_tc
                )
        else:
            pass
    else:
        pass

if __name__ == "__main__":
    main()