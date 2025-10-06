# src/main.py

from modules.sbs_data_fetcher import download_dataset
from modules.sbs_data_processing import process_dataset
from modules.gcs_manager import GCSManager

def main():
    # --- Funcion principal que orquesta la descarga y el procesamiento de datos ---
    print("Iniciando el proceso principal...")
    bucket_name = 'opendataanalyzer_datas'
    path_file = 'SBS_EEFF_ANALYZED.csv'
    tc = 'SBS_TC.csv'    
    try:
        # 1. Recibe tanto la bandera booleana como la lista de archivos nuevos
        gcs_manager = GCSManager()
        sbs_eeff_analyzed = gcs_manager.download_csv_as_df(bucket_name, path_file)
        was_downloaded, files_in_memory = download_dataset(sbs_eeff_analyzed)
    except Exception as e:
        print(f"Ha ocurrido un error critico durante la descarga: {e}")
        download_check = False
    # --- 2. Si hubo descargas, ejecuta la siguiente funcion ---
    if was_downloaded:
        print(f"\nNuevos archivos fueron descargados. Iniciando el procesamiento...")
        try:
            # Pasa la lista de archivos como argumento
            process_dataset(files_in_memory)
            print("Procesamiento completado exitosamente.")
        except Exception as e:
            print(f"Ha ocurrido un error durante el procesamiento de los datos: {e}")
    else:
        print("\nNo se descargaron nuevos archivos. No se requiere procesamiento adicional. Fin.")

if __name__ == "__main__":
    main()