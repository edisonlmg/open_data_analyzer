# src/main.py

from src.modules.data_fetcher import download_dataset
from src.modules.data_processing import process_dataset

def main():
    # --- Funcion principal que orquesta la descarga y el procesamiento de datos ---
    print("Iniciando el proceso principal...")
    try:
        # 1. Recibe tanto la bandera booleana como la lista de archivos nuevos
        download_check, urls_to_check = download_dataset()
    except Exception as e:
        print(f"Ha ocurrido un error critico durante la descarga: {e}")
        download_check = False
    # --- 2. Si hubo descargas, ejecuta la siguiente funcion ---
    if download_check:
        print(f"\nNuevos archivos fueron descargados. Iniciando el procesamiento...")
        try:
            # Pasa la lista de archivos como argumento
            process_dataset(urls_to_check)
            print("Procesamiento completado exitosamente.")
        except Exception as e:
            print(f"Ha ocurrido un error durante el procesamiento de los datos: {e}")
    else:
        print("\nNo se descargaron nuevos archivos. No se requiere procesamiento adicional. Fin.")

if __name__ == "__main__":
    main()