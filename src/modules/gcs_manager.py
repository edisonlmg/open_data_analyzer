# /path/to/your/project/gcs_manager.py
import os
import io
import sys
from pathlib import Path
import pandas as pd
# if __name__ == "__main__":
#     project_root = Path(__file__).parent.parent.parent
#     sys.path.insert(0, str(project_root))
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from src.utils import get_logger

class GCSManager:
    """
    Una clase para gestionar la conexión y operaciones con Google Cloud Storage.
    
    Carga las credenciales desde un archivo .env que debe contener la variable
    GOOGLE_APPLICATION_CREDENTIALS con la ruta al archivo JSON de la cuenta de servicio.
    """

    def __init__(self):
        """
        Inicializa el cliente de Google Cloud Storage.
        """
        self.logger = get_logger('gcs')
        try:
            # Carga las variables de entorno desde el archivo .env
            load_dotenv()
            self.client = storage.Client()
            self.logger.info("✅ Conexión exitosa con Google Cloud Storage.")
        except Exception as e:
            self.logger.error(f"❌ Error al conectar con Google Cloud Storage: {e}")
            self.client = None

    def download_csv_as_df(self, bucket_name: str, source_blob_name: str) -> pd.DataFrame | None:
        """
        Descarga un archivo CSV desde un bucket de GCS y lo carga en un DataFrame de pandas.

        :param bucket_name: El nombre del bucket de GCS.
        :param source_blob_name: La ruta completa del archivo dentro del bucket (ej: 'data/input.csv').
        :return: Un DataFrame de pandas con los datos o None si ocurre un error.
        """
        if not self.client:
            self.logger.error("❌ Cliente de GCS no inicializado.")
            return None

        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            
            self.logger.info(f"⬇️ Descargando archivo '{source_blob_name}' del bucket '{bucket_name}'...")
            
            # Descargar el contenido del archivo como bytes
            file_bytes = blob.download_as_bytes()
            
            # Usar io.BytesIO para leer los bytes como un archivo en memoria
            df = pd.read_csv(io.BytesIO(file_bytes))
            
            self.logger.info("✅ Archivo descargado y cargado en DataFrame exitosamente.")
            return df
        except NotFound:
            self.logger.error(f"❌ Error: El archivo '{source_blob_name}' no se encontró en el bucket '{bucket_name}'.")
            return None
        except Exception as e:
            self.logger.error(f"❌ Ocurrió un error inesperado al descargar: {e}", exc_info=True)
            return None

    def upload_df_as_csv(self, df: pd.DataFrame, bucket_name: str, destination_blob_name: str):
        """
        Sube un DataFrame de pandas a un bucket de GCS como un archivo CSV.

        :param df: El DataFrame de pandas que se va a subir.
        :param bucket_name: El nombre del bucket de GCS.
        :param destination_blob_name: La ruta completa donde se guardará el archivo (ej: 'data/output.csv').
        """
        if not self.client:
            self.logger.error("❌ Cliente de GCS no inicializado.")
            return

        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            self.logger.info(f"⬆️ Subiendo DataFrame a '{destination_blob_name}' en el bucket '{bucket_name}'...")

            # Convertir DataFrame a CSV en formato string, sin incluir el índice
            csv_data = df.to_csv(index=False)
            
            # Subir el string como un archivo
            blob.upload_from_string(csv_data, content_type='text/csv')
            
            self.logger.info(f"✅ DataFrame subido exitosamente a: gs://{bucket_name}/{destination_blob_name}")
        except Exception as e:
            self.logger.error(f"❌ Ocurrió un error al subir el DataFrame: {e}", exc_info=True)

