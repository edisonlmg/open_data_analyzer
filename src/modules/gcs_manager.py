# src/modules/gcs_manager.py
import os
import io
from pathlib import Path
import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from src.utils import get_logger

class GCSManager:
    """
    Gestiona la conexión y las operaciones con Google Cloud Storage (GCS).
    
    Esta clase utiliza las credenciales de la cuenta de servicio de Google Cloud
    especificadas en la variable de entorno `GOOGLE_APPLICATION_CREDENTIALS`.
    
    Funciona en dos modos:
    - Local: Carga credenciales desde archivo .env
    - GitHub Actions: Usa la variable de entorno GOOGLE_APPLICATION_CREDENTIALS
      configurada por el workflow
    """
    def __init__(self):
        """
        Inicializa el cliente de Google Cloud Storage.
        """
        self.logger = get_logger('sbs')
        try:
            # Solo cargar .env si existe (ejecución local)
            # En GitHub Actions, la variable ya está configurada por el workflow
            env_file = Path('.env')
            if env_file.exists():
                self.logger.info("🔧 Ejecutando en modo local. Cargando credenciales desde .env")
                load_dotenv()
            else:
                self.logger.info("☁️ Ejecutando en modo remoto (GitHub Actions). Usando credenciales del entorno")
            
            # Verificar que las credenciales estén configuradas
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if not credentials_path:
                raise ValueError("GOOGLE_APPLICATION_CREDENTIALS no está configurado")
            
            self.client = storage.Client()
            self.logger.info("✅ Conexión exitosa con Google Cloud Storage.")
        except Exception as e:
            self.logger.error(f"❌ Error al conectar con Google Cloud Storage: {e}")
            self.client = None

    def download_csv_as_df(self, bucket_name: str, source_blob_name: str) -> pd.DataFrame | None:
        """
        Descarga un archivo CSV de GCS y lo carga en un DataFrame de pandas.
        
        Args:
            bucket_name: Nombre del bucket de GCS.
            source_blob_name: Ruta del archivo dentro del bucket (ej: 'data/input.csv').
        
        Returns:
            Un DataFrame de pandas con los datos, o None si el archivo no se encuentra
            o si ocurre un error.
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
        Sube un DataFrame de pandas a GCS como un archivo CSV.
        
        El DataFrame se convierte a formato CSV en memoria y luego se sube al bucket
        especificado. El índice del DataFrame no se incluye en el archivo CSV.
        
        Args:
            df: El DataFrame de pandas a subir.
            bucket_name: El nombre del bucket de GCS de destino.
            destination_blob_name: La ruta completa donde se guardará el archivo en el bucket.
        """
        if not self.client:
            self.logger.error("❌ Cliente de GCS no inicializado.")
            return

        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            self.logger.info(f"⬆️ Subiendo DataFrame a '{destination_blob_name}' en el bucket '{bucket_name}'...")

            # Convertir DataFrame a CSV en formato string, sin incluir el índice
            csv_data = df.to_csv(index=False, encoding='utf-8-sig') # utf-8-sig agrega el BOM para compatibilidad con Excel
            
            # Subir el string como un archivo
            blob.upload_from_string(csv_data, content_type='text/csv')
            
            self.logger.info(f"✅ DataFrame subido exitosamente a: gs://{bucket_name}/{destination_blob_name}")
        except Exception as e:
            self.logger.error(f"❌ Ocurrió un error al subir el DataFrame: {e}", exc_info=True)
