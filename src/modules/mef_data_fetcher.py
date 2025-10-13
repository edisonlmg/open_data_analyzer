# src/modules/sbs_data_fetcher.py

import sys
import requests
import pandas as pd
from io import BytesIO
from time import sleep
from pathlib import Path
from requests.adapters import HTTPAdapter

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

import src.utils as utils


def download_dataset(urls: dict) -> dict[str, pd.DataFrame]:
    """
    Descarga datasets desde URLs y los carga en DataFrames de pandas.
    Maneja cortes de conexión con reintentos y descarga por streaming.
    """
    datasets = {}
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    for name, url in urls.items():
        print(f"Descargando {name} desde {url}...")
        success = False

        for attempt in range(3):  # hasta 3 intentos
            try:
                with session.get(url, stream=True, timeout=60, verify=False) as r:
                    r.raise_for_status()
                    buffer = BytesIO()
                    for chunk in r.iter_content(chunk_size=1024 * 512):  # 512 KB
                        if chunk:
                            buffer.write(chunk)
                    buffer.seek(0)
                    datasets[name] = pd.read_csv(buffer)
                    print(f"✅ {name} descargado correctamente.")
                    success = True
                    break
            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError) as e:
                print(f"⚠️ Error en intento {attempt+1}/3: {e}")
                sleep(5)  # espera antes de reintentar

        if not success:
            raise Exception(f"❌ No se pudo descargar {name} después de varios intentos.")

    return datasets



