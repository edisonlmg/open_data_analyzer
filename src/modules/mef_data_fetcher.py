# src/modules/mef_data_fetcher.py

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


def download_url(url: str, retries: int = 3, timeout: int = 60) -> pd.DataFrame | None:
    """
    Descarga un solo dataset desde una URL y lo carga en un DataFrame.
    Retorna None si no se pudo descargar después de varios intentos.
    """
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    for attempt in range(retries):
        try:
            with session.get(url, stream=True, timeout=timeout, verify=False) as r:
                r.raise_for_status()
                buffer = BytesIO()
                for chunk in r.iter_content(chunk_size=1024 * 512):  # 512 KB
                    if chunk:
                        buffer.write(chunk)
                buffer.seek(0)
                df = pd.read_csv(buffer)
                print(f"✅ descargado correctamente.")
                return df
        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError) as e:
            print(f"⚠️ Error en intento {attempt+1}/{retries}: {e}")
            sleep(5)

    print(f"❌ No se pudo descargar después de varios intentos.")
    return None


def download_dict_urls(urls: dict[str, str]) -> dict[str, pd.DataFrame]:
    """
    Descarga múltiples datasets desde un diccionario de URLs.
    Usa download_single_dataset() para cada entrada y devuelve
    solo los datasets descargados exitosamente.
    """
    datasets = {}
    for name, url in urls.items():
        df = download_url(url)
        if df is not None:
            datasets[name] = df
    return datasets

def download_any_url(key: str, start_year: int, urls: list[str]) -> dict[str, pd.DataFrame]:
    """
    Descarga datasets anuales desde una lista de URLs (con placeholders para {year}).
    Si una URL falla, intenta con la siguiente.
    """
    dict_df = {}
    year = start_year
    position = 0

    while position < len(urls):
        url_template = urls[position]
        try:
            print(f"🔽 Intentando descargar {key}_{year} desde {url_template.format(year=year)}")
            df = download_url(url_template.format(year=year))
            dict_df[f"{key}_{year}"] = df
            print(f"✅ Descargado {key}_{year}")
            year += 1  # pasa al siguiente año si fue exitoso
        except Exception as e:
            print(f"⚠️ Error con {url_template.format(year=year)}: {e}")
            position += 1  # prueba con la siguiente URL
            if position < len(urls):
                print(f"🔁 Cambiando a la siguiente URL ({position+1}/{len(urls)})...")
            else:
                print("⛔ No quedan URLs disponibles. Finalizando.")
                break

    return dict_df
