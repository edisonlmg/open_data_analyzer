"""
Import libraries
"""

import os
import ssl
import warnings
from datetime import datetime as dt

import pandas as pd
import pyfiglet
import requests
import urllib3
from colorama import Fore, Style
from dotenv import load_dotenv

"""
Set settings
"""

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

"""
Title
"""

print(
    Fore.GREEN 
    + Style.BRIGHT 
    + pyfiglet.figlet_format("Open Data Analyzer") 
    + Style.RESET_ALL
    )

"""
Start timer
"""

t0 = dt.now()
print(
    Fore.YELLOW
    + "\nHora de inicio:"
    + t0.strftime("%H:%M:%S")
    + Style.RESET_ALL
    )


"""
Subtitle
"""

print(
Fore.BLUE
+ Style.BRIGHT
+ """\n
=============================================
Descarga de datasets ODMEF
=============================================
\n"""
+ Style.RESET_ALL
)

"""
Define functions
"""

def str_to_date(date_str):
    return dt.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").date()

"""
Load urls
"""

dic_urls = {
    **{
        "DETALLE_INVERSIONES": os.getenv("url_detalle_inv"),
        "CIERRE_INVERSIONES": os.getenv("url_cierre_inv"),
        "INVERSIONES_DESACTIVADAS": os.getenv("url_inv_desactivadas"),
    },
    **{f"{year}-Gasto-Devengado": os.getenv(f"url_gasto{year}") for year in range(2012, 2026)},
    **{f"{year}-Gastos-ETES": os.getenv(f"url_etes{year}") for year in range(2015, 2026)},
    **{f"{year}-Gasto-OPDS": os.getenv(f"url_opds{year}") for year in range(2018, 2026)},
}

"""
Update dates off
"""

with open("data/raw/datesoff.pkl", "rb") as file:
    dic_dates = pd.read_pickle(file)

print("\nActualizando fechas de corte de los datasets...", end="")

dic_dates_new = {}

for num, (key, value) in enumerate(dic_urls.items()):
    response = requests.head(dic_urls[key], verify=False)
    dic_dates_new[key] = str_to_date(response.headers["Last-Modified"])

print(" Ok \u2705")

"""
Download datasets
"""

total = len(dic_urls)
dic_datasets = {}

print("\nCargando datasets: \n")

for num, (key, url) in enumerate(dic_urls.items()):

    if dic_dates[key] != dic_dates_new[key]:
        print(f"\tDescargando {key}: data {num + 1}/{total}...", end="")
        dic_datasets[key] = pd.read_csv(url)
        dic_datasets[key].to_pickle(f"data/raw/{key}.pkl.bz2")
        print(" Ok \u2705")
    else:
        print(f"\tAbriendo {key}: data {num + 1}/{total}...", end="")
        dic_datasets[key] = pd.read_pickle(f"data/raw/{key}.pkl.bz2")
        print(" Ok \u2705")

print("\nDatasets cargados correctamente \u2705")

"""
Save dates off
"""

with open("data/raw/datesoff.pkl", "wb") as file:
    pd.to_pickle(dic_dates_new, file)







