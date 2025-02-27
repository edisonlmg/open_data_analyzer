"""
Import libraries
"""

import os
import ssl
import warnings
from datetime import datetime as dt

import pandas as pd
import urllib3
from dotenv import load_dotenv
import pyfiglet
from colorama import Fore, Style
import requests

"""
Set settings
"""

os.chdir('../../')
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

"""
Title
"""

print(Fore.GREEN + Style.BRIGHT + pyfiglet.figlet_format("Extract ODMEF") + Style.RESET_ALL)

"""
Define functions
"""

print("\nPreparando...", end="")

def date_transform(date_str):
    return dt.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y%m%d")

"""
Define urls
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

print(" Ok \u2705")

"""
Extract datesoff
"""

print("\nExtrayendo fechas de corte de los datasets...", end="")

dic_dates = {}

for num, (key, value) in enumerate(dic_urls.items()):
    
    response = requests.head(dic_urls[key], verify=False)

    dic_dates[key] = date_transform(response.headers["Last-Modified"])

print(" Ok \u2705")


"""
Download datasets
"""

total = len(dic_urls)

dic_datesets = {}

t0 = dt.now()

print("\nDescargando datasets: \n")

for num, (key, value) in enumerate(dic_urls.items()):

    if not os.path.exists(f"data/raw/{key}_{dic_dates[key]}.pkl.bz2"):

        print(f"\tDescargando {key}: data {num + 1}/{total}...", end="")
        
        dic_datesets[key] = pd.read_csv(value)
        dic_datesets[key].to_pickle(f"data/raw/{key}_{dic_dates[key]}.pkl.bz2")
        
        print(" Ok \u2705")

    else:

        print(f"\tAbriendo {key}: data {num + 1}/{total}...", end="")

        dic_datesets[key] = pd.read_pickle(f"data/raw/{key}_{dic_dates[key]}.pkl.bz2")

        print(" Ok \u2705")

t1 = dt.now()

print(f"\nDatasets descargados en:", str(t1 - t0).split(".")[0])
