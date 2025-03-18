"""
Import libraries
"""

import numpy as np
import pandas as pd
from colorama import Fore, Style

from src.extract_odmef import dic_datasets

"""
Subtitle
"""

print(
Fore.BLUE
+ Style.BRIGHT
+ """\n
=============================================
Transformar datasets ODMEF
=============================================
\n"""
+ Style.RESET_ALL
)


"""
Load datasets
"""

activas = dic_datasets["DETALLE_INVERSIONES"]
cerradas = dic_datasets["CIERRE_INVERSIONES"]
desactivadas = dic_datasets["INVERSIONES_DESACTIVADAS"]

"""
Process datasets
"""

print("\nUniendo datasets de inversiones...")

activas = activas.assign(
    DEVENGADO_ACUMULADO=activas["DEV_ANIO_ACTUAL"]
    + activas["DEVEN_ACUMUL_ANIO_ANT"]
).rename(
    {
        "NOMBRE_OPMI": "NOM_OPMI",
        "NOMBRE_UF": "NOM_UF",
        "NOMBRE_UEI": "NOM_UEI",
        "NOMBRE_UEP": "NOM_UEP",
    },
    axis=1,
)

cerradas = cerradas.rename({"DEVEN_ACUMULADO": "DEVENGADO_ACUMULADO"}, axis=1)

desactivadas = desactivadas.rename({"COD_SNIP": "CODIGO_SNIP"}, axis=1)

inv = (
    pd.concat([activas, cerradas, desactivadas], axis=0, ignore_index=True)
    .drop_duplicates(subset="CODIGO_SNIP")
    .reset_index(drop=True)
)

inv = (
    inv.drop_duplicates("CODIGO_SNIP")
    .dropna(subset=["CODIGO_SNIP", "ESTADO", "SITUACION", "FECHA_REGISTRO"])
    .reset_index(drop=True)
)

print("\nTransformar dataset de inversiones...")

# Convertir fechas a formato de fechas

fechas = [
    "FECHA_REGISTRO",
    "FECHA_VIABILIDAD",
    "FEC_REG_F9",
    "FECHA_ULT_ACT_F12B",
    "ULT_FEC_DECLA_ESTIM",
    "FEC_INI_EJECUCION",
    "FEC_FIN_EJECUCION",
    "FEC_INI_EJEC_FISICA",
    "FEC_FIN_EJEC_FISICA",
    "FEC_CIERRE",
    "FEC_INI_OPER",
]

for fecha in fechas:
    inv[fecha] = inv[fecha].str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    inv[fecha] = pd.to_datetime(inv[fecha], format="%Y-%m-%d", errors="coerce")

# Modificar columnas

inv = (
    inv.assign(
        SALDO=np.where(
            inv["DEVENGADO_ACUMULADO"] > inv["COSTO_ACTUALIZADO"],
            0,
            inv["COSTO_ACTUALIZADO"] - inv["DEVENGADO_ACUMULADO"],
        ),
        EJECUCION=np.where(
            inv["COSTO_ACTUALIZADO"] == 0,
            0,
            inv["DEVENGADO_ACUMULADO"] / inv["COSTO_ACTUALIZADO"],
        ),
        COSTO_ACTUALIZADO=inv["COSTO_ACTUALIZADO"] - inv["CTRL_CONCURR"],
        ANIO_REGISTRO=inv["FECHA_REGISTRO"].dt.year,
        ANIO_VIABILIDAD=inv["FECHA_VIABILIDAD"].dt.year,
    )
    .replace(
        {
            "ESTADO": {
                "DESACTIVADO PERMANENTE": "DESACTIVADO",
                "DESACTIVADO ": "DESACTIVADO",
                "DESACTIVADO TEMPORAL": "DESACTIVADO",
                "DESACTIVADO   PERMANENTE": "DESACTIVADO",
            },
            "SITUACION": {
                "VIABLE": "VIABLE/APROBADO",
                "APROBADO": "VIABLE/APROBADO",
                "NO VIABLE": "NO VIABLE/NO APROBADO",
                "NO APROBADO": "NO VIABLE/APROBADO",
            },
            "DEPARTAMENTO": {"-MUL.DEP-": "_MULTIDEPARTAMENTO_"},
        }
    )
    .fillna({"DEPARTAMENTO": "_MULTIDEPARTAMENTO_"})
)

# Categorizar variables

# inv["ESTADO"] = pd.Categorical(
#     inv["ESTADO"], categories=["ACTIVO", "CERRADO", "DESACTIVADO"]
# )

# inv["SITUACION"] = pd.Categorical(
#     inv["SITUACION"],
#     categories=["VIABLE/APROBADO", "NO VIABLE/NO APROBADO", "EN FORMULACION"],
# )

# inv["NIVEL"] = pd.Categorical(inv["NIVEL"], categories=["GN", "GR", "GL"])


"""
Segmentar inversiones por rango de costo
"""

inv["RANGO"] = pd.cut(
    inv["COSTO_ACTUALIZADO"],
    bins=[
        inv["COSTO_ACTUALIZADO"].min() - 1,
        5e6,
        10e6,
        300e6,
        1000e6,
        inv["COSTO_ACTUALIZADO"].max() + 1,
    ],
    labels=[
        "(0;5]",
        "(5;10]",
        "(10;300]",
        "(300;1,000]",
        "(1,000;+]",
    ],
)

"""
Modalidades de ejecución
"""

inv = (
    inv
    .assign(
        MODALIDAD=lambda df: df["DES_MODALIDAD"].fillna(np.nan)
    )
    .assign(
        MODALIDAD=lambda df: np.where(
            df["MODALIDAD"].str.contains("^[^a-zA-Z]*$", regex=True, na=False), 
            np.nan, 
            df["MODALIDAD"]
            )
    )
    .assign(
        MODALIDAD=lambda df: np.where(
            ~df["MODALIDAD"].str.contains("ADMINISTRACIÓN DIRECTA", na=True), 
            "ADMINISTRACIÓN INDIRECTA", 
            df["MODALIDAD"]
            )
    )
    .assign(
        MODALIDAD=lambda df: np.where(
            ~df["MODALIDAD"].str.contains("ADMINISTRACIÓN INDIRECTA", na=True), 
            "ADMINISTRACIÓN DIRECTA", 
            df["MODALIDAD"]
        )
    )
    .assign(
        MODALIDAD=lambda df: np.where(
            ~(df["MODALIDAD"].isin(["ADMINISTRACIÓN DIRECTA", "ADMINISTRACIÓN INDIRECTA"])
              | df["MODALIDAD"].isnull()), 
              "MIXTA", 
              df["MODALIDAD"]
        )
    )
    .replace(
        {
            "MODALIDAD" : {
                "ADMINISTRACIÓN DIRECTA":"DIRECTA",
                "ADMINISTRACIÓN INDIRECTA":"INDIRECTA"
            }
        }
    )
)

inv["MODALIDAD"] = inv["MODALIDAD"].fillna("NO REGISTRADO")

inv_mod = (
    (inv["ESTADO"] == "ACTIVO") & (inv["SITUACION"] == "VIABLE/APROBADO")
).sum()

no_reg = (
    (inv["ESTADO"] == "ACTIVO")
    & (inv["SITUACION"] == "VIABLE/APROBADO")
    & (inv["MODALIDAD"] == "NO REGISTRADO")
).sum()

print(round((1 - no_reg / inv_mod) * 100, 1))

"""
Transform gastos
"""




"""
Save dataset
"""

inv.to_parquet(
    f"data/processing/inversiones.parquet", engine="pyarrow", compression="brotli"
)

