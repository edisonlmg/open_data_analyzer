# src/modules/plpp_data_processing.py

import pandas as pd
import numpy as np


def _departamento_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma DEPARTAMENTO_NOMBRE según NIVEL_GOBIERNO.
    - != 1: usa DEPARTAMENTO_NOMBRE o PLIEGO_NOMBRE limpio
    - == 1: mantiene valor o asigna 'NO APLICA'
    """
    if "NIVEL_GOBIERNO" not in df.columns:
        return df
    
    # Crear una copia de la columna de pliego y limpiarla
    pliego_limpio = df["PLIEGO_NOMBRE"].replace(
        {r"(?i)^GOBIERNO REGIONAL (DEL DEPARTAMENTO DE|DE LA)\s*": ""},
        regex=True
    )
    
    # Regional/Local: usar DEPARTAMENTO_NOMBRE si existe, si no, usar pliego limpio
    mask_regional = df["NIVEL_GOBIERNO"] != 1
    df.loc[mask_regional, "DEPARTAMENTO_EJECUTORA_NOMBRE"] = (
        df.loc[mask_regional, "DEPARTAMENTO_EJECUTORA_NOMBRE"]
        .combine_first(pliego_limpio[mask_regional]).fillna("NO APLICA")
    )
    
    # Gobierno Central: completar vacíos
    mask_central = df["NIVEL_GOBIERNO"] == 1
    df.loc[mask_central, "DEPARTAMENTO_EJECUTORA_NOMBRE"] = (
        df.loc[mask_central, "DEPARTAMENTO_EJECUTORA_NOMBRE"].fillna("NO APLICA")
    )
    
    return df


def _clean_dataframe(df: pd.DataFrame, rename_map: dict, drop_cols: list[str]) -> pd.DataFrame:
    """
    Limpia y estandariza un DataFrame:
    - Renombra columnas
    - Elimina columnas innecesarias
    - Limpia cadenas y reemplaza vacíos por NaN
    - Rellena valores faltantes en campos clave
    """
    df_clean = (
        df
        .rename(columns=rename_map)
        .drop(columns=drop_cols, errors="ignore")
        .replace(r"^\s*$", np.nan, regex=True)
    )

    # Aplicar la transformación condicional de DEPARTAMENTO_NOMBRE
    df_clean = _departamento_transform(df_clean)

    # Completar campos de sector
    if "SECTOR" in df_clean.columns:
        df_clean["SECTOR"] = df_clean["SECTOR"].fillna(98).astype("int64")
    if "SECTOR_NOMBRE" in df_clean.columns:
        df_clean["SECTOR_NOMBRE"] = df_clean["SECTOR_NOMBRE"].fillna("GOBIERNOS LOCALES")

    return df_clean


def _process_dict_of_dataframes(dic_df: dict[str, pd.DataFrame], rename_map: dict, drop_cols: list[str]) -> dict[str, pd.DataFrame]:
    """Aplica la limpieza a cada DataFrame del diccionario."""
    return {
        key: _clean_dataframe(df, rename_map, drop_cols)
        for key, df in dic_df.items()
        if isinstance(df, pd.DataFrame)
    }


def _merge_dataframes(dic_df: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Concatena todos los DataFrames del diccionario en un solo DataFrame."""
    if not dic_df:
        return pd.DataFrame()
    return pd.concat(dic_df.values(), ignore_index=True)


def process_dataset_plpp(dic_df_plp: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Pipeline completo de procesamiento de datasets PLP:
    1. Renombra y limpia columnas.
    2. Estandariza valores faltantes.
    3. Concatena todos los DataFrames resultantes.
    """
    rename_map = {
        "DEPARTAMENTO_EJECUTORA": "DEPARTAMENTO",
        "DEPARTAMENTO_EJECUTORA_NOMBRE": "DEPARTAMENTO_NOMBRE",
        "PROVINCIA_EJECUTORA": "PROVINCIA",
        "PROVINCIA_EJECUTORA_NOMBRE": "PROVINCIA_NOMBRE",
        "TIPO_ACTIVIDAD_ACCOBRA_NOMBRE": "TIPO_ACTIVIDAD_ACCION_OBRA_NOMBRE",
        "TIPO_ACTIVIDAD_ACCION_OBRA_NOM": "TIPO_ACTIVIDAD_ACCION_OBRA_NOMBRE",
    }

    drop_cols = [
        "DEPARTAMENTO_META",
        "DEPARTAMENTO_META_NOMBRE",
        "INDICADOR_GASTO",
    ]

    cleaned_dict = _process_dict_of_dataframes(dic_df_plp, rename_map, drop_cols)
    merged_df = _merge_dataframes(cleaned_dict)

    return merged_df
