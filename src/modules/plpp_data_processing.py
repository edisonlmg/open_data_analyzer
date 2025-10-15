# src/modules/plpp_data_processing.py

import pandas as pd
import numpy as np


def _departamento_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma DEPARTAMENTO_NOMBRE solo cuando NIVEL_GOBIERNO != 1.
    Para NIVEL_GOBIERNO == 1, asigna 'NO APLICA' si el campo está vacío.
    """
    if "NIVEL_GOBIERNO" not in df.columns:
        return df

    # Máscara: aplica transformación solo cuando NIVEL_GOBIERNO != 1
    mask = df["NIVEL_GOBIERNO"] != 1

    if mask.any():
        sub_df = df.loc[mask].copy()

        provincia = sub_df.get("PROVINCIA_NOMBRE", pd.Series(index=sub_df.index))
        pliego = sub_df.get("PLIEGO_NOMBRE", pd.Series(index=sub_df.index))

        sub_df["DEPARTAMENTO_NOMBRE"] = (
            provincia.combine_first(
                pliego.replace(
                    {
                        r"(?i)^GOBIERNO REGIONAL DEL DEPARTAMENTO DE\s*": "",
                        r"(?i)^GOBIERNO REGIONAL DE LA\s*": ""
                    },
                    regex=True
                )
            )
            .fillna("NO APLICA")
        )

        df.loc[mask, "DEPARTAMENTO_NOMBRE"] = sub_df["DEPARTAMENTO_NOMBRE"]

    # Para NIVEL_GOBIERNO == 1: no modificar, solo completar si falta
    mask_gob_central = df["NIVEL_GOBIERNO"] == 1
    if mask_gob_central.any():
        df.loc[mask_gob_central, "DEPARTAMENTO_NOMBRE"] = (
            df.loc[mask_gob_central, "DEPARTAMENTO_NOMBRE"]
            .where(df.loc[mask_gob_central, "DEPARTAMENTO_NOMBRE"].notna(), "NO APLICA")
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
    df = (
        df.rename(columns=rename_map)
          .drop(columns=drop_cols, errors="ignore")
          .replace(r"^\s*$", np.nan, regex=True)
    )

    # Limpiar texto solo en columnas de tipo object
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    # Completar campos de sector
    if "SECTOR" in df.columns:
        df["SECTOR"] = df["SECTOR"].fillna(98).astype("int64")
    if "SECTOR_NOMBRE" in df.columns:
        df["SECTOR_NOMBRE"] = df["SECTOR_NOMBRE"].fillna("GOBIERNOS LOCALES")

    # Aplicar la transformación condicional de DEPARTAMENTO_NOMBRE
    df = _departamento_transform(df)

    return df


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


