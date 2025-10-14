# src/modules/plpp_data_processing.py

import pandas as pd
import numpy as np

def _clean_dataframe(df: pd.DataFrame, rename_map: dict, drop_cols: list[str]) -> pd.DataFrame:
    """
    Limpia y estandariza un DataFrame:
    - Renombra columnas
    - Elimina columnas innecesarias
    - Limpia cadenas y reemplaza vacíos por NaN
    - Rellena valores faltantes en campos clave
    """
    df = (
        df
        .rename(columns=rename_map)
        .drop(columns=drop_cols, errors="ignore")
        .assign(
            **{
                col: df[col].astype(str).str.strip()
                for col in df.select_dtypes(include="object").columns
            }
        )
        .replace('', np.nan)
        .assign(
            SECTOR=lambda d: d['SECTOR'].fillna(98).astype('int64'),
            SECTOR_NOMBRE=lambda d: d['SECTOR_NOMBRE'].fillna('GOBIERNOS LOCALES'),
            DEPARTAMENTO=lambda d: d['DEPARTAMENTO'].fillna(0).astype('int64'),
            DEPARTAMENTO_NOMBRE=lambda d: d['DEPARTAMENTO_NOMBRE'].fillna('NO APLICA'),
            PROVINCIA=lambda d: d['PROVINCIA'].fillna(0).astype('int64'),
            PROVINCIA_NOMBRE=lambda d: (
                d['PROVINCIA_NOMBRE']
                .combine_first(
                    d['PLIEGO_NOMBRE']
                    .replace(
                        {
                            r'(?i)^GOBIERNO REGIONAL DEL DEPARTAMENTO DE\s*': '',
                            r'(?i)^GOBIERNO REGIONAL DE LA\s*': ''
                        },
                        regex=True
                    )
                    .str.strip()
                )
                .fillna('NO APLICA')
            )
        )
    )
    return df

def _process_dict_of_dataframes(dic_df: dict[str, pd.DataFrame], rename_map: dict, drop_cols: list[str]) -> dict[str, pd.DataFrame]:
    """
    Aplica la limpieza a cada DataFrame del diccionario.
    """
    return {key: _clean_dataframe(df, rename_map, drop_cols) for key, df in dic_df.items()}


def _merge_dataframes(dic_df: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Concatena todos los DataFrames del diccionario en un solo DataFrame.
    """
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

    drop_cols = ["DEPARTAMENTO_META", "DEPARTAMENTO_META_NOMBRE", "INDICADOR_GASTO"]

    cleaned_dict = _process_dict_of_dataframes(dic_df_plp, rename_map, drop_cols)
    merged_df = _merge_dataframes(cleaned_dict)

    return merged_df

