# src/modules/data_processing.py

import pandas as pd
import numpy as np
import logging

def _align_columns_names(inv_datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Alinea los nombres de las columnas al formato estandar."""
    df_aligned = {}
    for key, df in inv_datasets.items():
        df_aligned[key] = (
            df.rename(
                {
                    'NOMBRE_OPMI': 'NOM_OPMI',
                    'OPI': 'NOM_OPMI',
                    'NOMBRE_UF': 'NOM_UF',
                    'NOMBRE_UEI': 'NOM_UEI',
                    'NOMBRE_UEP': 'NOM_UEP',
                    'DEVEN_ACUMULADO': 'DEVENGADO_ACUMULADO',
                    'ANIO_PROC':'ANIO_PROCESO',
                    'COD_SNIP': 'CODIGO_SNIP',
                    'SUBPROGRAM':'SUBPROGRAMA',
                    'INICIO_EJEC_FISICA':'FEC_INI_EJEC_FISICA',
                    'CULMINACION_EJEC_FISICA':'FEC_FIN_EJEC_FISICA',
                    'NUM_HABITANTES_BENEF':'BENEFICIARIO'
                },
                axis=1
            )
        )
    return df_aligned

def _devengado_acumulado_activas(inv_datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Calcula la columna DEVENGADO_ACUMULADO para los datasets de inversiones activas."""
    inv_datasets_dev = inv_datasets.copy()
    inv_datasets_dev['DETALLE_INVERSIONES'] = (
        inv_datasets_dev['DETALLE_INVERSIONES']
        .assign(
            DEVENGADO_ACUMULADO = (
                inv_datasets_dev['DETALLE_INVERSIONES']['DEV_ANIO_ACTUAL']
                +inv_datasets_dev['DETALLE_INVERSIONES']['DEVEN_ACUMUL_ANIO_ANT']
                )
        )
    )
    return inv_datasets_dev

def _build_inv_df(inv_datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Construye el DataFrame final de inversiones a partir de los datasets descargados."""
    df_inv = _align_columns_names(inv_datasets)
    df_inv = _devengado_acumulado_activas(df_inv)
    df_inv = (
        pd.concat(
            df_inv.values(),
            axis=0,
            ignore_index=True
            )
        .drop_duplicates(subset='CODIGO_SNIP')
        .reset_index(drop=True)
    )
    return df_inv

def _converter_columns_dates(df_inv: pd.DataFrame) -> pd.DataFrame:
    """Convierte las columnas de fechas al tipo datetime."""
    dates = [
        'FECHA_REGISTRO','FECHA_VIABILIDAD','FEC_REG_F9','FECHA_ULT_ACT_F12B',
        'ULT_FEC_DECLA_ESTIM','FEC_INI_EJECUCION','FEC_FIN_EJECUCION',
        'FEC_INI_EJEC_FISICA','FEC_FIN_EJEC_FISICA','FEC_CIERRE','FEC_INI_OPER',
    ]
    df_inv_dates = df_inv.pipe(lambda df: (
        df.assign(
            **{
                date: pd.to_datetime(
                    (
                        df[date]
                        .astype(str)
                        .str.extract(r'(\d{4}-\d{2}-\d{2})',expand=False)
                    ),
                    format='%Y-%m-%d',
                    errors='coerce'
                    )
                for date in dates
                }
            )
        ))
    return df_inv_dates

def _converter_columns_numeric(df_inv: pd.DataFrame) -> pd.DataFrame:
    """Asegura que las columnas numéricas no tengan valores no numéricos."""
    exact_names = [
        'COSTO_ACTUALIZADO', 'CTRL_CONCURR', 'EJECUCION',
        'NUM_HABITANTES_BENEF', 'CONTRIB_CIERRE_BRECHA',
        'TOTAL_LIQUIDACION','BENEFICIARIO'
    ]
    prefixes = ['DEV', 'PMI', 'MONTO', 'SALDO', 'AVANCE']
    suffixes = ['ANIO_ACTUAL']
    matched_cols = set()
    # 1️⃣ Columnas exactas
    matched_cols.update([col for col in exact_names if col in df_inv.columns])
    # 2️⃣ Columnas que comienzan con los prefijos
    for prefix in prefixes:
        matched_cols.update([col for col in df_inv.columns if col.startswith(prefix)])
    # 3️⃣ Columnas que terminan con los sufijos
    for suffix in suffixes:
        matched_cols.update([col for col in df_inv.columns if col.endswith(suffix)])
    # 4️⃣ Convertir numéricamente
    df_inv_numeric = df_inv.assign(
        **{
            col: pd.to_numeric(df_inv[col], errors='coerce').fillna(0)
            for col in matched_cols
        }
    )
    return df_inv_numeric

def _calculate_columns(df_inv: pd.DataFrame) -> pd.DataFrame:
    """Calcula las columnas derivadas SALDO, EJECUCION, ANIO_REGISTRO, ANIO_VIABILIDAD y ANIO_CIERRE."""
    df_inv_calc = df_inv.assign(
        SALDO = lambda df: (
            np.where(
                df['DEVENGADO_ACUMULADO'] > df['COSTO_ACTUALIZADO'], 0,
                round(df['COSTO_ACTUALIZADO'] - df['DEVENGADO_ACUMULADO'], 0
                      )
                )
            ),
        EJECUCION = lambda df: (
            np.where(
                df['COSTO_ACTUALIZADO'] == 0, 0,
                round(df['DEVENGADO_ACUMULADO'] / df['COSTO_ACTUALIZADO'] * 100, 1
                      )
                )
            ),
        COSTO_ACTUALIZADO = lambda df: df['COSTO_ACTUALIZADO'] - df['CTRL_CONCURR'],
        ANIO_REGISTRO = lambda df: df['FECHA_REGISTRO'].dt.year,
        ANIO_VIABILIDAD = lambda df: df['FECHA_VIABILIDAD'].dt.year,
        ANIO_CIERRE = lambda df: df['FEC_CIERRE'].dt.year
        )
    return df_inv_calc

def _standarize_columns(df_inv: pd.DataFrame) -> pd.DataFrame:
    """Estandariza los nombres de las columnas y sus valores."""
    df_inv_standar = (
        df_inv
        .replace(
            {
                'ESTADO': {
                    'DESACTIVADO ': 'DESACTIVADO PERMANENTE',
                    'DESACTIVADO   PERMANENTE': 'DESACTIVADO PERMANENTE',
                },
                'SITUACION': {
                    'VIABLE': 'VIABLE/APROBADO',
                    'APROBADO': 'VIABLE/APROBADO',
                    'NO VIABLE': 'NO VIABLE/APROBADO',
                    'NO APROBADO': 'NO VIABLE/APROBADO',
                },
                'DEPARTAMENTO': {'-MUL.DEP-': 'MULTIDEPARTAMENTO'},
            }
        )
        .fillna({'DEPARTAMENTO': 'NO REGISTRA'})
        .rename({'FEC_CIERRE':'FECHA_CIERRE'}, axis=1)
    )
    return df_inv_standar

def process_dataset_inv(inv_datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Procesa los datasets de inversiones y devuelve un DataFrame estandarizado."""
    logging.info("🔄 Procesando datasets de inversiones...")
    df_inv = _build_inv_df(inv_datasets)
    df_inv = (
        df_inv
        .pipe(_converter_columns_dates)
        .pipe(_converter_columns_numeric)
        .pipe(_calculate_columns)
        .pipe(_standarize_columns)
    )
    logging.info("✅ Datasets de inversiones procesados correctamente.")
    return df_inv

