# src/modules/data_processing.py

import os
import sys
import pandas as pd
import numpy as np
import logging
import io
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

import src.utils as utils
from src.modules.sbs_data_fetcher import download_dataset

def _open_excel_in_memory_as_df(file_in_memory: io.BytesIO,
                                sheet_open_first: int = 2) -> pd.DataFrame:
    # --- Abre un archivo Excel en memoria como DataFrame. ---
    # Intenta abrir desde el número de hoja especificado hacia atrás
    sheets = list(range(sheet_open_first - 1, -1, -1))  
    for sheet in sheets:
        try:
            dataset_eeff = pd.read_excel(file_in_memory, sheet_name=sheet)
            return dataset_eeff  # Retorna inmediatamente cuando encuentra una hoja válida
        except Exception:
            continue  # Intenta la siguiente hoja
    # Si ninguna hoja funcionó
    raise FileNotFoundError("No se encontraron hojas válidas")

def _convert_excels_in_dict_to_df(dict_datasets_bytesio: dict, 
                                   name_files: str = '',
                                   sheet_open_first: int = 2,
                                   logger: logging.Logger | None = None) -> dict:
    # --- Convierte diccionario de archivos Excel en memoria a DataFrames. ---
    dict_datasets_df = {}
    errores_count = 0
    for key, value in dict_datasets_bytesio.items():
        if name_files in key:
            try:
                dict_datasets_df[key] = _open_excel_in_memory_as_df(value,sheet_open_first)
            except FileNotFoundError as e:
                errores_count += 1
                if logger:
                    logger.warning(f"No se pudo abrir '{key}': {e}")
            except Exception as e:
                errores_count += 1
                if logger:
                    logger.warning(f"Error inesperado al abrir '{key}': {e}")
    if logger and (errores_count > 0):
        logger.info(f"Total de archivos omitidos: {errores_count}")
    return dict_datasets_df

def _extract_metadata_from_filename(filename: str) -> tuple[int, int, str, str]:
    # --- Extrae metadatos del nombre del archivo ---
    months_map = [
        'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
    ]
    try:
        date = int(filename.split('_')[-1])
        year = int(filename.split('_')[-1][:4])
        month_num = int(filename.split('_')[-1][-2:])
        month_name = months_map[month_num - 1]
        kind = ' '.join(filename.split('_')[:2])
    except (IndexError, ValueError):
        raise ValueError(f"El nombre del archivo '{filename}' no sigue el formato esperado.")
    return date, year, month_name, kind

def _clean_str_or_liststr(terms: str | list[str]) -> list[str]:
    # --- Limpia y normaliza una cadena o lista de cadenas para búsqueda. ---
    if isinstance(terms, str):
        terms = [terms]
    # Normalizamos términos
    terms = [str(term).strip().lower() for term in terms]
    return terms

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # --- Limpia y normaliza un DataFrame para búsqueda. ---
    df_str = (
        df
        .dropna(axis=0, how='all')
        .dropna(axis=1, how='all')
        .reset_index(drop=True)
        )
    return df_str

def _build_mask_to_search(df: pd.DataFrame, terms: list|str, exact: bool = True) -> pd.DataFrame:
    # --- Construye una máscara booleana para buscar términos en un DataFrame. ---
    if exact:
        mask = df.isin(terms)
    else:
        mask = df.apply(
            lambda col: col.str.contains('|'.join(map(str, terms)), na=False)
            )
    return mask

def _localize_terms(df: pd.DataFrame, terms_clean: str | list[str], 
                    exact: bool = True) -> tuple[int, int] | tuple[None, None]:
    """Devuelve las coordenadas de la primera coincidencia."""
    # Normalizamos términos
    terms_clean = _clean_str_or_liststr(terms_clean)
    # Normalizamos DataFrame
    df_str_clean = _clean_df(df).astype(str)
    df_str_clean = df_str_clean.where(df_str_clean.notna(), "")
    df_str_clean = df_str_clean.map(lambda x: x.strip().lower())
    # Máscara booleana
    mask = _build_mask_to_search(df_str_clean, terms_clean, exact)
    # Todas las coincidencias (filtrar solo True)
    stacked = mask.stack()
    assert isinstance(stacked, pd.Series)  # ayuda al type checker
    matches = stacked[stacked]
    if matches.empty:
        return (None, None)
    row, colname = matches.index[0]
    rowidx = df.index.get_indexer([row])[0]
    colidx = df.columns.get_indexer([colname])[0]
    return rowidx, colidx

def _build_eeff_dataframe(dataset_eeff: pd.DataFrame, pos_if: tuple,
                          pos_isf: tuple, pos_rn: tuple) -> pd.DataFrame:
    # Si cualquiera es (None, None) → devolver DataFrame vacío
    if pos_if == (None, None) or pos_isf == (None, None) or pos_rn == (None, None):
        return pd.DataFrame()
    # --- Construye el DataFrame final de EEFF a partir de las posiciones encontradas ---
    idx_row_if, idx_col_if = pos_if
    idx_row_isf, _ = pos_isf
    idx_row_rn, _ = pos_rn
    # Construcción del DataFrame
    df_clean = _clean_df(dataset_eeff)
    heads = (
        df_clean
        .iloc[(idx_row_if - 2):(idx_row_if), idx_col_if:]
        .T
        .ffill()
        )
    heads.columns = ['ENTIDAD', 'MONEDA']
    heads = pd.MultiIndex.from_frame(heads)
    data_rows = {
        'INGRESOS FINANCIEROS': df_clean.iloc[idx_row_if, idx_col_if:].values,
        'INGRESOS SERVICIOS FINANCIEROS': df_clean.iloc[idx_row_isf, idx_col_if:].values,
        'RESULTADO NETO': df_clean.iloc[idx_row_rn, idx_col_if:].values
    }
    temp_df = pd.DataFrame(data_rows, index=heads)
    return temp_df

def _transform_eeff_dataframe(key: str, dataset_eeff: pd.DataFrame) -> pd.DataFrame | None:
    # --- Transforma un DataFrame de EEFF en el formato final esperado ---
    if dataset_eeff is None or dataset_eeff.empty:
        return None
    date, year, month_name, kind = _extract_metadata_from_filename(key)
    df_processed = (
        dataset_eeff
        .apply(pd.to_numeric, errors="coerce").dropna(axis=0, how='all').reset_index()
        .reset_index()
        .assign(
            ENTIDAD =lambda df: df["ENTIDAD"].astype(str).str.strip().replace('', None).ffill()
        )
        .pipe(lambda df: df[~(
                    df['ENTIDAD'].str.lower().str.startswith('total') 
                    | df['ENTIDAD'].str.lower().str.contains('sucursal'))])
        .assign(
            ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.replace(r"[\d*/()]", "", regex=True),
            DATE=date, PERIODO=year, MES=month_name, TIPO=kind,
            INGRESO=lambda df: df["INGRESOS FINANCIEROS"] + df["INGRESOS SERVICIOS FINANCIEROS"]
        )
        [['DATE', 'PERIODO', 'MES', 'TIPO', 'ENTIDAD', 'MONEDA', 'INGRESOS FINANCIEROS',
          'INGRESOS SERVICIOS FINANCIEROS', 'INGRESO', 'RESULTADO NETO']]
    )
    return df_processed

def process_dataset_eeff(files_in_memory: dict, if_terms: str | list[str], 
                          isf_terms: str | list[str], rn_terms: str | list[str], 
                          logger: logging.Logger) -> pd.DataFrame:
    # --- Abrir datasets de EEFF ---
    logger.info("--- Iniciando sección: Procesamiento de EEFF ---")
    datasets_eeff = _convert_excels_in_dict_to_df(
        files_in_memory, name_files='EEFF', logger=logger
        )
    if not datasets_eeff:
        logger.warning("No se encontraron archivos de EEFF para procesar.")
        return pd.DataFrame()
    # --- Procesa todos los DataFrames de EEFF y los concatena en uno solo ---
    list_of_eeff_df = []
    processed_count = 0
    for key, dataset_eeff in datasets_eeff.items():
        try:
            pos_if = _localize_terms(dataset_eeff, if_terms)
            pos_isf = _localize_terms(dataset_eeff, isf_terms)
            pos_rn = _localize_terms(dataset_eeff, rn_terms)
            df = _build_eeff_dataframe(dataset_eeff, pos_if, pos_isf, pos_rn)
            df_processed = _transform_eeff_dataframe(key, df)
            if df_processed is not None:
                list_of_eeff_df.append(df_processed)
                processed_count += 1
        except Exception as e:
            logger.error(f"No se pudo procesar EEFF de '{key}': {e}", exc_info=False)
    if not list_of_eeff_df:
        logger.warning("No se pudo procesar ningún archivo de EEFF.")
        return pd.DataFrame()
    df_eeff = pd.concat(list_of_eeff_df, axis=0, ignore_index=True)
    logger.info(
        f"Procesamiento de EEFF completado. Se procesaron {processed_count}/{len(datasets_eeff)} archivos. ✅")
    return df_eeff

def _build_tc_dataframe(key: str, dataset_tc: pd.DataFrame, pos_tc: tuple) -> pd.DataFrame:
    # Si cualquiera es (None, None) → devolver DataFrame vacío
    if pos_tc == (None, None):
        return pd.DataFrame()
    # --- Construye el DataFrame final de EEFF a partir de las posiciones encontradas ---
    list_of_tc_rows = []
    idx_row_tc, idx_col_tc = pos_tc
    date, year, month_name, _ = _extract_metadata_from_filename(key)
    # Construcción del DataFrame
    df_clean = _clean_df(dataset_tc)
    tc_value = df_clean.iloc[idx_row_tc, idx_col_tc]  # Valor en columna adyacente
    tc_row = pd.DataFrame({
        'DATE': date, 'PERIODO': year, 'MES': month_name, 'TC': tc_value
    }, index=[0])
    list_of_tc_rows.append(tc_row)
    if not list_of_tc_rows:
        temp_df = pd.DataFrame()
    else:
        temp_df = pd.concat(list_of_tc_rows, axis=0, ignore_index=True)
    return temp_df

def process_dataset_tc(files_in_memory: dict, tc_terms: str | list[str],
                       logger: logging.Logger) -> pd.DataFrame:
    # --- Abrir datasets de EEFF ---
    logger.info("--- Iniciando sección: Procesamiento de TC ---")
    datasets_tc = _convert_excels_in_dict_to_df(
        files_in_memory, name_files='Banca_Multiple_EEFF',
        sheet_open_first=1, logger=logger
        )
    if not datasets_tc:
        logger.warning("No se encontraron archivos para procesar.")
        return pd.DataFrame()
    # --- Procesa todos los DataFrames de EEFF y los concatena en uno solo ---
    list_of_tc_df = []
    processed_count = 0
    for key, dataset_tc in datasets_tc.items():
        try:
            pos_tc = _localize_terms(dataset_tc, tc_terms, exact=False)
            df_processed = _build_tc_dataframe(key, dataset_tc, pos_tc)
            if df_processed is not None:
                list_of_tc_df.append(df_processed)
                processed_count += 1
        except Exception as e:
            logger.error(f"No se pudo procesar EEFF de '{key}': {e}", exc_info=False)
    if not list_of_tc_df:
        logger.warning("No se pudo procesar ningún archivo.")
        return pd.DataFrame()
    df_eeff = pd.concat(list_of_tc_df, axis=0, ignore_index=True)
    logger.info(
        f"Procesamiento completado. Se procesaron {processed_count}/{len(datasets_tc)} archivos. ✅")
    return df_eeff

if __name__ == "__main__":
    FINANCIAL_INCOME_TERMS = "INGRESOS FINANCIEROS"
    SERVICE_INCOME_TERMS = "INGRESOS POR SERVICIOS FINANCIEROS"
    NET_RESULT_TERMS = [
        "RESULTADO NETO DEL EJERCICIO",
        "UTILIDAD ( PÉRDIDA ) NETA",
        "UTILIDAD (PÉRDIDA) NETA"
    ]
    TC_TERMS = "TIPO DE CAMBIO"
    bucket_name = 'opendataanalyzer_datas'
    path_file_eeff = 'SBS_EEFF_PROCESSED.csv'
    path_file_tc = 'SBS_TC_PROCESSED.csv'
    gcs_manager = utils.GCSManager()
    sbs_eeff_processed = gcs_manager.download_csv_as_df(bucket_name, path_file_eeff)
    sbs_tc_processed = gcs_manager.download_csv_as_df(bucket_name, path_file_tc)
    if sbs_eeff_processed is not None:
        files_in_memory = download_dataset(sbs_eeff_processed)
        if bool(files_in_memory):
            sbs_eeff_actualyzed = process_dataset_eeff(
                files_in_memory, FINANCIAL_INCOME_TERMS,
                SERVICE_INCOME_TERMS, NET_RESULT_TERMS,
                utils.get_logger('SBS')
                )
            sbs_eeff_processed = pd.concat(
                [sbs_eeff_processed, sbs_eeff_actualyzed], 
                axis=0, ignore_index=True
                )
            sbs_tc_processed = process_dataset_tc(
                files_in_memory, TC_TERMS,
                utils.get_logger('SBS')
                )
        else:
            pass
    else:
        pass
    

