# src/modules/data_processing.py

import os
import sys
import pandas as pd
import logging
import io
from pathlib import Path

if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

import src.utils as utils
from src.modules.sbs_data_fetcher import download_dataset
from src.modules.gcs_manager import GCSManager

bucket_name = 'opendataanalyzer_datas'
path_file = 'SBS_EEFF_ANALYZED.csv'
gcs_manager = GCSManager()
sbs_eeff_analyzed = gcs_manager.download_csv_as_df(bucket_name, path_file)
was_downloaded, files_in_memory = download_dataset(sbs_eeff_analyzed)

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
                                   logger: logging.Logger | None = None) -> dict:
    # --- Convierte diccionario de archivos Excel en memoria a DataFrames. ---
    dict_datasets_df = {}
    errores_count = 0
    for key, value in dict_datasets_bytesio.items():
        if name_files in key:
            try:
                dict_datasets_df[key] = _open_excel_in_memory_as_df(value)
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


# --- Variables de Configuración ---
MONTHS_MAP = [
    'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
]
FINANCIAL_INCOME_TERMS = "INGRESOS FINANCIEROS"
SERVICE_INCOME_TERMS = "INGRESOS POR SERVICIOS FINANCIEROS"
NET_RESULT_TERMS = [
    "RESULTADO NETO DEL EJERCICIO",
    "UTILIDAD ( PÉRDIDA ) NETA",
    "UTILIDAD (PÉRDIDA) NETA"
]


def _extract_metadata_from_filename(filename: str) -> tuple[int, int, int, str, str]:
    # --- Extrae metadatos del nombre del archivo ---
    try:
        date = int(filename.split('_')[-1])
        year = int(filename.split('_')[-1][:4])
        month_num = int(filename.split('_')[-1][-2:])
        month_name = MONTHS_MAP[month_num - 1]
        kind = ' '.join(key.split('_')[:2])
    except (IndexError, ValueError):
        raise ValueError(f"El nombre del archivo '{filename}' no sigue el formato esperado.")
    return date, year, month_num, month_name, kind

def _clean_str_or_liststr(terms: str | list[str]) -> list[str]:
    # --- Limpia y normaliza una cadena o lista de cadenas para búsqueda. ---
    if isinstance(terms, str):
        terms = [terms]
    # Normalizamos términos
    terms = [str(term).strip().lower() for term in terms]
    return terms

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # --- Limpia y normaliza un DataFrame para búsqueda. ---
    df_str = df.astype(str).where(df.notna(), "")
    df_str = (
        df_str
        .dropna(axis=0, how='all')
        .dropna(axis=1, how='all')
        .reset_index(drop=True)
        )
    df_str = df_str.map(lambda x: x.strip().lower())
    return df_str

def _build_mask_to_search(df: pd.DataFrame, terms: list[str], exact: bool) -> pd.DataFrame:
    # --- Construye una máscara booleana para buscar términos en un DataFrame. ---
    if exact:
        mask = df.isin(terms)
    else:
        mask = df.apply(
            lambda col: col.str.contains('|'.join(map(str, terms)), na=False)
            )
    return mask

def _localize_terms(df: pd.DataFrame, terms_clean: list[str], 
                    exact: bool = False) -> tuple[int, int] | tuple[None, None]:
    """Devuelve las coordenadas de la primera coincidencia."""
    # Normalizamos términos
    terms_clean = _clean_str_or_liststr(terms_clean)
    # Normalizamos DataFrame
    df_str_clean = _clean_df(df)
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

def _missing_terms(positions_terms: list[tuple[int, int]], terms: list[str]) -> list[str]:
    # --- Identifica términos que no se encontraron en el DataFrame ---
    if not all(positions_terms):
        missing = []
        if not positions_terms[0]: missing.append(terms[0])
        if not pos_isf: missing.append("Servicios Financieros")
        if not pos_rn: missing.append("Resultado Neto")
        raise ValueError(f"No se encontraron las filas clave: {', '.join(missing)}.")

def _build_eeff_dataframe(dataset_eeff: pd.DataFrame, pos_if: tuple[int, int],
                          pos_isf: tuple[int, int], pos_rn: tuple[int, int]) -> pd.DataFrame:
    # --- Construye el DataFrame final de EEFF a partir de las posiciones encontradas ---
    idx_row_if, idx_col_if = pos_if
    idx_row_isf, _ = pos_isf
    idx_row_rn, _ = pos_rn

    # Construcción del DataFrame
    heads = (
        dataset_eeff
        .iloc[(idx_row_if - 2):(idx_row_if), idx_col_if:]
        .T
        .ffill()
        )
    heads.columns = ['ENTIDAD', 'MONEDA']
    heads = pd.MultiIndex.from_frame(heads)
    data_rows = {
        'INGRESOS FINANCIEROS': dataset_eeff.iloc[idx_row_if, idx_col_if:].values,
        'INGRESOS SERVICIOS FINANCIEROS': dataset_eeff.iloc[idx_row_isf, idx_col_if:].values,
        'RESULTADO NETO': dataset_eeff.iloc[idx_row_rn, idx_col_if:].values
    }
    temp_df = pd.DataFrame(data_rows, index=heads).T
    return temp_df


def _process_single_eeff_file(dict_datasets: dict) -> dict:
    # --- Construye 
   
   for key, value in dict_datasets.items():
        if 'EEFF' in key:
            try:
                dataset_eeff = _open_dataset(value)
                if dataset_eeff is None:
                    raise ValueError("No se pudo abrir el dataset de EEFF.")
            except Exception as e:
                raise ValueError(f"No se pudo abrir el dataset de EEFF de '{key}': {e}", exc_info=False)
                continue

            # try:
            #     date, year, month_num, month_name, kind = _extract_metadata_from_filename(key)
            # except ValueError as e:
            #     logger.error(f"Error al extraer metadatos del nombre del archivo '{key}': {e}", exc_info=False)
            #     continue

            # try:
            #     pos_if = _localize_terms(dataset_eeff, FINANCIAL_INCOME_TERMS, exact=True)
            #     pos_isf = _localize_terms(dataset_eeff, SERVICE_INCOME_TERMS, exact=True)
            #     pos_rn = _localize_terms(dataset_eeff, NET_RESULT_TERMS, exact=False)

            #     _missing_terms([pos_if, pos_isf, pos_rn], 
            #                    [FINANCIAL_INCOME_TERMS, SERVICE_INCOME_TERMS, "Resultado Neto"])
            # except ValueError as e:
            #     logger.error(f"Error al localizar términos en '{key}': {e}", exc_info=False)
            #     continue

            # try:
            #     processed_df = _build_eeff_dataframe(dataset_eeff, pos_if, pos_isf, pos_rn)
            # except Exception as e:
            #     logger.error(f"Error al construir DataFrame de EEFF para '{key}': {e}", exc_info=False)
            #     continue

            # # Enriquecimiento del DataFrame
            # processed_df = (
            #     processed_df
            #     .apply(pd.to_numeric, errors="coerce").dropna(axis=0, how='all').reset_index()
            #     .pipe(lambda df: df[~(
            #                 df['ENTIDAD'].str.lower().str.startswith('total') | df['ENTIDAD'].str.lower().str.contains(
            #             'sucursal'))])
            #     .assign(
            #         ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.replace(r"[\d*/()]", "", regex=True).str.strip(),
            #         DATE=date, PERIODO=year, MES=month_name, TIPO=kind,
            #         INGRESO=lambda df: df["INGRESOS FIN
      
    # # Localización de filas clave
    # pos_if = _localize_terms(dataset_eeff, FINANCIAL_INCOME_TERMS, exact=True)
    # pos_isf = _localize_terms(dataset_eeff, SERVICE_INCOME_TERMS, exact=True)
    # pos_rn = _localize_terms(dataset_eeff, NET_RESULT_TERMS, exact=True)

    # idx_row_if, idx_col_if = pos_if
    # idx_row_isf, _ = pos_isf
    # idx_row_rn, _ = pos_rn

    
    # # Limpieza y enriquecimiento del DataFrame
    # processed_df = (
    #     temp_df
    #     .apply(pd.to_numeric, errors="coerce").dropna(axis=0, how='all').reset_index()
    #     .pipe(lambda df: df[~(
    #                 df['ENTIDAD'].str.lower().str.startswith('total') | df['ENTIDAD'].str.lower().str.contains(
    #             'sucursal'))])
    #     .assign(
    #         ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.replace(r"[\d*/()]", "", regex=True).str.strip(),
    #         DATE=date, PERIODO=year, MES=month_name, TIPO=kind,
    #         INGRESO=lambda df: df["INGRESOS FINANCIEROS"] + df["INGRESOS SERVICIOS FINANCIEROS"]
    #     )
    #     [['DATE', 'PERIODO', 'MES', 'TIPO', 'ENTIDAD', 'MONEDA', 'INGRESOS FINANCIEROS',
    #       'INGRESOS SERVICIOS FINANCIEROS', 'INGRESO', 'RESULTADO NETO']]
    # )
    
    return dict_datasets

def process_dataset(new_files: list[str]):
    """
    Función principal que orquesta el procesamiento de los datasets descargados.
    """
    # --- 1. Setup Inicial ---
    logger = utils.get_logger()
    logger.info(">>> Iniciando el proceso de transformación de datos...")
    root_dir = utils.get_project_root()
    output_raw_dir = os.path.join(root_dir, 'data', 'raw')
    output_processed_dir = os.path.join(root_dir, 'data', 'processed')
    os.makedirs(output_processed_dir, exist_ok=True)
    logger.info(f"Directorio de datos procesados verificado: '{output_processed_dir}'")

    # --- 2. Transformar Datasets de Tipo de Cambio (TC) ---
    logger.info("--- Iniciando sección: Transformación de Tipo de Cambio (TC) ---")
    list_of_tc_rows = []
    list_banca_eff = [f for f in new_files if 'Banca' in f and 'EEFF' in f]
    logger.info(f"Se encontraron {len(list_banca_eff)} archivos de 'Banca Múltiple' para extraer TC.")

    for key in tqdm(list_banca_eff, desc="Buscando TC por periodo"):
        try:
            date = int(key.split('_')[-1])
            year = key.split('_')[-1][:4]
            month_num = int(key.split('_')[-1][-2:])
            month_name = MONTHS_MAP[month_num - 1]

            file_path = os.path.join(output_raw_dir, f'{key}.xls')
            dataset_bg = pd.read_excel(file_path, sheet_name=0)

            pos = _localize(dataset_bg, "Tipo de Cambio")
            if pos:
                idx_row, idx_col = pos
                tc_value = dataset_bg.iloc[idx_row, idx_col + 1]  # Valor en columna adyacente
                tc_row = pd.DataFrame({
                    'DATE': date, 'PERIODO': year, 'MES': month_name, 'TC': tc_value
                }, index=[0])
                list_of_tc_rows.append(tc_row)
                logger.debug(f"TC extraído exitosamente de '{key}.xls'. Valor: {tc_value}")
            else:
                raise ValueError("No se encontró la celda 'Tipo de Cambio'")
        except Exception as e:
            logger.error(f"No se pudo extraer TC de '{key}': {e}", exc_info=False)

    if not list_of_tc_rows:
        logger.warning("No se pudo extraer información de TC de ningún archivo.")
        df_tc = pd.DataFrame()
    else:
        df_tc = pd.concat(list_of_tc_rows, axis=0, ignore_index=True)
        logger.info(f"Búsqueda de TC completada. Se extrajeron {len(df_tc)} registros. ✅")

    # --- 3. Transformar Datasets de Estados Financieros (EEFF) ---
    logger.info("--- Iniciando sección: Transformación de Estados Financieros (EEFF) ---")
    list_of_eeff_df = []
    list_datasets_eeff = [f for f in new_files if 'EEFF' in f]
    processed_count = 0
    logger.info(f"Se procesarán {len(list_datasets_eeff)} archivos de EEFF.")

    for key in tqdm(list_datasets_eeff, desc="Procesando EEFF"):
        try:
            processed_df = _process_single_eeff_file(key, output_raw_dir, logger)
            if processed_df is not None:
                list_of_eeff_df.append(processed_df)
                processed_count += 1
        except Exception as e:
            logger.error(f"No se pudo procesar EEFF de '{key}': {e}", exc_info=False)

    if not list_of_eeff_df:
        logger.warning("No se pudo procesar ningún archivo de EEFF.")
        df_eeff = pd.DataFrame()
    else:
        df_eeff = pd.concat(list_of_eeff_df, axis=0, ignore_index=True)
        logger.info(
            f"Procesamiento de EEFF completado. Se procesaron {processed_count}/{len(list_datasets_eeff)} archivos. ✅")

    # --- 4. Crear Datasets para Dashboards ---
    logger.info("--- Iniciando sección: Creación de vistas para análisis ---")
    if not df_eeff.empty:
        df_eeff_anual = df_eeff.drop_duplicates(['PERIODO', 'TIPO', 'ENTIDAD', 'MONEDA'], keep='last').reset_index(
            drop=True)

        df_share = (
            df_eeff_anual.query("MONEDA == 'TOTAL'")
            .assign(
                TOTAL_SEGMENTO=lambda df: df.groupby(["PERIODO", "TIPO"])["INGRESO"].transform("sum"),
                PARTICIPACION=lambda df: df["INGRESO"] / df["TOTAL_SEGMENTO"] * 100
            )
            [["PERIODO", "TIPO", "ENTIDAD", "PARTICIPACION"]]
        )

        df_hhi = (
            df_share.groupby(["PERIODO", "TIPO"])
            .agg(NUM_ENTIDADES=("ENTIDAD", "nunique"), HHI=("PARTICIPACION", lambda x: (x ** 2).sum()))
            .reset_index()
        )

        df_final = df_eeff_anual.merge(df_hhi, how="left", on=["PERIODO", "TIPO"])
        logger.info("Vistas de análisis (Share, HHI) creadas exitosamente. ✅")
    else:
        logger.warning("No hay datos de EEFF para crear vistas de análisis. Omitiendo este paso. ⚠️")
        df_final = pd.DataFrame()

    # --- 5. Guardar Datasets Procesados ---
    logger.info("--- Iniciando sección: Guardado de datos procesados ---")

    # Guardar datos analizados
    if not df_final.empty:
        path = os.path.join(output_processed_dir, 'SBS_DATA_ANALYZED.csv')
        df_final.to_csv(path, index=False, encoding='utf-8-sig')
        logger.info(f"Archivo 'SBS_DATA_ANALYZED.csv' guardado con {len(df_final)} filas.")
    else:
        logger.info("No se generó el archivo 'SBS_DATA_ANALYZED.csv' porque no había datos.")

    # Guardar datos de TC
    if not df_tc.empty:
        path = os.path.join(output_processed_dir, 'SBS_TC.csv')
        df_tc.to_csv(path, index=False, encoding='utf-8-sig')
        logger.info(f"Archivo 'SBS_TC.csv' guardado con {len(df_tc)} filas.")
    else:
        logger.info("No se generó el archivo 'SBS_TC.csv' porque no había datos.")

    logger.info("<<< Fin del procesamiento de datos. 🎉")
