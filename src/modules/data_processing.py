# src/modules/data_processing.py

import os
import warnings
import pandas as pd
from tqdm import tqdm
import src.utils as utils
from typing import Dict

warnings.filterwarnings('ignore')

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


def _localize(df: pd.DataFrame, terms, exact=False, occurrence="first") -> tuple:
    """Busca coincidencias de uno o varios términos dentro de un DataFrame."""
    if isinstance(terms, str):
        terms = [terms]
    terms = [str(t).strip().lower() for t in terms]
    df_str = df.astype(str).where(df.notna(), "").applymap(lambda x: x.strip().lower())

    if exact:
        mask = df_str.isin(terms)
    else:
        mask = df_str.apply(lambda col: col.str.contains('|'.join(map(str, terms)), na=False))

    matches = mask.stack()[lambda s: s]
    if matches.empty:
        return None, None

    if occurrence == "first":
        pos = 0
    elif occurrence == "last":
        pos = -1
    elif isinstance(occurrence, int) and occurrence > 0:
        pos = occurrence - 1
        if pos >= len(matches):
            pos = -1
    else:
        raise ValueError("occurrence debe ser 'first', 'last' o un entero positivo.")

    row, colname = matches.index[pos]
    rowidx = df.index.get_loc(row)
    colidx = df.columns.get_loc(colname)
    return rowidx, colidx


def process_dataset(urls_to_check: Dict[str, str]):
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
    list_banca_eff = [f for f in urls_to_check.keys() if 'Banca' in f and 'EEFF' in f]
    logger.info(f"Se encontraron {len(list_banca_eff)} archivos de 'Banca Múltiple' para extraer TC.")

    for key in tqdm(list_banca_eff, desc="Buscando TC por periodo"):
        try:
            date = int(key.split('_')[-1])
            year = key.split('_')[-1][:4]
            month_num = int(key.split('_')[-1][-2:])
            month_name = MONTHS_MAP[month_num - 1]

            file_path = os.path.join(output_raw_dir, f'{key}.xls')
            dataset_bg = pd.read_excel(file_path, sheet_name=0)

            idx_row, idx_col = _localize(dataset_bg, "Tipo de Cambio")
            if idx_row is not None:
                tc_value = dataset_bg.iloc[idx_row, idx_col]  # Asumimos valor en columna adyacente
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
    list_datasets_eeff = [f for f in urls_to_check.keys() if 'EEFF' in f]
    processed_count = 0
    logger.info(f"Se procesarán {len(list_datasets_eeff)} archivos de EEFF.")

    for key in tqdm(list_datasets_eeff, desc="Procesando EEFF"):
        try:
            file_path = os.path.join(output_raw_dir, f'{key}.xls')
            dataset_eeff = None
            for sheet in [1, 0]:  # Intenta leer la hoja 1, luego la 0
                try:
                    dataset_eeff = pd.read_excel(file_path, sheet_name=sheet)
                    logger.debug(f"Datos leídos desde la hoja {sheet} del archivo '{key}'.")
                    break
                except Exception:
                    logger.debug(f"No se pudo leer la hoja {sheet} de '{key}'. Intentando siguiente.")

            if dataset_eeff is None:
                raise FileNotFoundError(f"No se encontraron hojas de datos válidas en '{key}'.")

            # Extracción de metadatos del nombre del archivo
            date = int(key.split('_')[-1])
            year = int(key.split('_')[-1][:4])
            month_num = int(key.split('_')[-1][-2:])
            month_name = MONTHS_MAP[month_num - 1]
            kind = ' '.join(key.split('_')[:2])

            dataset_eeff = dataset_eeff.dropna(axis=0, how='all').dropna(axis=1, how='all').reset_index(drop=True)

            # Localización de filas clave
            idx_row_if, idx_col_if = _localize(dataset_eeff, FINANCIAL_INCOME_TERMS, exact=True)
            idx_row_isf, _ = _localize(dataset_eeff, SERVICE_INCOME_TERMS, exact=True)
            idx_row_rn, _ = _localize(dataset_eeff, NET_RESULT_TERMS, exact=True)

            if None in [idx_row_if, idx_row_isf, idx_row_rn]:
                raise ValueError(
                    "No se encontraron todas las filas clave (Ingresos Financieros, Servicios, Resultado Neto).")

            # Construcción del DataFrame
            heads = dataset_eeff.iloc[(idx_row_if - 2):(idx_row_if), idx_col_if:].T.fillna(method='ffill')
            heads = pd.MultiIndex.from_frame(heads, names=['ENTIDAD', 'MONEDA'])

            data_rows = {
                'INGRESOS FINANCIEROS': dataset_eeff.iloc[idx_row_if, idx_col_if:].values,
                'INGRESOS SERVICIOS FINANCIEROS': dataset_eeff.iloc[idx_row_isf, idx_col_if:].values,
                'RESULTADO NETO': dataset_eeff.iloc[idx_row_rn, idx_col_if:].values
            }

            temp_df = pd.DataFrame(data_rows, index=heads).T

            # Limpieza y enriquecimiento del DataFrame
            processed_df = (
                temp_df
                .apply(pd.to_numeric, errors="coerce").dropna(axis=0, how='all').reset_index()
                .pipe(lambda df: df[~(
                            df['ENTIDAD'].str.lower().str.startswith('total') | df['ENTIDAD'].str.lower().str.contains(
                        'sucursal'))])
                .assign(
                    ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.replace(r"[\d*/()]", "", regex=True).str.strip(),
                    DATE=date, PERIODO=year, MES=month_name, TIPO=kind,
                    INGRESO=lambda df: df["INGRESOS FINANCIEROS"] + df["INGRESOS SERVICIOS FINANCIEROS"]
                )
                [['DATE', 'PERIODO', 'MES', 'TIPO', 'ENTIDAD', 'MONEDA', 'INGRESOS FINANCIEROS',
                  'INGRESOS SERVICIOS FINANCIEROS', 'INGRESO', 'RESULTADO NETO']]
            )
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