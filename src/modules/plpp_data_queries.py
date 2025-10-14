# src/modules/plpp_data_queries.py

import pandas as pd

def _transform_values(df_plpp: pd.DataFrame) -> pd.DataFrame:
    df_transformed = df_plpp.assign(
        NIVEL_ENUMERATE = lambda df: (
            df['NIVEL_GOBIERNO'].astype(str)+'. '+df['NIVEL_GOBIERNO_NOMBRE']
        ),
        SECTOR_ENUMERATE = lambda df: (
            df['SECTOR'].astype(str)+'. '+df['SECTOR_NOMBRE']
        ),
        PLIEGO_ENUMERATE = lambda df: (
            df['PLIEGO'].astype(str)+'. '+df['PLIEGO_NOMBRE']
        ),
        FUNCION_ENUMERATE = lambda df: (
            df['FUNCION'].astype(str)+'. '+df['FUNCION_NOMBRE']
        ),
        FUENTE_FINANCIAMIENTO_ENUMERATE = lambda df: (
            df['FUENTE_FINANCIAMIENTO'].astype(str)+'. '+df['FUENTE_FINANCIAMIENTO_NOMBRE']
        ),
        CATEGORIA_GASTO_ENUMERATE = lambda df: (
            df['CATEGORIA_GASTO'].astype(str)+'. '+df['CATEGORIA_GASTO_NOMBRE']
        ),
        GENERICA_ENUMERATE = lambda df: (
            df['GENERICA'].astype(str)+'. '+df['GENERICA_NOMBRE']
        )
    )
    return df_transformed

def _query_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consulta para obtener el total del presupuesto asignado por año.
    """
    df_grouped = (
        df
        .pipe(_transform_values)
        .groupby(
            [
                'ANO_EJE',
                'NIVEL_ENUMERATE',
                'SECTOR_ENUMERATE',
                'PLIEGO_ENUMERATE',
                'DEPARTAMENTO_NOMBRE',
                'PROVINCIA_NOMBRE',
                'FUENTE_FINANCIAMIENTO_ENUMERATE',
                'CATEGORIA_GASTO_ENUMERATE',
                'GENERICA_ENUMERATE'
            ],
            dropna=False,
            as_index=False
            )
        .agg(
            {
                'MONTO_PL':'sum',
                'MONTO_LEY':'sum',
                'MONTO_PIA':'sum',
            }
            )
        .reset_index(drop=True)
        )
    return df_grouped

def _query_funcion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consulta para obtener el total del presupuesto asignado por año.
    """
    df_grouped = (
        df.loc[df['ANO_EJE']>df['ANO_EJE'].max()-2]
        .pipe(_transform_values)
        .groupby(
            [
                'ANO_EJE',
                'NIVEL_ENUMERATE',
                'SECTOR_ENUMERATE',
                'PLIEGO_ENUMERATE',
                'DEPARTAMENTO_NOMBRE',
                'PROVINCIA_NOMBRE',
                'FUNCION_ENUMERATE',
                'GENERICA_ENUMERATE'
            ],
            dropna=False,
            as_index=False
            )
        .agg(
            {
                'MONTO_PL':'sum',
                'MONTO_LEY':'sum',
                'MONTO_PIA':'sum',
            }
            )
        .reset_index(drop=True)
        )
    return df_grouped

def _query_large(df_query: pd.DataFrame) -> pd.DataFrame:
    """
    Consulta para obtener el total del presupuesto asignado por año.
    """
    df_large = (
        df_query
        .melt(
            id_vars=[col for col in df_query.columns if not col.startswith("MONTO_")],
            value_vars=["MONTO_PL", "MONTO_LEY", "MONTO_PIA"],
            var_name="DISPOSITIVO",
            value_name="MONTO"
        )
        .assign(DISPOSITIVO=lambda df: df["DISPOSITIVO"].str.replace("MONTO_", "", regex=False)))
    return df_large

def queries_dataset_plpp(df_plpp: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Ejecuta las consultas predefinidas sobre el DataFrame del PLPP.
    Devuelve un diccionario con los resultados.
    """
    dict_queries = {
        "PLPP_CATEGORIA": _query_categoria(df_plpp),
        "PLPP_FUNCION": _query_funcion(df_plpp)
    }
    
    dict_queries_large = {f"{key}_LARGE": _query_large(df) for key, df in dict_queries.items()}
    
    dict_queries.update(dict_queries_large)
    
    return dict_queries

