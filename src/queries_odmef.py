"""
Import libraries
"""

import os

import numpy as np
import pandas as pd
from colorama import Fore, Style

os.chdir('../')

from src.transform_odmef import inv

"""
Subtitle
"""

print(
Fore.BLUE
+ Style.BRIGHT
+ """\n
=============================================
Queries de datasets ODMEF
=============================================
\n"""
+ Style.RESET_ALL
)

keys = ['CODIGO_UNICO', 'CODIGO_SNIP']
order = keys + [col for col in inv.columns if col not in keys]
inv = inv.reindex(columns=order)

inv = inv.drop(
    columns=[
        'NOM_OPMI',
        'NOM_UF',
        'NOM_UEI',
        'SEC_EJEC',
        'NOM_UEP',
        'MONTO_LAUDO',
        'MONTO_FIANZA',
        'ALTERNATIVA',
        'DES_MODALIDAD',
        'PRIMER_DEVENGADO',
        'ULTIMO_DEVENGADO',
        'DEVEN_ACUMUL_ANIO_ANT',
        'SALDO_EJECUTAR',
        'TIENE_AVAN_FISICO',
        'AVANCE_FISICO',
        'AVANCE_EJECUCION',
        'ULT_FEC_DECLA_ESTIM',
        'DES_TIPOLOGIA',
        'PROG_ACTUAL_ANIO_ACTUAL',
        'MONTO_VALORIZACION',
        'SANEAMIENTO',
        'FEC_INI_EJECUCION',
        'FEC_FIN_EJECUCION',
        'FEC_INI_EJEC_FISICA',
        'FEC_FIN_EJEC_FISICA',
        'BENEFICIARIO',
        'ANIO_PROCESO',
        'FEC_INI_OPER',
        'RESPONSABLE_OPMI',
        'RESPONSABLE_UEI',
        'RESP_NOMBRE_UF',
        'OPI',
        'RESPONSABLE_OPI',
        'SUBPROGRAM',
        'NUM_HABITANTES_BENEF',
        'ANIO_PROC'
        ]
    )

columns = [
    ['NOMBRE_INVERSION'],
    ['NIVEL',
    'SECTOR',
    'ENTIDAD',
    'ESTADO',
    'SITUACION',
    'MONTO_VIABLE',
    'COSTO_ACTUALIZADO',
    'CTRL_CONCURR',
    'FECHA_REGISTRO',
    'FECHA_VIABILIDAD'],
    ['FUNCION',
    'PROGRAMA',
    'SUBPROGRAMA',
    'MARCO',
    'TIPO_INVERSION'],
    ['REGISTRADO_PMI',
    'PMI_ANIO_1',
    'PMI_ANIO_2',
    'PMI_ANIO_3',
    'PMI_ANIO_4'],
    ['EXPEDIENTE_TECNICO',
    'INFORME_CIERRE',
    'TIENE_F9',
    'FEC_REG_F9',
    'ETAPA_F9'],
    'DEV_ANIO_ACTUAL',
    'PIA_ANIO_ACTUAL',
    'PIM_ANIO_ACTUAL',
    'DEV_ENE_ANIO_VIG',
    'DEV_FEB_ANIO_VIG',
    'DEV_MAR_ANIO_VIG',
    'DEV_ABR_ANIO_VIG',
    'DEV_MAY_ANIO_VIG',
    'DEV_JUN_ANIO_VIG',
    'DEV_JUL_ANIO_VIG',
    'DEV_AGO_ANIO_VIG',
    'DEV_SET_ANIO_VIG',
    'DEV_OCT_ANIO_VIG',
    'DEV_NOV_ANIO_VIG',
    'DEV_DIC_ANIO_VIG',
    'CERTIF_ANIO_ACTUAL',
    'COMPROM_ANUAL_ANIO_ACTUAL',
    'TIENE_F8',
    'ETAPA_F8',
    'TIENE_F12B',
    'FECHA_ULT_ACT_F12B',
    'ULT_PERIODO_REG_F12B',
    'DEPARTAMENTO',
    'PROVINCIA',
    'DISTRITO',
    'UBIGEO',
    'LATITUD',
    'LONGITUD',
    'MONTO_ET_F8',
    'DEVENGADO_ACUMULADO',
    'FEC_CIERRE',
    'DES_CIERRE',
    'CULMINADA',
    'SALDO',
    'EJECUCION',
    'ANIO_REGISTRO',
    'ANIO_VIABILIDAD',
    'RANGO',
    'MODALIDAD'
    ]

inv[
    ['CODIGO_UNICO',
    'CODIGO_SNIP',
    'NOMBRE_INVERSION']
].to_csv("inversiones_1.csv", index=False)

inv[
    ['CODIGO_UNICO',
    'CODIGO_SNIP',
    'NIVEL',
    'SECTOR',
    'ENTIDAD',
    'ESTADO',
    'SITUACION',
    'MONTO_VIABLE',
    'COSTO_ACTUALIZADO',
    'CTRL_CONCURR',
    'FECHA_REGISTRO',
    'FECHA_VIABILIDAD']
].to_csv("inversiones_2.csv", index=False)

inv.to_csv("inversiones.csv", index=False, encoding="utf-8-sig")


inv.fillna(0).to_csv("inversiones.csv", index=False, encoding="utf-8-sig")


