# src/utils.py

import os
import logging

def get_logger(log_file: str = "info.log", level=logging.INFO) -> logging.Logger:
    """
    Configura y devuelve un logger que escribe en log_file sin imprimir en consola.
    """
    logger = logging.getLogger("OpenDataAnalyzer")
    logger.setLevel(level)
    # Evitar duplicados si la función se llama varias veces
    if not logger.handlers:
        handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        ))
        logger.addHandler(handler)
    return logger

def get_project_root(levels: int = 2) -> str:
    """
    Obtiene la ruta raíz del proyecto subiendo 'levels' niveles desde el archivo actual.

    :param levels: Número de niveles de carpetas hacia arriba.
    :return: Ruta absoluta del directorio raíz calculado.
    """
    path = os.path.abspath(__file__)
    for _ in range(levels):
        path = os.path.dirname(path)
    return path