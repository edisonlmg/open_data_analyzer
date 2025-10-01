# src/utils.py

import logging

def get_logger(process_name: str, level=logging.INFO) -> logging.Logger:
    """
    Configura y devuelve un logger que escribe en {process_name}_info.log
    
    Args:
        process_name: Nombre del proceso (ej: "sbs", "sunat", "bcrp")
        level: Nivel de logging (default: INFO)
    
    Returns:
        Logger configurado para el proceso específico
    """
    log_file = f"{process_name}_info.log"
    logger_name = f"OpenDataAnalyzer.{process_name}"
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Evitar duplicados si la función se llama varias veces
    if not logger.handlers:
        handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        ))
        logger.addHandler(handler)
    
    return logger

