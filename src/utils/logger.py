"""
Sistema de logging para o pipeline ECD
"""
import logging
import os
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = 'ecd',
    log_file: Optional[str] = None,
    level: str = 'INFO'
) -> logging.Logger:
    """
    Configura e retorna um logger

    Args:
        name: Nome do logger
        log_file: Caminho do arquivo de log (opcional)
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Logger configurado
    """
    # Criar diretório de logs se não existir
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

    # Configurar logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Remover handlers existentes
    logger.handlers = []

    # Formato de log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler para arquivo (se especificado)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_execution_time(logger: logging.Logger, start_time: datetime, task_name: str):
    """
    Registra tempo de execução de uma tarefa

    Args:
        logger: Logger a ser usado
        start_time: Timestamp de início
        task_name: Nome da tarefa
    """
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"{task_name} concluído em {elapsed:.2f} segundos")


def log_dataframe_info(logger: logging.Logger, df, df_name: str):
    """
    Registra informações sobre um DataFrame Spark

    Args:
        logger: Logger a ser usado
        df: DataFrame Spark
        df_name: Nome do DataFrame para log
    """
    try:
        count = df.count()
        logger.info(f"{df_name}: {count:,} registros")
        return count
    except Exception as e:
        logger.error(f"Erro ao contar registros de {df_name}: {str(e)}")
        return 0
