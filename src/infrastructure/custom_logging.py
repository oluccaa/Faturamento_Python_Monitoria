import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Configuração Padrão
LOG_DIR = Path("logs")
LOG_FILE_NAME = "faturamento_etl.log"
LOG_PATH = LOG_DIR / LOG_FILE_NAME

class ColoredConsoleFormatter(logging.Formatter):
    """
    Formatador para colorir o console e facilitar a leitura do operador.
    """
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s | %(levelname)-8s | %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: green + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

def setup_logger(name="BillingMonitor"):
    """
    Configura o logger global da aplicação.
    """
    # Garante que o diretório de logs existe
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Evita duplicidade de logs se reiniciado
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Handler de Arquivo (Rotação de 10MB, guarda 5 arquivos)
    file_handler = RotatingFileHandler(
        LOG_PATH, 
        maxBytes=10*1024*1024, 
        backupCount=5, 
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | [%(filename)s:%(lineno)d] | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    # 2. Handler de Console (Colorido)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO) # No console, mostra apenas INFO para cima
    console_handler.setFormatter(ColoredConsoleFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Instância global do logger
logger = setup_logger()