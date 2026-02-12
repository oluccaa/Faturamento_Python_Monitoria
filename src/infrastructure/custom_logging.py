import logging
import sys
import copy
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

# Tenta importar CONFIG, mas define falbacks se falhar (evita crash em import circular)
try:
    from src.config import CONFIG
    LOG_FILE_PATH = CONFIG.LOG_FILE
    APP_NAME = getattr(CONFIG, "APP_NAME", "AppPadrao")
except ImportError:
    LOG_FILE_PATH = Path("logs/app.log")
    APP_NAME = "AppFallback"

# ==============================================================================
# INFRAESTRUTURA: SISTEMA DE LOGS (Refatorado & Otimizado)
# ==============================================================================

class ColoredConsoleFormatter(logging.Formatter):
    """
    Formatador de alta performance para o console com cache de formatters.
    """
    
    # Definição de Cores ANSI
    GREY = "\x1b[38;20m"
    GREEN = "\x1b[32;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    # Formato base
    BASE_FMT = "%(asctime)s | %(levelname)-8s | (%(filename)s:%(lineno)d) | %(message)s"

    def __init__(self):
        super().__init__(datefmt='%H:%M:%S')
        
        # OTIMIZAÇÃO: Pré-compila os formatters INTEIROS, não apenas as strings.
        # Assim não recriamos objetos logging.Formatter a cada log (Ganho de CPU).
        self.FORMATTERS = {
            logging.DEBUG: logging.Formatter(self.GREY + self.BASE_FMT + self.RESET, datefmt='%H:%M:%S'),
            logging.INFO: logging.Formatter(self.GREEN + self.BASE_FMT + self.RESET, datefmt='%H:%M:%S'),
            logging.WARNING: logging.Formatter(self.YELLOW + self.BASE_FMT + self.RESET, datefmt='%H:%M:%S'),
            logging.ERROR: logging.Formatter(self.RED + self.BASE_FMT + self.RESET, datefmt='%H:%M:%S'),
            logging.CRITICAL: logging.Formatter(self.BOLD_RED + self.BASE_FMT + self.RESET, datefmt='%H:%M:%S'),
        }

    def format(self, record):
        # A cópia é necessária para não colorir o log que vai para o arquivo
        record_copy = copy.copy(record)
        
        # Busca o formatter pré-compilado ou usa o padrão (Debug/Grey)
        formatter = self.FORMATTERS.get(record.levelno, self.FORMATTERS[logging.DEBUG])
        
        return formatter.format(record_copy)

def setup_logger() -> logging.Logger:
    """
    Configura o Logger Singleton da Aplicação.
    """
    logger = logging.getLogger(APP_NAME)
    
    # Se já tiver handlers configurados, retorna o existente (Singleton Pattern real)
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # Evita duplicar logs no console raiz do Python

    # --- 1. CONFIGURAÇÃO DO ARQUIVO (File Handler) ---
    try:
        log_dir = LOG_FILE_PATH.parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | [%(module)s:%(funcName)s] | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = RotatingFileHandler(
            LOG_FILE_PATH,
            maxBytes=20 * 1024 * 1024,  # 20 MB
            backupCount=10,             # Mantém 10 arquivos
            encoding='utf-8',
            delay=True                  # Cria arquivo só ao escrever
        )
        
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO) # Arquivo recebe INFO e acima
        logger.addHandler(file_handler)

    except (PermissionError, OSError) as e:
        sys.stderr.write(f"⚠️  [LOGGER] Falha ao criar arquivo de log: {e}\n")

    # --- 2. CONFIGURAÇÃO DO CONSOLE (Stream Handler) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredConsoleFormatter())
    console_handler.setLevel(logging.DEBUG) # Console mostra tudo
    
    logger.addHandler(console_handler)

    logger.debug(f"Logger inicializado. Gravando em: {LOG_FILE_PATH}")

    return logger

# Instância Singleton exportada
logger = setup_logger()