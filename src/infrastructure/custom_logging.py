import logging
import sys
import copy
from logging.handlers import RotatingFileHandler
from pathlib import Path
from src.config import CONFIG

# ==============================================================================
# INFRAESTRUTURA: SISTEMA DE LOGS (Refatorado)
# ==============================================================================

class ColoredConsoleFormatter(logging.Formatter):
    """
    Formatador de alta performance para o console.
    Aplica cores apenas nos níveis e metadados, sem poluir a mensagem original.
    """
    
    # Definição de Cores ANSI
    GREY = "\x1b[38;20m"
    GREEN = "\x1b[32;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    # Formato base para o console (mais compacto que o do arquivo)
    # Ex: 14:00:01 | INFO     | (main.py:45) | Mensagem...
    BASE_FMT = "%(asctime)s | %(levelname)-8s | (%(filename)s:%(lineno)d) | %(message)s"

    def __init__(self):
        super().__init__(fmt=self.BASE_FMT, datefmt='%H:%M:%S')
        
        # Pré-compila os formatos para evitar processamento a cada log (Ganho de Performance)
        self.FORMATS = {
            logging.DEBUG: self.GREY + self.BASE_FMT + self.RESET,
            logging.INFO: self.GREEN + self.BASE_FMT + self.RESET,
            logging.WARNING: self.YELLOW + self.BASE_FMT + self.RESET,
            logging.ERROR: self.RED + self.BASE_FMT + self.RESET,
            logging.CRITICAL: self.BOLD_RED + self.BASE_FMT + self.RESET,
        }

    def format(self, record):
        # IMPORTANTE: Criamos uma cópia do record para não injetar 
        # códigos de cor no LogRecord original. Se não fizermos isso, 
        # o RotatingFileHandler (arquivo) acabaria recebendo cores também,
        # sujando o arquivo de texto com caracteres estranhos.
        record_copy = copy.copy(record)
        
        log_fmt = self.FORMATS.get(record_copy.levelno, self.BASE_FMT)
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record_copy)

def setup_logger() -> logging.Logger:
    """
    Configura o Logger Singleton da Aplicação.
    """
    # Recupera ou cria o logger baseado no nome da app definido no CONFIG
    logger_name = getattr(CONFIG, "APP_NAME", "AppPadrao")
    logger = logging.getLogger(logger_name)
    
    # Define o nível GLOBAL como DEBUG (os handlers filtrarão depois)
    logger.setLevel(logging.DEBUG)

    # Evita duplicação de logs se a função for chamada múltiplas vezes (hot reload)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Define o formatter do arquivo (Completo e limpo, sem cores)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | [%(module)s:%(funcName)s] | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # --- 1. CONFIGURAÇÃO DO ARQUIVO (File Handler) ---
    try:
        log_file = CONFIG.LOG_FILE
        log_dir = log_file.parent
        
        # Criação segura do diretório
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=20 * 1024 * 1024,  # 20 MB
            backupCount=10,             # Mantém 10 arquivos antigos
            encoding='utf-8',
            delay=True                  # Cria o arquivo só ao escrever (Lazy)
        )
        
        file_handler.setFormatter(file_formatter)
        # O nível do arquivo geralmente é INFO para não "encher" o disco com DEBUG
        file_handler.setLevel(logging.INFO) 
        
        logger.addHandler(file_handler)

    except (PermissionError, OSError) as e:
        # Fallback: Se não conseguir criar arquivo, avisa no console mas não quebra a app
        sys.stderr.write(f"⚠️  AVISO DE INFRA: Não foi possível criar log em arquivo: {e}\n")

    # --- 2. CONFIGURAÇÃO DO CONSOLE (Stream Handler) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredConsoleFormatter())
    
    # O console mostra tudo (DEBUG), útil para desenvolvimento
    console_handler.setLevel(logging.DEBUG)
    
    logger.addHandler(console_handler)

    # Log de inicialização para confirmar que o sistema subiu
    logger.debug(f"Logger inicializado. Gravando em: {CONFIG.LOG_FILE}")

    return logger

# Instância Singleton exportada
logger = setup_logger()