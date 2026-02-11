import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    """
    Configuração centralizada.
    Agora inclui o controle de período via variáveis de ambiente.
    """
    # --- Identidade do Sistema ---
    APP_NAME: str = "OmieDataExtractor"
    VERSION: str = "1.0.0"
    
    # --- Caminhos de Estrutura ---
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    LOG_DIR: Path = BASE_DIR / "logs"
    LOG_FILE: Path = LOG_DIR / "sentinel.log" 
    OUTPUT_DIR: Path = BASE_DIR / "data" / "processed_billing"
    
    # --- Credenciais Omie ---
    OMIE_APP_KEY: str = os.getenv("OMIE_APP_KEY", "")
    OMIE_APP_SECRET: str = os.getenv("OMIE_APP_SECRET", "")
    
    # --- Configurações de Período (.env) ---
    # Se não estiver no .env, virá vazio e trataremos no main
    DATA_INICIO: str = os.getenv("DATA_INICIO", "")
    DATA_FIM: str = os.getenv("DATA_FIM", "")
    
    # --- Configurações de Requisição ---
    TIMEOUT_REQUEST: int = int(os.getenv("API_TIMEOUT", 40))

    def __post_init__(self):
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG = AppConfig()