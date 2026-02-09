import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    """
    Configuração simplificada focada exclusivamente na API Omie.
    Resolvendo o erro de atributo ausente para o sistema de logs.
    """
    # --- Identidade do Sistema ---
    APP_NAME: str = "OmieDataExtractor"
    VERSION: str = "1.0.0"
    
    # --- Caminhos de Estrutura ---
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    LOG_DIR: Path = BASE_DIR / "logs"
    # Adicionado para corrigir o erro de log
    LOG_FILE: Path = LOG_DIR / "sentinel.log" 
    
    # Pasta onde os JSONs limpos serão salvos
    OUTPUT_DIR: Path = BASE_DIR / "data" / "processed_billing"
    
    # --- Credenciais Omie (Ligadas ao .env) ---
    OMIE_APP_KEY: str = os.getenv("OMIE_APP_KEY", "")
    OMIE_APP_SECRET: str = os.getenv("OMIE_APP_SECRET", "")
    
    # --- Configurações de Requisição ---
    TIMEOUT_REQUEST: int = int(os.getenv("API_TIMEOUT", 40))

    def __post_init__(self):
        """Garante que as pastas de saída existam ao iniciar o sistema."""
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Instância única para importação
CONFIG = AppConfig()