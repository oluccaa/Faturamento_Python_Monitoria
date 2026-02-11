import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis do .env (se existir)
load_dotenv()

def get_env_int(key: str, default: int) -> int:
    """
    Helper seguro para converter variáveis de ambiente em inteiros.
    Evita quebra da aplicação se o valor no .env for inválido (ex: '30s').
    """
    val = os.getenv(key)
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        # Em caso de erro, retorna o default e segue o jogo (segurança)
        return default

@dataclass(frozen=True)
class AppConfig:
    """
    Configuração centralizada imutável (Frozen).
    Define caminhos, credenciais e parâmetros globais.
    """
    # --- Identidade do Sistema ---
    APP_NAME: str = "OmieDataExtractor"
    VERSION: str = "1.0.0"
    
    # --- Caminhos de Estrutura ---
    # __file__ = src/config.py -> parent = src -> parent = raiz do projeto
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    
    # Definição dinâmica baseada no BASE_DIR
    LOG_DIR: Path = BASE_DIR / "logs"
    LOG_FILE: Path = LOG_DIR / "sentinel.log" 
    OUTPUT_DIR: Path = BASE_DIR / "data" / "processed_billing"
    
    # --- Credenciais Omie ---
    OMIE_APP_KEY: str = os.getenv("OMIE_APP_KEY", "")
    OMIE_APP_SECRET: str = os.getenv("OMIE_APP_SECRET", "")
    
    # --- Configurações de Período ---
    DATA_INICIO: str = os.getenv("DATA_INICIO", "")
    DATA_FIM: str = os.getenv("DATA_FIM", "")
    
    # --- Configurações de Requisição ---
    # Usa o helper seguro para garantir que sempre teremos um inteiro válido
    TIMEOUT_REQUEST: int = get_env_int("API_TIMEOUT", 40)

    def __post_init__(self):
        """
        Garante a existência das pastas críticas ao inicializar a configuração.
        Nota: Em arquiteturas maiores, isso ficaria no 'main', mas aqui é prático.
        """
        try:
            self.LOG_DIR.mkdir(parents=True, exist_ok=True)
            self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            # Não quebramos a app aqui, deixamos o logger tentar lidar depois ou printamos erro crítico
            print(f"⚠️  CRÍTICO: Falha ao criar diretórios de infraestrutura: {e}")

# Instância Global Singleton
CONFIG = AppConfig()