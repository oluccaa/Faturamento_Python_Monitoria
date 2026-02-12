import requests
from typing import List, Dict, Any, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Fallback para evitar erro de import circular se config falhar
try:
    from src.config import CONFIG
    OMIE_APP_KEY = CONFIG.OMIE_APP_KEY
    OMIE_APP_SECRET = CONFIG.OMIE_APP_SECRET
    TIMEOUT_REQUEST = CONFIG.TIMEOUT_REQUEST
    APP_NAME = CONFIG.APP_NAME
    VERSION = CONFIG.VERSION
except ImportError:
    OMIE_APP_KEY = ""
    OMIE_APP_SECRET = ""
    TIMEOUT_REQUEST = 60
    APP_NAME = "OmieClient"
    VERSION = "1.0"

from src.infrastructure.custom_logging import logger

class OmieClient:
    """
    Cliente HTTP Padronizado para API Omie (v1).
    Implementa Connection Pooling, Auto-Retries e tratamento de erros nativo.
    """
    
    # Endpoints Padrão (Imutáveis)
    ENDPOINT_PEDIDOS = "https://app.omie.com.br/api/v1/produtos/pedido/"
    ENDPOINT_NFE = "https://app.omie.com.br/api/v1/produtos/nfconsultar/"
    
    def __init__(self):
        self.api_key = OMIE_APP_KEY
        self.api_secret = OMIE_APP_SECRET
        
        # Configuração de Resiliência (Retry Strategy)
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,  # Espera 1s, 2s, 4s entre tentativas
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        # OTIMIZAÇÃO: Session para reutilização de conexão TCP/SSL (Keep-Alive)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': f'{APP_NAME}/{VERSION} (Python Service)'
        })

    def request(self, endpoint: str, call: str, param: Optional[Union[Dict, List]] = None) -> Dict[str, Any]:
        """
        Método 'Core' que segue estritamente a estrutura de envelope JSON da Omie.
        """
        # Padrão Omie: 'param' deve ser sempre uma lista de objetos.
        safe_param = [param] if isinstance(param, dict) else (param or [])

        payload = {
            "call": call,
            "app_key": self.api_key,
            "app_secret": self.api_secret,
            "param": safe_param
        }

        try:
            response = self.session.post(
                endpoint, 
                json=payload, 
                timeout=TIMEOUT_REQUEST
            )
            
            response.raise_for_status()
            
            data = response.json()
            
            # Tratamento de Erro Lógico da Omie (Status 200, mas com falha de negócio)
            if "faultstring" in data:
                error_msg = data.get("faultstring")
                # Não logamos como erro se for apenas "Não existem registros", pois é comum em paginação final
                if "não existem registros" in str(error_msg).lower():
                    logger.debug(f"ℹ️ Fim da paginação ou sem dados para {call}: {error_msg}")
                    return {"total_de_paginas": 0, "registros": []} # Retorno seguro vazio
                
                logger.error(f"⛔ Erro de Negócio Omie [{call}]: {error_msg}")
                raise Exception(f"Omie API Logical Error: {error_msg}")
                
            return data

        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na chamada {call} (Limite: {TIMEOUT_REQUEST}s).")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"📡 Erro HTTP na Omie ({call}): {e.response.status_code} - {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Falha Crítica de Conexão em {call}: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Erro Inesperado no Cliente Omie ({call}): {e}")
            raise

    # -------------------------------------------------------------------------
    # Métodos de Domínio (Abstração para chamadas do Sistema)
    # -------------------------------------------------------------------------
    
    def listar_pedidos(self, pagina: int, data_de: str, data_ate: str) -> dict:
        """
        Executa a listagem de pedidos de produtos faturados/venda.
        """
        param = {
            "pagina": pagina,
            "registros_por_pagina": 50, # Reduzi para 50 para evitar timeouts em payloads gigantes
            "apenas_importado_api": "N",
            "filtrar_por_data_de": data_de,
            "filtrar_por_data_ate": data_ate,
            "apenas_resumo": "N" # Traz os ITENS (Crucial para a validação)
        }
        return self.request(self.ENDPOINT_PEDIDOS, "ListarPedidos", param)

    def listar_nfs(self, pagina: int, data_de: str, data_ate: str) -> dict:
        """
        Executa a listagem de Notas Fiscais (NFe) no período.
        CORRIGIDO: Parâmetros ajustados para o padrão 'ListarNF'.
        """
        param = {
            "nPagina": pagina,            # Omie usa nPagina aqui
            "nRegPorPagina": 50,          # Omie usa nRegPorPagina aqui
            "apenas_importado_api": "N",  # Este costuma ser igual
            "dEmiInicial": data_de,       # Filtro de data inicial
            "dEmiFinal": data_ate,        # Filtro de data final
            # Removido 'ordenar_por' pois nem sempre é respeitado ou necessário
        }
        return self.request(self.ENDPOINT_NFE, "ListarNF", param)