import requests
import time
from typing import List, Dict, Any, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Tenta carregar configs, fallback se falhar
try:
    from src.config import CONFIG
    OMIE_APP_KEY = CONFIG.OMIE_APP_KEY
    OMIE_APP_SECRET = CONFIG.OMIE_APP_SECRET
    TIMEOUT_REQUEST = 90  # Timeout alto para garantir downloads grandes
except ImportError:
    OMIE_APP_KEY = ""
    OMIE_APP_SECRET = ""
    TIMEOUT_REQUEST = 90

from src.infrastructure.custom_logging import logger

class OmieClient:
    """
    Cliente HTTP Otimizado para ETL (Extração em Lote).
    """
    ENDPOINT_PEDIDOS = "https://app.omie.com.br/api/v1/produtos/pedido/"
    ENDPOINT_NFE = "https://app.omie.com.br/api/v1/produtos/nfconsultar/"
    
    def __init__(self):
        self.api_key = OMIE_APP_KEY
        self.api_secret = OMIE_APP_SECRET
        
        # Estratégia de Retry Agressiva para erros de servidor
        retry_strategy = Retry(
            total=5,
            backoff_factor=2, # 2s, 4s, 8s, 16s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Python ETL Service/1.0'
        })

    def request(self, endpoint: str, call: str, param: Optional[Union[Dict, List]] = None) -> Dict[str, Any]:
        """
        Executa a requisição POST padrão Omie com tratamento de erros de negócio.
        """
        payload = {
            "call": call,
            "app_key": self.api_key,
            "app_secret": self.api_secret,
            "param": [param] if isinstance(param, dict) else (param or [])
        }

        try:
            # THROTTLING: Pausa obrigatória para evitar sobrecarga (Erro 500/429)
            time.sleep(0.4) 
            
            response = self.session.post(endpoint, json=payload, timeout=TIMEOUT_REQUEST)
            response.raise_for_status()
            
            data = response.json()
            
            # Verifica erros lógicos da API (ex: página não existe, chave inválida)
            if "faultstring" in data:
                error_msg = data.get("faultstring", "")
                
                # "ERROR: Não existem registros..." é normal no fim da paginação
                if "não existem registros" in str(error_msg).lower():
                    return {
                        "total_de_paginas": 0, 
                        "registros": [], 
                        "pedido_venda_produto": [], 
                        "nfCadastro": []
                    }
                
                logger.error(f"⛔ Erro Lógico Omie [{call}]: {error_msg}")
                raise Exception(f"Omie Logic Error: {error_msg}")
                
            return data

        except Exception as e:
            logger.error(f"❌ Falha na requisição [{call}]: {str(e)[:150]}...")
            raise

    # --- MÉTODOS ESPECÍFICOS DE EXTRAÇÃO ---

    def listar_pedidos(self, pagina: int, data_de: str, data_ate: str) -> dict:
        """
        Baixa página de PEDIDOS. 
        Paginação: 50 itens (Pedidos são leves).
        """
        param = {
            "pagina": pagina,
            "registros_por_pagina": 50, 
            "apenas_importado_api": "N",
            "filtrar_por_data_de": data_de,
            "filtrar_por_data_ate": data_ate,
            "apenas_resumo": "N" # Traz os itens para validação futura
        }
        return self.request(self.ENDPOINT_PEDIDOS, "ListarPedidos", param)

    def listar_nfs(self, pagina: int, data_de: str, data_ate: str) -> dict:
        """
        Baixa página de NOTAS FISCAIS.
        Paginação: 20 itens (NFs são pesadas, contêm XML completo).
        """
        param = {
            "nPagina": pagina,
            "nRegPorPagina": 20, # Reduzido para evitar Timeout/500
            "apenas_importado_api": "N",
            "dEmiInicial": data_de,
            "dEmiFinal": data_ate,
        }
        return self.request(self.ENDPOINT_NFE, "ListarNFes", param)