import requests
from typing import List, Dict, Any, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import CONFIG
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
        self.api_key = CONFIG.OMIE_APP_KEY
        self.api_secret = CONFIG.OMIE_APP_SECRET
        
        # Configuração de Resiliência (Retry Strategy)
        # Tenta 3 vezes em erros de conexão ou códigos HTTP específicos (502, 503, 504)
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
            'User-Agent': f'{CONFIG.APP_NAME}/{CONFIG.VERSION} (Python Service)'
        })

    def request(self, endpoint: str, call: str, param: Optional[Union[Dict, List]] = None) -> Dict[str, Any]:
        """
        Método 'Core' que segue estritamente a estrutura de envelope JSON da Omie.
        
        Args:
            endpoint (str): URL completa do recurso.
            call (str): Nome da função na API.
            param (dict | list): Parâmetros da chamada. Encapsula em lista se for dict.
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
            # Timeout via CONFIG para evitar processos travados
            response = self.session.post(
                endpoint, 
                json=payload, 
                timeout=CONFIG.TIMEOUT_REQUEST
            )
            
            # Captura erros HTTP (4xx, 5xx)
            response.raise_for_status()
            
            data = response.json()
            
            # Tratamento de Erro Lógico da Omie (Status 200, mas com falha de negócio)
            if "faultstring" in data:
                error_msg = data.get("faultstring")
                logger.error(f"⛔ Erro de Negócio Omie [{call}]: {error_msg}")
                raise Exception(f"Omie API Logical Error: {error_msg}")
                
            return data

        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na chamada {call} (Limite: {CONFIG.TIMEOUT_REQUEST}s).")
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
    
    def post(self, call: str, param: dict) -> dict:
        """
        Alias para manter compatibilidade com o código legado.
        Assume o endpoint de Pedidos de Venda por padrão.
        """
        return self.request(self.ENDPOINT_PEDIDOS, call, param)

    def listar_pedidos(self, pagina: int, data_de: str, data_ate: str, apenas_resumo: bool = False) -> dict:
        """
        Executa a listagem de pedidos de produtos faturados/venda.
        """
        param = {
            "pagina": pagina,
            "registros_por_pagina": 100,
            "apenas_importado_api": "N",
            "filtrar_por_data_de": data_de,
            "filtrar_por_data_ate": data_ate,
            "apenas_resumo": "S" if apenas_resumo else "N"
        }
        return self.request(self.ENDPOINT_PEDIDOS, "ListarPedidos", param)

    def listar_nfs(self, pagina: int, data_de: str, data_ate: str) -> dict:
        """
        Executa a listagem de Notas Fiscais (NFe) no período.
        """
        param = {
            "pagina": pagina,
            "registros_por_pagina": 100,
            "apenas_importado_api": "N",
            "ordenar_por": "CODIGO",
            "dEmiInicial": data_de,
            "dEmiFinal": data_ate
        }
        return self.request(self.ENDPOINT_NFE, "ListarNF", param)