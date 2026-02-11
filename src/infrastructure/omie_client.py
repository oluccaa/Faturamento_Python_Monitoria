import requests
from src.config import CONFIG
from src.infrastructure.custom_logging import logger

class OmieClient:
    def __init__(self):
        self.api_key = CONFIG.OMIE_APP_KEY
        self.api_secret = CONFIG.OMIE_APP_SECRET
        # URL correta para o método ListarPedidos
        self.url = "https://app.omie.com.br/api/v1/produtos/pedido/"
        self._headers = {'Content-Type': 'application/json'}

    def post(self, call: str, param: dict) -> dict:
        """
        Método genérico POST para chamadas da API Omie.
        Compatível com a chamada feita em monitor_faturamento.py.
        """
        payload = {
            "call": call,
            "app_key": self.api_key,
            "app_secret": self.api_secret,
            "param": [param]
        }
        
        try:
            # Faz a requisição POST
            response = requests.post(self.url, json=payload, headers=self._headers, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Verifica se a API retornou um erro de aplicação (ex: falha interna)
            if data.get("faultstring"):
                raise Exception(f"Erro Omie API: {data.get('faultstring')}")
                
            return data

        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout na chamada {call} após 60s.")
            raise
        except Exception as e:
            logger.error(f"❌ Erro na API Omie ({call}): {e}")
            raise

    # Mantive este método como alias, caso algum outro arquivo antigo tente usá-lo
    def listar_pedidos(self, pagina: int, data_de: str, data_ate: str):
        param = {
            "pagina": pagina,
            "registros_por_pagina": 100,
            "apenas_importado_api": "N",
            "filtrar_por_data_de": data_de,
            "filtrar_por_data_ate": data_ate,
            "apenas_resumo": "N"
        }
        return self.post("ListarPedidos", param)