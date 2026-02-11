import requests
from src.config import CONFIG
from src.infrastructure.custom_logging import logger

class OmieClient:
    def __init__(self):
        self.api_key = CONFIG.OMIE_APP_KEY
        self.api_secret = CONFIG.OMIE_APP_SECRET
        self.base_url = "https://app.omie.com.br/api/v1/produtos/pedido/"
        self._headers = {'Content-Type': 'application/json'}

    def listar_pedidos(self, pagina: int, data_de: str, data_ate: str):
        payload = {
            "call": "ListarPedidos",
            "app_key": self.api_key,
            "app_secret": self.api_secret,
            "param": [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "apenas_importado_api": "N",
                "filtrar_por_data_de": data_de,
                "filtrar_por_data_ate": data_ate,
                "apenas_resumo": "N" # Traz o JSON completo como você pediu
            }]
        }
        
        try:
            response = requests.post(self.base_url, json=payload, headers=self._headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Erro na API Omie: {e}")
            raise