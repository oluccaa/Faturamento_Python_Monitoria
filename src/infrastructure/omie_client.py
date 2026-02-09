import requests
from src.config import CONFIG
from src.infrastructure.logging import logger

class OmieClient:
    def __init__(self):
        self.url = "https://app.omie.com.br/api/v1/produtos/pedido/"

    def post(self, call: str, param: dict) -> dict:
        """Centraliza as chamadas POST para a Omie com tratamento de erro."""
        payload = {
            "call": call,
            "app_key": CONFIG.OMIE_APP_KEY,
            "app_secret": CONFIG.OMIE_APP_SECRET,
            "param": [param] # A Omie espera o parâmetro dentro de uma lista
        }
        
        try:
            response = requests.post(self.url, json=payload, timeout=CONFIG.TIMEOUT_REQUEST)
            
            # Se der erro 500, logamos o JSON de resposta para depuração real
            if response.status_code == 500:
                logger.error(f"❌ Erro 500 na Omie: {response.text}")
                
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"💥 Falha na comunicação com a API: {e}")
            raise