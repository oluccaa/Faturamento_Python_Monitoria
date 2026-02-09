import json
import time
from datetime import datetime
from src.config import CONFIG
from src.infrastructure.omie_client import OmieClient
from src.infrastructure.logging import logger
from src.domain.services import BillingDomainService

class BillingApplication:
    def __init__(self):
        self.client = OmieClient()
        self.domain = BillingDomainService()

    def run_extraction(self, data_inicio: str, data_fim: str):
        logger.info(f"🚀 Iniciando Extração DDD: {data_inicio} até {data_fim}")
        
        all_cleaned_orders = {}
        page = 1
        total_pages = 1

        while page <= total_pages:
            param = {
                "pagina": page,
                "registros_por_pagina": 100,
                "filtrar_por_data_de": data_inicio,
                "filtrar_por_data_ate": data_fim,
                "apenas_resumo": "N"
            }

            try:
                data = self.client.post("ListarPedidos", param)
                total_pages = data.get("total_de_paginas", 1)
                orders = data.get("pedido_venda_produto", [])

                if isinstance(orders, dict): orders = [orders]

                for order in orders:
                    pv = str(order.get("cabecalho", {}).get("numero_pedido", "S_NUM"))
                    all_cleaned_orders[pv] = self.domain.clean_order_data(order)

                logger.info(f"📄 Página {page}/{total_pages} ok. Total parcial: {len(all_cleaned_orders)}")
                page += 1
                time.sleep(0.2)

            except Exception:
                break

        self._save(all_cleaned_orders, data_inicio)

    def _save(self, data: dict, ref: str):
        if not data: return
        path = CONFIG.OUTPUT_DIR / f"faturamento_{ref.replace('/', '_')}.json"
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"💾 Sucesso! {len(data)} pedidos salvos em {path}")

if __name__ == "__main__":
    app = BillingApplication()
    print("\n1. Mês Atual | 2. Período Customizado")
    op = input("Opção: ")
    
    if op == "2":
        ini = input("Início (DD/MM/AAAA): ")
        fim = input("Fim (DD/MM/AAAA): ")
        app.run_extraction(ini, fim if fim else ini)
    else:
        hoje = datetime.now().strftime("%d/%m/%Y")
        app.run_extraction(f"01{hoje[2:]}", hoje)