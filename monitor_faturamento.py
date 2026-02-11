import time
from datetime import datetime
from src.config import CONFIG
from src.infrastructure.omie_client import OmieClient
from src.infrastructure.repositories import JsonRepository
from src.domain.services import BillingDomainService
from src.infrastructure.logging import logger

class BillingApplication:
    def __init__(self):
        self.client = OmieClient()
        self.repo = JsonRepository()
        self.service = BillingDomainService()
        
        # Carrega filtros na memória
        self.manifestados = self.repo.load_filter_set("manifestados.json")
        self.processados = self.repo.load_filter_set("processados.json")
        
        # Filtro unificado (Blocklist)
        self.filtro_ids = self.manifestados.union(self.processados)
        logger.info(f"🛡️ Filtros carregados: {len(self.filtro_ids)} IDs ignorados.")

    def run(self):
        data_inicio = CONFIG.DATA_INICIO or datetime.now().strftime("%d/%m/%Y")
        data_fim = CONFIG.DATA_FIM or datetime.now().strftime("%d/%m/%Y")
        
        logger.info(f"🚀 Iniciando processamento: {data_inicio} até {data_fim}")

        refined_orders = {}
        processed_ids_buffer = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            try:
                # 1. Conexão e Download (JSON Bruto em Memória)
                raw_response = self.client.listar_pedidos(page, data_inicio, data_fim)
                
                if page == 1:
                    total_pages = raw_response.get("total_de_paginas", 1)

                # Normaliza lista de pedidos
                orders_list = raw_response.get("pedido_venda_produto", [])
                if isinstance(orders_list, dict): orders_list = [orders_list]

                for raw_order in orders_list:
                    # 2. Extração de Identificadores
                    cabecalho = raw_order.get("cabecalho", {})
                    
                    # ID INTERNO (Ex: 10120853337) -> Usado para COMPARAÇÃO/FILTRO
                    codigo_pedido = str(cabecalho.get("codigo_pedido"))
                    
                    # ID VISUAL (Ex: 13090) -> Usado para CHAVE do JSON
                    numero_pedido = str(cabecalho.get("numero_pedido"))

                    # 3. Comparação com Manifestados e Processados
                    if codigo_pedido in self.filtro_ids:
                        continue # Pula se já existir

                    # 4. Geração do JSON Refinado
                    refined_data = self.service.clean_order_data(raw_order)
                    
                    # Armazena no dicionário final usando numero_pedido
                    refined_orders[numero_pedido] = refined_data
                    
                    # Guarda ID interno para atualizar histórico
                    processed_ids_buffer.append(codigo_pedido)

                logger.info(f"📄 Página {page}/{total_pages} processada.")
                page += 1
                time.sleep(0.2) # Evita bloqueio da API

            except Exception as e:
                logger.error(f"⚠️ Falha crítica na página {page}: {e}")
                break

        # 5. Salva Resultados
        if refined_orders:
            self.repo.save_refined_json(refined_orders, data_inicio)
            self.repo.update_processed_list("processados.json", processed_ids_buffer)
            logger.info(f"🏁 Processo finalizado. {len(refined_orders)} novos pedidos refinados.")
        else:
            logger.warning("⚠️ Nenhum novo pedido encontrado para processar.")

if __name__ == "__main__":
    app = BillingApplication()
    app.run()