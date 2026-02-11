import time
from datetime import datetime
from src.config import CONFIG
from src.infrastructure.omie_client import OmieClient
from src.infrastructure.custom_logging import logger
from src.infrastructure.repositories import JsonRepository
from src.domain.services import BillingDomainService

class BillingApplication:
    def __init__(self):
        # 1. Camada de Infraestrutura
        self.client = OmieClient()
        self.repo = JsonRepository()
        
        # 2. Carregamento de Dados Auxiliares (Cache Local)
        self.manifestados = self.repo.load_filter_set("manifestados.json")
        self.processados_antigos = self.repo.load_filter_set("processados.json")
        
        # Carrega mapas estáticos (Vendedores e Categorias)
        vendedores_data = self.repo.load_dict("vendedores.json")
        
        # MUDANÇA AQUI: Carrega o arquivo local de categorias em vez da API
        categorias_data = self.repo.load_dict("categorias.json")
        
        # 3. Camada de Domínio (Injeção de Dependência)
        self.domain = BillingDomainService(
            vendedores_map=vendedores_data,
            categorias_map=categorias_data
        )
        
        # Filtro Unificado
        self.filtro_bloqueio = self.manifestados.union(self.processados_antigos)
        logger.info(f"🛡️ Filtros carregados: {len(self.filtro_bloqueio)} IDs ignorados.")

    def run_extraction(self, data_inicio: str, data_fim: str):
        # ... (O restante do método permanece idêntico ao anterior)
        # O fluxo de extração não muda, apenas a fonte das categorias.
        start_time = time.time()
        logger.info(f"🚀 Iniciando Extração: {data_inicio} até {data_fim}")
        
        all_cleaned_orders = {}
        ids_processados_agora = []
        page, total_pages, skipped_count = 1, 1, 0
        
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
                    cabecalho = order.get("cabecalho", {})
                    cod_pedido = str(cabecalho.get("codigo_pedido", ""))
                    num_pedido = str(cabecalho.get("numero_pedido", "S_NUM"))

                    if cod_pedido in self.filtro_bloqueio:
                        skipped_count += 1
                        continue
                    
                    # O domain service agora usará o mapa do arquivo categorias.json
                    dados_limpos = self.domain.clean_order_data(order)
                    all_cleaned_orders[num_pedido] = dados_limpos
                    ids_processados_agora.append(cod_pedido)

                logger.info(f"📄 Pág {page}/{total_pages} | Novos: {len(all_cleaned_orders)}")
                page += 1
                time.sleep(0.2)

            except Exception as e:
                logger.warning(f"⚠️ Erro na página {page}: {e}. Tentando novamente...")
                time.sleep(2)
                continue

        # Salva outputs
        if all_cleaned_orders:
            self.repo.save_refined_json(all_cleaned_orders, data_inicio)
        
        if ids_processados_agora:
            self.repo.update_processed_list("processados.json", ids_processados_agora)

        duration = time.time() - start_time
        logger.info(f"🏁 Finalizado em {duration:.2f}s. Ignorados: {skipped_count}")

if __name__ == "__main__":
    app = BillingApplication()
    dt_inicio = CONFIG.DATA_INICIO or datetime.now().strftime("%d/%m/%Y")
    dt_fim = CONFIG.DATA_FIM or datetime.now().strftime("%d/%m/%Y")
    app.run_extraction(dt_inicio, dt_fim)