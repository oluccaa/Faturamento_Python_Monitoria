import time
import sys
from datetime import datetime
from typing import Dict, List, Any

from src.config import CONFIG
from src.infrastructure.omie_client import OmieClient
from src.infrastructure.custom_logging import logger
from src.infrastructure.repositories import JsonRepository
from src.domain.services import BillingDomainService

class BillingApplication:
    def __init__(self):
        """
        Orquestrador principal da extração de faturamento.
        Inicializa infraestrutura, carrega caches e prepara o domínio.
        """
        logger.info("🔧 Inicializando Aplicação de Monitoria de Faturamento...")
        
        # 1. Camada de Infraestrutura
        self.client = OmieClient()
        self.repo = JsonRepository(CONFIG.BASE_DIR)
        
        # 2. Carregamento de Dados Auxiliares (Cache Local)
        self.manifestados = self.repo.load_filter_set("manifestados.json")
        self.processados_antigos = self.repo.load_filter_set("processados.json")
        
        # Carrega mapas para enriquecimento (Vendedores e Categorias)
        vendedores_map = self._load_and_map_vendedores()
        categorias_map = self._load_and_map_categorias()
        
        # 3. Camada de Domínio (Injeção de Dependência)
        self.domain = BillingDomainService(
            vendedores_map=vendedores_map,
            categorias_map=categorias_map
        )
        
        # Filtro Unificado (Ignorar pedidos já processados ou manifestados manualmente)
        self.filtro_bloqueio = self.manifestados.union(self.processados_antigos)
        logger.info(f"🛡️  Filtro de Bloqueio Ativo: {len(self.filtro_bloqueio)} IDs ignorados.")

    def _load_and_map_vendedores(self) -> Dict:
        """Carrega vendedores.json e garante que seja um Dicionário {ID: Dados}."""
        raw_data = self.repo.load_dict("vendedores.json")
        if isinstance(raw_data, list):
            logger.info(f"🔄 Convertendo lista de {len(raw_data)} vendedores para Mapa Hash...")
            return {str(v.get('codigo_vendedor')): v for v in raw_data if isinstance(v, dict)}
        return raw_data if isinstance(raw_data, dict) else {}

    def _load_and_map_categorias(self) -> Dict:
        """Carrega categorias.json e garante que seja um Dicionário {ID: Nome}."""
        raw_data = self.repo.load_dict("categorias.json")
        if isinstance(raw_data, list):
            logger.info(f"🔄 Convertendo lista de {len(raw_data)} categorias para Mapa Hash...")
            return {str(c.get('codigo')): c.get('descricao') for c in raw_data if isinstance(c, dict)}
        return raw_data if isinstance(raw_data, dict) else {}

    def _fetch_nfe_map(self, data_inicio: str, data_fim: str) -> Dict[str, dict]:
        """
        Busca TODAS as Notas Fiscais do período e cria um mapa { nIdPedido: DadosNF }.
        Isso permite cruzamento O(1) durante o processamento dos pedidos.
        """
        logger.info(f"🔎 Indexando Notas Fiscais (NFe) de {data_inicio} a {data_fim}...")
        nf_map = {}
        page = 1
        total_pages = 1
        
        try:
            while page <= total_pages:
                try:
                    # Busca Notas Fiscais
                    data = self.client.listar_nfs(page, data_inicio, data_fim)
                    total_pages = data.get("total_de_paginas", 1)
                    nfs = data.get("nfCadastro", [])
                    
                    for nf in nfs:
                        # Tenta encontrar o vínculo com o Pedido na lista de detalhes
                        det = nf.get("det", [])
                        n_id_pedido = ""
                        
                        # Geralmente o nIdPedido está no primeiro item da nota
                        if isinstance(det, list) and len(det) > 0:
                            n_id_pedido = str(det[0].get("nIdPedido", ""))
                        
                        # Se encontrou vínculo válido, indexa no mapa
                        if n_id_pedido and n_id_pedido != "0":
                            cleaned_nf = self.domain.clean_nf_data(nf)
                            nf_map[n_id_pedido] = cleaned_nf
                    
                    logger.debug(f"   📑 NFs Pág {page}/{total_pages} indexadas.")
                    page += 1
                    time.sleep(0.2) # Rate limit suave
                    
                except Exception as e:
                    logger.warning(f"   ⚠️ Falha ao buscar NFs Pág {page}: {e}. Tentando próxima...")
                    page += 1 # Pula página com erro para não travar tudo
                    
        except Exception as e:
            logger.error(f"❌ Erro crítico ao indexar NFs: {e}. O relatório seguirá sem dados fiscais.")
        
        logger.info(f"✅ Indexação Fiscal Concluída: {len(nf_map)} vínculos encontrados.")
        return nf_map

    def run_extraction(self, data_inicio: str, data_fim: str):
        start_time = time.time()
        logger.info(f"🚀 Iniciando Extração Completa: {data_inicio} até {data_fim}")
        
        # 1. PRÉ-CARREGAMENTO DAS NFS (NOVO)
        # Trazemos as NFs para a memória ANTES de processar os pedidos
        nf_reference_map = self._fetch_nfe_map(data_inicio, data_fim)
        
        all_cleaned_orders: Dict[str, Any] = {}
        ids_processados_agora: List[str] = []
        
        # Controle de Paginação
        page = 1
        total_pages = 1 
        skipped_count = 0
        retries = 0
        MAX_RETRIES = 3 
        
        try:
            while page <= total_pages:
                try:
                    # Chamada encapsulada e tipada
                    data = self.client.listar_pedidos(
                        pagina=page, 
                        data_de=data_inicio, 
                        data_ate=data_fim
                    )
                    
                    total_pages = data.get("total_de_paginas", 1)
                    orders = data.get("pedido_venda_produto", [])
                    if isinstance(orders, dict): 
                        orders = [orders]

                    new_items_count = 0
                    
                    for order in orders:
                        cabecalho = order.get("cabecalho", {})
                        cod_pedido = str(cabecalho.get("codigo_pedido", ""))
                        num_pedido = str(cabecalho.get("numero_pedido", "S_NUM"))

                        # 1. Filtro de Bloqueio (Já processado?)
                        if cod_pedido in self.filtro_bloqueio:
                            skipped_count += 1
                            continue
                        
                        # 2. Processamento via Domínio
                        try:
                            dados_limpos = self.domain.clean_order_data(order)
                            
                            # --- ENRIQUECIMENTO (CRUZAMENTO COM NF) ---
                            if cod_pedido in nf_reference_map:
                                dados_limpos["nota_fiscal"] = nf_reference_map[cod_pedido]
                            else:
                                dados_limpos["nota_fiscal"] = {} # Garante estrutura vazia se não tiver nota
                            
                            # Usamos o numero_pedido como chave para o JSON final
                            all_cleaned_orders[num_pedido] = dados_limpos
                            ids_processados_agora.append(cod_pedido)
                            new_items_count += 1
                            
                        except Exception as e:
                            logger.error(f"❌ Erro ao processar pedido {num_pedido}: {e}")

                    logger.info(f"📄 Pedidos Pág {page}/{total_pages} | Capturados: {new_items_count} | Ignorados: {skipped_count}")
                    
                    # Sucesso: Avança e reseta retries
                    page += 1
                    retries = 0 
                    time.sleep(0.2) 

                except Exception as e:
                    retries += 1
                    wait_time = 2 ** retries # Backoff exponencial
                    logger.warning(f"⚠️ Erro na pág {page} (Tentativa {retries}/{MAX_RETRIES}): {e}")
                    
                    if retries > MAX_RETRIES:
                        logger.critical(f"⛔ Falha persistente na página {page}. Abortando extração.")
                        break
                    
                    logger.info(f"⏳ Aguardando {wait_time}s para tentar novamente...")
                    time.sleep(wait_time)

        except KeyboardInterrupt:
            logger.warning("🛑 Execução interrompida pelo usuário! Salvando dados parciais...")
        
        except Exception as e:
            logger.critical(f"💥 Erro fatal na aplicação: {e}")

        finally:
            # BLOCO DE SEGURANÇA: Salva tudo o que conseguiu extrair
            self._save_results(all_cleaned_orders, ids_processados_agora, data_inicio)
            
            duration = time.time() - start_time
            logger.info(f"🏁 Finalizado em {duration:.2f}s. Total Extraído: {len(all_cleaned_orders)}. Ignorados (Cache): {skipped_count}")

    def _save_results(self, orders: dict, processed_ids: list, date_ref: str):
        """Persiste os dados extraídos no disco."""
        if not orders:
            logger.warning("⚠️ Nenhum pedido novo encontrado para salvar.")
            return

        logger.info("💾 Salvando dados enriquecidos no disco...")
        
        # 1. Salva o JSON com os dados detalhados (Refined)
        self.repo.save_refined_json(orders, date_ref)
        
        # 2. Atualiza a lista de IDs processados (Incremental)
        if processed_ids:
            self.repo.update_processed_list("processados.json", processed_ids)
            logger.info(f"📝 {len(processed_ids)} novos IDs adicionados ao histórico de processados.")

if __name__ == "__main__":
    # Configuração de datas com fallback seguro
    now_str = datetime.now().strftime("%d/%m/%Y")
    
    # Prioridade: .env > Data Atual
    dt_inicio = CONFIG.DATA_INICIO if CONFIG.DATA_INICIO else now_str
    dt_fim = CONFIG.DATA_FIM if CONFIG.DATA_FIM else now_str
    
    app = BillingApplication()
    app.run_extraction(dt_inicio, dt_fim)