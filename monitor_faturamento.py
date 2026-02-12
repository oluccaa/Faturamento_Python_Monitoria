import time
import sys
from datetime import datetime
from typing import Dict, List, Any, Set, Optional

# Bloco de importação segura
try:
    from src.config import CONFIG
    from src.infrastructure.omie_client import OmieClient
    from src.infrastructure.custom_logging import logger
    from src.infrastructure.repositories import JsonRepository
    from src.domain.services import BillingDomainService
except ImportError as e:
    print(f"❌ Erro Crítico de Importação: {e}")
    sys.exit(1)

class BillingApplication:
    def __init__(self):
        """
        Orquestrador principal da extração de faturamento.
        Suporta estratégia Híbrida: Cache de NFs + Consulta Pontual para Faturamentos Cruzados.
        """
        logger.info("🔧 Inicializando Aplicação de Monitoria de Faturamento...")
        
        # 1. Infraestrutura
        self.client = OmieClient()
        self.repo = JsonRepository(CONFIG.BASE_DIR)
        
        # 2. Dados Auxiliares
        self.manifestados = self.repo.load_filter_set("manifestados.json")
        self.processados_antigos = self.repo.load_filter_set("processados.json")
        
        vendedores_map = self._load_and_map_vendedores()
        categorias_map = self._load_and_map_categorias()
        
        # 3. Domínio
        self.domain = BillingDomainService(
            vendedores_map=vendedores_map,
            categorias_map=categorias_map
        )
        
        # Filtro de Bloqueio
        self.filtro_bloqueio: Set[str] = self.manifestados.union(self.processados_antigos)
        logger.info(f"🛡️ Filtro de Bloqueio Ativo: {len(self.filtro_bloqueio)} IDs ignorados.")

    def _load_and_map_vendedores(self) -> Dict:
        raw_data = self.repo.load_dict("vendedores.json")
        if isinstance(raw_data, list):
            return {str(v.get('codigo_vendedor', '')).strip(): v for v in raw_data if isinstance(v, dict)}
        return raw_data if isinstance(raw_data, dict) else {}

    def _load_and_map_categorias(self) -> Dict:
        raw_data = self.repo.load_dict("categorias.json")
        if isinstance(raw_data, list):
            return {str(c.get('codigo', '')).strip(): c.get('descricao', 'N/D') for c in raw_data if isinstance(c, dict)}
        return raw_data if isinstance(raw_data, dict) else {}

    def _fetch_nfe_map(self, data_inicio: str, data_fim: str) -> Dict[str, dict]:
        """
        Estratégia de Cache: Busca todas as NFs do período para evitar chamadas individuais
        para 90% dos casos.
        """
        logger.info(f"🔎 Indexando Cache de NFs (NFe) de {data_inicio} a {data_fim}...")
        nf_map = {}
        page = 1
        total_pages = 1
        
        try:
            while page <= total_pages:
                data = self.client.listar_nfs(page, data_inicio, data_fim)
                total_pages = data.get("total_de_paginas", 1)
                nfs = data.get("nfCadastro", [])
                
                if not nfs: break

                for nf in nfs:
                    compl = nf.get("compl", {})
                    n_id_pedido = str(compl.get("nIdPedido", "0")).strip()
                    
                    # Tenta pegar do item se não tiver no cabeçalho
                    if (n_id_pedido == "0" or not n_id_pedido) and nf.get("det"):
                        n_id_pedido = str(nf["det"][0].get("nIdPedido", "0")).strip()
                    
                    if n_id_pedido and n_id_pedido != "0":
                        nf_map[n_id_pedido] = nf
                
                if page % 5 == 0:
                    logger.debug(f"   📑 Cache NFs: Pág {page}/{total_pages}...")
                
                page += 1
                # Pequeno sleep para não saturar a API na indexação
                time.sleep(0.05) 
                
        except Exception as e:
            logger.error(f"❌ Erro ao indexar NFs: {e}")
        
        logger.info(f"✅ Cache Fiscal Pronto: {len(nf_map)} notas vinculadas.")
        return nf_map

    def run_extraction(self, data_inicio: str, data_fim: str):
        start_time = time.time()
        logger.info(f"🚀 Iniciando Processamento Híbrido: {data_inicio} até {data_fim}")
        
        # 1. Cria o Cache de NFs (Performance O(1))
        nf_reference_map = self._fetch_nfe_map(data_inicio, data_fim)
        
        all_cleaned_orders: Dict[str, Any] = {}
        ids_processados_agora: List[str] = []
        
        page = 1
        total_pages = 1
        stats = {"capturados": 0, "ignorados": 0, "api_lookups": 0, "cache_hits": 0}
        
        try:
            while page <= total_pages:
                # Busca Pedidos
                data = self.client.listar_pedidos(pagina=page, data_de=data_inicio, data_ate=data_fim)
                total_pages = data.get("total_de_paginas", 1)
                orders = data.get("pedido_venda_produto", [])
                
                # Normaliza lista unitária
                if isinstance(orders, dict): orders = [orders]

                for order in orders:
                    cab = order.get("cabecalho", {})
                    info = order.get("infoCadastro", {})
                    cod_pedido = str(cab.get("codigo_pedido", "")).strip()
                    num_pedido = str(cab.get("numero_pedido", "S_NUM")).strip()
                    
                    # 1. Filtro de Bloqueio (Já processados)
                    if cod_pedido in self.filtro_bloqueio:
                        stats["ignorados"] += 1
                        continue

                    raw_nf = {}
                    
                    # 2. Verifica se o pedido está faturado
                    is_faturado = info.get("faturado", "N") == "S" or cab.get("etapa") in ["60", "70", "80"]
                    
                    if is_faturado:
                        # 3. ESTRATÉGIA HÍBRIDA DE BUSCA DE NF
                        
                        # Tenta Cache Primeiro (Rápido)
                        if cod_pedido in nf_reference_map:
                            raw_nf = nf_reference_map[cod_pedido]
                            stats["cache_hits"] += 1
                        else:
                            # Tenta API Individual (Lento, mas necessário para datas cruzadas)
                            # Ex: Pedido dia 30, NF dia 02 do próximo mês
                            try:
                                logger.debug(f"🔍 Buscando NF na API para Pedido {num_pedido}...")
                                raw_nf = self.client.consultar_nfe_por_pedido(int(cod_pedido))
                                if raw_nf:
                                    stats["api_lookups"] += 1
                                    # Sleep extra para evitar Rate Limit em loops de consulta
                                    time.sleep(0.2) 
                            except Exception as e:
                                logger.warning(f"⚠️ Falha ao buscar NF individual {num_pedido}: {e}")
                                raw_nf = {}

                    # 4. Processamento e Limpeza (Domínio)
                    # Se raw_nf estiver vazio, o domain preenche com vazios
                    nf_limpa = self.domain.clean_nf_data(raw_nf) if raw_nf else None
                    
                    # Se tivermos NF, geramos hash de validação
                    validation_hash = None
                    if raw_nf:
                         check = self.domain.validar_integridade(raw_nf, order)
                         if check["status"] == "OK":
                             validation_hash = check["hash_validacao"]
                         else:
                             # Se falhar na validação, loga mas processa o pedido mesmo assim (opcional)
                             logger.warning(f"⚠️ Divergência Pedido x NF ({num_pedido}): {check['erros']}")

                    dados_limpos = self.domain.clean_order_data(
                        order, 
                        nf_data=nf_limpa, 
                        validation_hash=validation_hash
                    )
                    
                    all_cleaned_orders[num_pedido] = dados_limpos
                    ids_processados_agora.append(cod_pedido)
                    stats["capturados"] += 1

                logger.info(f"📄 Pág {page}/{total_pages} | Cache: {stats['cache_hits']} | API Extra: {stats['api_lookups']} | Total: {stats['capturados']}")
                
                # Checkpoint a cada 5 páginas
                if page % 5 == 0:
                    self._save_results(all_cleaned_orders, ids_processados_agora, data_inicio, is_checkpoint=True)

                page += 1
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.warning("🛑 Interrompido pelo usuário.")
        except Exception as e:
            logger.critical(f"💥 Erro fatal: {e}", exc_info=True)
        finally:
            self._save_results(all_cleaned_orders, ids_processados_agora, data_inicio)
            duration = time.time() - start_time
            logger.info(f"🏁 Fim ({duration:.2f}s). Cache Hits: {stats['cache_hits']} | API Lookups: {stats['api_lookups']}")

    def _save_results(self, orders: dict, processed_ids: list, date_ref: str, is_checkpoint: bool = False):
        if not orders: return
        label = "CHECKPOINT" if is_checkpoint else "FINAL"
        try:
            self.repo.save_refined_json(orders, date_ref)
            if processed_ids and not is_checkpoint:
                # Só atualiza a lista de processados no final para evitar pular itens em caso de rerun parcial
                self.repo.update_processed_list("processados.json", processed_ids)
            if not is_checkpoint:
                logger.info(f"💾 Dados salvos com sucesso ({len(orders)} registros).")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar {label}: {e}")

if __name__ == "__main__":
    now_str = datetime.now().strftime("%d/%m/%Y")
    
    # Suporte a argumentos CLI
    dt_inicio = sys.argv[1] if len(sys.argv) > 1 else (CONFIG.DATA_INICIO or now_str)
    dt_fim = sys.argv[2] if len(sys.argv) > 2 else (CONFIG.DATA_FIM or now_str)
        
    app = BillingApplication()
    app.run_extraction(dt_inicio, dt_fim)