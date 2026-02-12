import time
import sys
from datetime import datetime
from typing import Dict, List, Any, Set, Optional

# Bloco de importação segura para evitar falhas se rodar fora do pacote
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
        Atualizado para suportar validação de integridade Pedido x NF.
        """
        logger.info("🔧 Inicializando Aplicação de Monitoria de Faturamento...")
        
        # 1. Camada de Infraestrutura e Persistência
        self.client = OmieClient()
        self.repo = JsonRepository(CONFIG.BASE_DIR)
        
        # 2. Carregamento de Dados Auxiliares (Cache Local O(1))
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
        Busca TODAS as Notas Fiscais e mantém o RAW JSON (Bruto) para validação.
        IMPORTANTE: Não limpamos os dados aqui para não perder os itens ('det').
        """
        logger.info(f"🔎 Indexando Notas Fiscais (NFe) de {data_inicio} a {data_fim}...")
        nf_map = {}
        page = 1
        total_pages = 1
        
        try:
            while page <= total_pages:
                try:
                    data = self.client.listar_nfs(page, data_inicio, data_fim)
                    total_pages = data.get("total_de_paginas", 1)
                    nfs = data.get("nfCadastro", [])
                    
                    if not nfs: break

                    for nf in nfs:
                        compl = nf.get("compl", {})
                        n_id_pedido = str(compl.get("nIdPedido", "0")).strip()
                        
                        # Fallback para itens se nIdPedido estiver zerado no compl
                        det = nf.get("det", [])
                        if (n_id_pedido == "0" or not n_id_pedido) and det:
                            n_id_pedido = str(det[0].get("nIdPedido", "0")).strip()
                        
                        if n_id_pedido and n_id_pedido != "0":
                            # GUARDAMOS A NF BRUTA para que o DomainService possa validar os itens depois
                            nf_map[n_id_pedido] = nf
                    
                    # Log de progresso discreto a cada 5 páginas para não poluir
                    if page % 5 == 0:
                        logger.debug(f"   📑 NFs Pág {page}/{total_pages} indexadas...")

                    page += 1
                    time.sleep(0.1)
                except Exception as e:
                    logger.warning(f"⚠️ Falha na pág {page} de NFs: {e}")
                    page += 1 
                    
        except Exception as e:
            logger.error(f"❌ Erro crítico ao indexar NFs: {e}")
        
        logger.info(f"✅ Indexação Fiscal Concluída: {len(nf_map)} vínculos encontrados.")
        return nf_map

    def run_extraction(self, data_inicio: str, data_fim: str):
        start_time = time.time()
        logger.info(f"🚀 Iniciando Extração com Validação Cross-Check: {data_inicio} até {data_fim}")
        
        # 1. Carrega NFs BRUTAS para o mapa de memória
        nf_reference_map = self._fetch_nfe_map(data_inicio, data_fim)
        
        all_cleaned_orders: Dict[str, Any] = {}
        ids_processados_agora: List[str] = []
        page, total_pages, skipped_count, erro_validacao_count = 1, 1, 0, 0
        
        try:
            while page <= total_pages:
                try:
                    # Busca Pedidos (apenas_resumo='N' deve estar garantido no client ou configurado aqui)
                    data = self.client.listar_pedidos(pagina=page, data_de=data_inicio, data_ate=data_fim)
                    total_pages = data.get("total_de_paginas", 1)
                    orders = data.get("pedido_venda_produto", [])
                    if isinstance(orders, dict): orders = [orders]

                    new_items_count = 0

                    for order in orders:
                        # Inicializa variáveis críticas antes de qualquer lógica para evitar UnboundLocalError
                        num_pedido = "DESCONHECIDO"
                        cod_pedido = "0"
                        
                        try:
                            cab = order.get("cabecalho", {})
                            cod_pedido = str(cab.get("codigo_pedido", "")).strip()
                            num_pedido = str(cab.get("numero_pedido", "S_NUM")).strip()

                            if cod_pedido in self.filtro_bloqueio:
                                skipped_count += 1
                                continue
                            
                            # Tenta validar com NF
                            raw_nf = nf_reference_map.get(cod_pedido)
                            
                            if raw_nf:
                                # --- NOVO FLUXO DE VALIDAÇÃO ---
                                # 1. Valida integridade (Itens, Valores, Quantidades)
                                check = self.domain.validar_integridade(raw_nf, order)
                                
                                if check["status"] == "OK":
                                    # 2. Limpa os dados da NF para o sumário
                                    nf_limpa = self.domain.clean_nf_data(raw_nf)
                                    
                                    # 3. Gera o Pedido Refinado com o Hash de Integridade
                                    dados_limpos = self.domain.clean_order_data(
                                        order, 
                                        nf_data=nf_limpa, 
                                        validation_hash=check["hash_validacao"]
                                    )
                                    
                                    all_cleaned_orders[num_pedido] = dados_limpos
                                    ids_processados_agora.append(cod_pedido)
                                    new_items_count += 1
                                else:
                                    erro_validacao_count += 1
                                    logger.warning(f"⚖️ Pedido {num_pedido} REPROVADO na validação: {check['erros']}")
                            else:
                                # Se não tem NF, salva apenas o pedido
                                dados_limpos = self.domain.clean_order_data(order)
                                all_cleaned_orders[num_pedido] = dados_limpos
                                ids_processados_agora.append(cod_pedido)
                                new_items_count += 1

                        except Exception as e:
                            logger.error(f"❌ Erro ao processar pedido {num_pedido}: {e}")

                    logger.info(f"📄 Pág {page}/{total_pages} | Capturados: {new_items_count} | Ignorados: {skipped_count} | Divergentes: {erro_validacao_count}")
                    
                    if page % 5 == 0:
                        self._save_results(all_cleaned_orders, ids_processados_agora, data_inicio, is_checkpoint=True)

                    page += 1
                    time.sleep(0.2)
                except Exception as e:
                    logger.error(f"⚠️ Erro na pág {page}: {e}")
                    page += 1

        except KeyboardInterrupt:
            logger.warning("🛑 Execução interrompida pelo usuário! Salvando dados parciais...")
        except Exception as e:
            logger.critical(f"💥 Erro fatal no loop principal: {e}")
        finally:
            self._save_results(all_cleaned_orders, ids_processados_agora, data_inicio)
            duration = time.time() - start_time
            logger.info(f"🏁 Finalizado em {duration:.2f}s. Sucesso: {len(all_cleaned_orders)}. Divergentes: {erro_validacao_count}")

    def _save_results(self, orders: dict, processed_ids: list, date_ref: str, is_checkpoint: bool = False):
        if not orders: return
        
        label = "CHECKPOINT" if is_checkpoint else "FINAL"
        if not is_checkpoint:
            logger.info(f"💾 Salvando dados ({label}) no disco...")

        try:
            self.repo.save_refined_json(orders, date_ref)
            if processed_ids:
                self.repo.update_processed_list("processados.json", processed_ids)
        except Exception as e:
            logger.error(f"❌ Erro ao salvar ({label}): {e}")

if __name__ == "__main__":
    now_str = datetime.now().strftime("%d/%m/%Y")
    
    # Tratamento simples de argumentos de linha de comando (opcional)
    # Ex: python monitor_faturamento.py 01/01/2024 31/01/2024
    if len(sys.argv) >= 3:
        dt_inicio = sys.argv[1]
        dt_fim = sys.argv[2]
    else:
        dt_inicio = CONFIG.DATA_INICIO or now_str
        dt_fim = CONFIG.DATA_FIM or now_str
        
    app = BillingApplication()
    app.run_extraction(dt_inicio, dt_fim)