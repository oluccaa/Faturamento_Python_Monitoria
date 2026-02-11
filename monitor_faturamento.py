import json
import time
import sys
from pathlib import Path
from datetime import datetime
from src.config import CONFIG
from src.infrastructure.omie_client import OmieClient
from src.infrastructure.logging import logger
from src.domain.services import BillingDomainService

class BillingApplication:
    def __init__(self):
        self.client = OmieClient()
        self.domain = BillingDomainService()
        
        # Carrega filtros e define caminhos
        self.file_manifestados = Path("manifestados.json")
        self.file_processados = Path("processados.json")
        self.manifestados = self._load_id_set(self.file_manifestados)

    def _load_id_set(self, filepath: Path) -> set:
        """
        Lê um arquivo JSON e retorna um set de strings para busca rápida.
        Funciona tanto para manifestados.json quanto para processados.json.
        """
        if not filepath.exists():
            if filepath.name == "manifestados.json":
                logger.warning(f"⚠️ Arquivo {filepath} não encontrado. Nenhum filtro aplicado.")
            return set()
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                # Garante que seja um set de strings
                return set(map(str, dados))
        except Exception as e:
            logger.error(f"❌ Erro ao carregar {filepath}: {e}")
            return set()

    def _update_processados_file(self, novos_ids: list):
        """
        Lê o arquivo de processados existente, adiciona os novos IDs 
        e salva novamente mantendo a lista única e atualizada.
        """
        if not novos_ids:
            return

        ids_atuais = self._load_id_set(self.file_processados)
        total_antes = len(ids_atuais)
        
        # Adiciona os novos IDs ao conjunto existente
        ids_atuais.update(novos_ids)
        
        try:
            with open(self.file_processados, 'w', encoding='utf-8') as f:
                # Salva ordenado para facilitar leitura manual se necessário
                json.dump(sorted(list(ids_atuais)), f, indent=2)
            
            logger.info(f"✅ Histórico atualizado: {len(ids_atuais) - total_antes} novos IDs adicionados ao '{self.file_processados}'.")
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar arquivo de processados: {e}")

    def run_extraction(self, data_inicio: str, data_fim: str):
        start_time_total = time.time()
        logger.info(f"🚀 Iniciando Extração Automática: {data_inicio} até {data_fim}")
        
        all_cleaned_orders = {}
        ids_processados_nesta_rodada = [] # Lista para guardar os IDs extraídos agora
        
        page = 1
        total_pages = 1
        max_retries = 3
        skipped_count = 0 

        while page <= total_pages:
            param = {
                "pagina": page,
                "registros_por_pagina": 100,
                "filtrar_por_data_de": data_inicio,
                "filtrar_por_data_ate": data_fim,
                "apenas_resumo": "N"
            }

            success = False
            for attempt in range(1, max_retries + 1):
                try:
                    start_time_page = time.time()
                    data = self.client.post("ListarPedidos", param)
                    duration_page = time.time() - start_time_page
                    
                    if page == 1:
                        total_pages = data.get("total_de_paginas", 1)
                    
                    orders = data.get("pedido_venda_produto", [])
                    if isinstance(orders, dict): orders = [orders]

                    for order in orders:
                        cabecalho = order.get("cabecalho", {})
                        
                        # ID INTERNO (Usado para validação com manifesto e processados)
                        cod_pedido = str(cabecalho.get("codigo_pedido", ""))
                        
                        # NÚMERO VISUAL (Usado como chave no JSON final)
                        num_pedido = str(cabecalho.get("numero_pedido", "S_NUM"))
                        
                        # --- LÓGICA DE FILTRAGEM ---
                        # Se o ID interno estiver no manifesto, ignoramos
                        if cod_pedido in self.manifestados:
                            skipped_count += 1
                            continue 
                        # ---------------------------

                        # Limpa e adiciona ao resultado
                        all_cleaned_orders[num_pedido] = self.domain.clean_order_data(order)
                        
                        # Guarda o ID interno para salvar em processados.json depois
                        ids_processados_nesta_rodada.append(cod_pedido)

                    logger.info(
                        f"📄 Pág {page}/{total_pages} | "
                        f"⏱️ {duration_page:.2f}s | "
                        f"📦 Acumulado: {len(all_cleaned_orders)} (Ignorados: {skipped_count})"
                    )
                    
                    page += 1
                    success = True
                    time.sleep(0.2)
                    break 

                except Exception as e:
                    logger.warning(f"⚠️ Erro na página {page} (Tentativa {attempt}/{max_retries}): {e}")
                    time.sleep(2 ** attempt)
            
            if not success:
                logger.error(f"❌ Falha crítica na página {page}. Parando extração.")
                break

        # 1. Salva o arquivo de faturamento (JSON Limpo)
        self._save_faturamento(all_cleaned_orders, data_inicio)
        
        # 2. Atualiza o arquivo de IDs processados (Histórico)
        if ids_processados_nesta_rodada:
            self._update_processados_file(ids_processados_nesta_rodada)
        
        # Métricas Finais
        end_time_total = time.time()
        duration_total = end_time_total - start_time_total
        tempo_fmt = f"{duration_total/60:.1f} min" if duration_total > 60 else f"{duration_total:.2f}s"

        logger.info(f"🏁 Finalizado. Salvos: {len(all_cleaned_orders)} | Manifestados Ignorados: {skipped_count} | Tempo: {tempo_fmt}")

    def _save_faturamento(self, data: dict, ref: str):
        if not data: 
            logger.warning("⚠️ Nenhum dado novo encontrado para salvar no faturamento.")
            return
            
        safe_date = ref.replace('/', '_')
        filename = f"faturamento_{safe_date}.json"
        path = CONFIG.OUTPUT_DIR / filename
        
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Arquivo de faturamento salvo: {path}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar arquivo de faturamento: {e}")

if __name__ == "__main__":
    app = BillingApplication()
    
    # 1. Carrega configurações
    dt_inicio = CONFIG.DATA_INICIO
    dt_fim = CONFIG.DATA_FIM
    hoje = datetime.now().strftime("%d/%m/%Y")
    
    # 2. Lógica Inteligente de Datas
    if not dt_inicio:
        logger.warning("⚠️ DATA_INICIO não definida. Usando data de hoje.")
        dt_inicio = hoje
        
    if not dt_fim:
        logger.info(f"ℹ️ DATA_FIM não definida. Assumindo até hoje ({hoje}).")
        dt_fim = hoje

    logger.info(f"⚙️ Configuração de Período: {dt_inicio} até {dt_fim}")

    # 3. Executa
    app.run_extraction(dt_inicio, dt_fim)