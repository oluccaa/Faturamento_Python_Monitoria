import json
import os
import requests
from typing import Dict, Any
from src.config import CONFIG
from src.infrastructure.logging import logger

class DataExtractorOmie:
    """
    Serviço especializado em extração seletiva (Slicing).
    Reduz 20k linhas para apenas os campos essenciais de faturamento.
    """
    def __init__(self):
        self.endpoint = "https://app.omie.com.br/api/v1/produtos/pedido/"
        self.output_dir = "data/processed_billing"
        os.makedirs(self.output_dir, exist_ok=True)

    def _mapear_campos_obrigatorios(self, p: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reconstrói o JSON mantendo APENAS a estrutura solicitada.
        Nenhum campo fora desta lista será mantido.
        """
        return {
            "cabecalho": {
                "bloqueado": p.get("cabecalho", {}).get("bloqueado"),
                "codigo_cenario_impostos": p.get("cabecalho", {}).get("codigo_cenario_impostos"),
                "codigo_cliente": p.get("cabecalho", {}).get("codigo_cliente"),
                "codigo_parcela": p.get("cabecalho", {}).get("codigo_parcela"),
                "codigo_pedido": p.get("cabecalho", {}).get("codigo_pedido"),
                "data_previsao": p.get("cabecalho", {}).get("data_previsao"),
                "etapa": p.get("cabecalho", {}).get("etapa"),
                "numero_pedido": p.get("cabecalho", {}).get("numero_pedido"),
                "origem_pedido": p.get("cabecalho", {}).get("origem_pedido"),
                "qtde_parcelas": p.get("cabecalho", {}).get("qtde_parcelas"),
                "quantidade_itens": p.get("cabecalho", {}).get("quantidade_itens")
            },
            "infoCadastro": {
                "autorizado": p.get("infoCadastro", {}).get("autorizado"),
                "cImpAPI": p.get("infoCadastro", {}).get("cImpAPI"),
                "cancelado": p.get("infoCadastro", {}).get("cancelado"),
                "dAlt": p.get("infoCadastro", {}).get("dAlt"),
                "dFat": p.get("infoCadastro", {}).get("dFat"),
                "dInc": p.get("infoCadastro", {}).get("dInc"),
                "denegado": p.get("infoCadastro", {}).get("denegado"),
                "devolvido": p.get("infoCadastro", {}).get("devolvido"),
                "devolvido_parcial": p.get("infoCadastro", {}).get("devolvido_parcial"),
                "faturado": p.get("infoCadastro", {}).get("faturado"),
                "hAlt": p.get("infoCadastro", {}).get("hAlt"),
                "hFat": p.get("infoCadastro", {}).get("hFat"),
                "hInc": p.get("infoCadastro", {}).get("hInc"),
                "uAlt": p.get("infoCadastro", {}).get("uAlt"),
                "uFat": p.get("infoCadastro", {}).get("uFat"),
                "uInc": p.get("infoCadastro", {}).get("uInc")
            },
            "informacoes_adicionais": {
                "codProj": p.get("informacoes_adicionais", {}).get("codProj"),
                "codVend": p.get("informacoes_adicionais", {}).get("codVend"),
                "codigo_categoria": p.get("informacoes_adicionais", {}).get("codigo_categoria"),
                "codigo_conta_corrente": p.get("informacoes_adicionais", {}).get("codigo_conta_corrente"),
                "consumidor_final": p.get("informacoes_adicionais", {}).get("consumidor_final"),
                "enviar_email": p.get("informacoes_adicionais", {}).get("enviar_email"),
                "enviar_pix": p.get("informacoes_adicionais", {}).get("enviar_pix"),
                "numero_pedido_cliente": p.get("informacoes_adicionais", {}).get("numero_pedido_cliente"),
                "utilizar_emails": p.get("informacoes_adicionais", {}).get("utilizar_emails")
            },
            "lista_parcelas": p.get("lista_parcelas", {}),
            "observacoes": {
                "obs_venda": p.get("observacoes", {}).get("obs_venda")
            },
            "total_pedido": {
                "valor_total_pedido": p.get("total_pedido", {}).get("valor_total_pedido")
            }
        }

    def processar_faturamento(self, mes: int, ano: int):
        """Busca dados e gera o JSON reestruturado e limpo."""
        logger.info(f"Iniciando extração rigorosa: {mes:02d}/{ano}")
        
        # Filtro de data conforme sua necessidade
        data_referencia = f"01/{mes:02d}/{ano}"
        
        payload = {
            "call": "ListarPedidos",
            "app_key": CONFIG.OMIE_APP_KEY,
            "app_secret": CONFIG.OMIE_APP_SECRET,
            "param": [{
                "pagina": 1,
                "registros_por_pagina": 100,
                "filtrar_por_data_de": data_referencia,
                "apenas_resumo": "N"
            }]
        }

        try:
            response = requests.post(self.endpoint, json=payload, timeout=40)
            response.raise_for_status()
            data_bruta = response.json()
            
            pedidos_originais = data_bruta.get("pedido_venda_produto", [])
            if isinstance(pedidos_originais, dict):
                pedidos_originais = [pedidos_originais]

            # Reestruturação total
            pedidos_reestruturados = {}
            for pedido in pedidos_originais:
                numero_pv = str(pedido.get("cabecalho", {}).get("numero_pedido", "SEM_NUMERO"))
                # Aqui acontece a mágica: o mapeamento ignorando o que não foi pedido
                pedidos_reestruturados[numero_pv] = self._mapear_campos_obrigatorios(pedido)

            # Salva o arquivo com o novo faturamento limpo
            save_path = os.path.join(self.output_dir, f"faturamento_limpo_{ano}_{mes:02d}.json")
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(pedidos_reestruturados, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Sucesso: {len(pedidos_reestruturados)} pedidos limpos e salvos.")
            return pedidos_reestruturados

        except Exception as e:
            logger.error(f"Erro crítico na extração: {e}")
            return {}

# Instanciação para uso no sistema
EXTRACTOR = DataExtractorOmie()