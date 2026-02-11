from typing import Dict, Any, Union
from src.domain.entities import (
    PedidoRefinado, Cabecalho, InfoCadastro, InformacoesAdicionais, 
    ListaParcelas, Parcela, Observacoes, TotalPedido
)

class BillingDomainService:
    def __init__(self, vendedores_map: Dict[str, Any] = None, categorias_map: Dict[str, str] = None):
        """
        Inicializa o serviço com os mapas para tradução (ID -> Nome).
        """
        self.vendedores_map = vendedores_map or {}
        self.categorias_map = categorias_map or {}

    def _get_safe_dict(self, source: dict, key: str) -> dict:
        """
        Helper para extrair dicionários da API Omie de forma segura.
        Se a API retornar uma lista vazia [] (comum em campos vazios), 
        retorna um dicionário vazio {} para evitar erro de .get().
        """
        value = source.get(key, {})
        if isinstance(value, list):
            return {}
        return value

    def clean_order_data(self, raw_order: dict) -> dict:
        """
        Transforma o JSON bruto da Omie no formato refinado específico solicitado.
        """
        
        # 1. Cabecalho (Usa o helper seguro)
        raw_cab = self._get_safe_dict(raw_order, "cabecalho")
        cabecalho = Cabecalho(
            bloqueado=str(raw_cab.get("bloqueado", "N")),
            codigo_cenario_impostos=str(raw_cab.get("codigo_cenario_impostos", "")),
            codigo_cliente=raw_cab.get("codigo_cliente"),
            codigo_parcela=str(raw_cab.get("codigo_parcela", "")),
            codigo_pedido=raw_cab.get("codigo_pedido"),
            data_previsao=str(raw_cab.get("data_previsao", "")),
            etapa=str(raw_cab.get("etapa", "")),
            numero_pedido=str(raw_cab.get("numero_pedido", "")),
            origem_pedido=str(raw_cab.get("origem_pedido", "")),
            qtde_parcelas=raw_cab.get("qtde_parcelas", 0),
            quantidade_itens=raw_cab.get("quantidade_itens", 0)
        )

        # 2. InfoCadastro
        raw_info = self._get_safe_dict(raw_order, "infoCadastro")
        info = InfoCadastro(
            autorizado=str(raw_info.get("autorizado", "N")),
            cImpAPI=str(raw_info.get("cImpAPI", "N")),
            cancelado=str(raw_info.get("cancelado", "N")),
            dAlt=str(raw_info.get("dAlt", "")),
            dFat=str(raw_info.get("dFat", "")),
            dInc=str(raw_info.get("dInc", "")),
            denegado=str(raw_info.get("denegado", "N")),
            devolvido=str(raw_info.get("devolvido", "N")),
            devolvido_parcial=str(raw_info.get("devolvido_parcial", "N")),
            faturado=str(raw_info.get("faturado", "N")),
            hAlt=str(raw_info.get("hAlt", "")),
            hFat=str(raw_info.get("hFat", "")),
            hInc=str(raw_info.get("hInc", "")),
            uAlt=str(raw_info.get("uAlt", "")),
            uFat=str(raw_info.get("uFat", "")),
            uInc=str(raw_info.get("uInc", ""))
        )

        # 3. InformacoesAdicionais
        raw_adic = self._get_safe_dict(raw_order, "informacoes_adicionais")
        
        # Tradução de Vendedor
        cod_vend = str(raw_adic.get("codVend", ""))
        nome_vendedor = self.vendedores_map.get(cod_vend, {}).get("nome", f"Vendedor {cod_vend}")
        if cod_vend == "0" or not cod_vend:
            nome_vendedor = "Venda Direta"

        # Tradução de Categoria
        cod_cat = str(raw_adic.get("codigo_categoria", ""))
        nome_categoria = self.categorias_map.get(cod_cat, f"Categoria {cod_cat}")

        adicionais = InformacoesAdicionais(
            codProj=raw_adic.get("codProj"),
            codVend=raw_adic.get("codVend"),
            vendedor_nome=nome_vendedor,
            codigo_categoria=cod_cat,
            categoria_nome=nome_categoria,
            codigo_conta_corrente=raw_adic.get("codigo_conta_corrente"),
            consumidor_final=str(raw_adic.get("consumidor_final", "N")),
            enviar_email=str(raw_adic.get("enviar_email", "N")),
            enviar_pix=str(raw_adic.get("enviar_pix", "N")),
            numero_pedido_cliente=str(raw_adic.get("numero_pedido_cliente", "")),
            utilizar_emails=str(raw_adic.get("utilizar_emails", ""))
        )

        # 4. Lista de Parcelas (O ponto mais crítico do erro)
        raw_parcelas_container = self._get_safe_dict(raw_order, "lista_parcelas")
        raw_parcelas_list = raw_parcelas_container.get("parcela", [])
        
        if isinstance(raw_parcelas_list, dict):
            raw_parcelas_list = [raw_parcelas_list]
            
        parcelas_refinadas = []
        for p in raw_parcelas_list:
            if isinstance(p, dict): # Proteção extra dentro da lista
                parcelas_refinadas.append(Parcela(
                    data_vencimento=str(p.get("data_vencimento", "")),
                    numero_parcela=p.get("numero_parcela"),
                    percentual=p.get("percentual", 0),
                    quantidade_dias=p.get("quantidade_dias", 0),
                    valor=p.get("valor", 0.0)
                ))
            
        lista_parcelas = ListaParcelas(parcela=parcelas_refinadas)

        # 5. Observacoes
        raw_obs = self._get_safe_dict(raw_order, "observacoes")
        observacoes = Observacoes(
            obs_venda=str(raw_obs.get("obs_venda", ""))
        )

        # 6. Total Pedido
        raw_total = self._get_safe_dict(raw_order, "total_pedido")
        total_pedido = TotalPedido(
            valor_total_pedido=raw_total.get("valor_total_pedido", 0.0)
        )

        # Montagem Final
        pedido = PedidoRefinado(
            cabecalho=cabecalho,
            infoCadastro=info,
            informacoes_adicionais=adicionais,
            lista_parcelas=lista_parcelas,
            observacoes=observacoes,
            total_pedido=total_pedido
        )

        return pedido.to_dict()