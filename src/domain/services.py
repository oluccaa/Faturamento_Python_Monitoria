from typing import Dict, Any, Union, List
from src.domain.entities import (
    PedidoRefinado, Cabecalho, InfoCadastro, InformacoesAdicionais, 
    ListaParcelas, Parcela, Observacoes, TotalPedido
)

class BillingDomainService:
    def __init__(self, vendedores_map: Dict[str, Any] = None, categorias_map: Dict[str, str] = None):
        """
        Inicializa o serviço de domínio com mapas de cache para tradução de IDs.
        """
        self.vendedores_map = vendedores_map or {}
        self.categorias_map = categorias_map or {}

    def _get_safe_dict(self, source: Any, key: str) -> dict:
        """
        Helper para extrair dicionários da API Omie.
        Trata o caso comum onde a API retorna lista vazia [] em vez de objeto {}.
        """
        if not isinstance(source, dict):
            return {}
        value = source.get(key, {})
        return value if isinstance(value, dict) else {}

    def _resolve_vendedor_name(self, cod_vend: Any) -> str:
        """
        Resolve o nome do vendedor de forma resiliente a diferentes formatos de cache.
        """
        cod_str = str(cod_vend)
        if not cod_vend or cod_str == "0":
            return "Venda Direta"
        
        entry = self.vendedores_map.get(cod_str)
        if not entry:
            return f"Vendedor {cod_str}"
            
        if isinstance(entry, dict):
            # Tenta pegar o nome em diferentes chaves possíveis da API Omie
            return entry.get("nome") or entry.get("nome_exibicao") or f"Vendedor {cod_str}"
        
        return str(entry)

    def clean_nf_data(self, raw_nf: dict) -> dict:
        """
        Normaliza os dados da Nota Fiscal para o cruzamento de dados.
        """
        ide = self._get_safe_dict(raw_nf, "ide")
        compl = self._get_safe_dict(raw_nf, "compl")
        
        return {
            "nNF": str(ide.get("nNF", "")).strip(),
            "dEmi": str(ide.get("dEmi", "")).strip(),
            "hEmi": str(ide.get("hEmi", "")).strip(),
            "cChaveNFe": str(compl.get("cChaveNFe", "")).strip()
        }

    def clean_order_data(self, raw_order: dict) -> dict:
        """
        Converte o JSON bruto da Omie em uma entidade PedidoRefinado fortemente tipada.
        """
        
        # 1. Cabecalho
        raw_cab = self._get_safe_dict(raw_order, "cabecalho")
        cabecalho = Cabecalho(
            bloqueado=str(raw_cab.get("bloqueado", "N")),
            codigo_cenario_impostos=str(raw_cab.get("codigo_cenario_impostos", "")),
            codigo_cliente=int(raw_cab.get("codigo_cliente", 0)),
            codigo_parcela=str(raw_cab.get("codigo_parcela", "")),
            codigo_pedido=int(raw_cab.get("codigo_pedido", 0)),
            data_previsao=str(raw_cab.get("data_previsao", "")),
            etapa=str(raw_cab.get("etapa", "")),
            numero_pedido=str(raw_cab.get("numero_pedido", "")),
            origem_pedido=str(raw_cab.get("origem_pedido", "")),
            qtde_parcelas=int(raw_cab.get("qtde_parcelas", 0)),
            quantidade_itens=int(raw_cab.get("quantidade_itens", 0))
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
        cod_vend = str(raw_adic.get("codVend", ""))
        cod_cat = str(raw_adic.get("codigo_categoria", ""))
        
        adicionais = InformacoesAdicionais(
            codProj=int(raw_adic.get("codProj", 0)),
            codVend=int(cod_vend) if cod_vend.isdigit() else 0,
            vendedor_nome=self._resolve_vendedor_name(cod_vend),
            codigo_categoria=cod_cat,
            categoria_nome=self.categorias_map.get(cod_cat, f"Categoria {cod_cat}"),
            codigo_conta_corrente=int(raw_adic.get("codigo_conta_corrente", 0)),
            consumidor_final=str(raw_adic.get("consumidor_final", "N")),
            enviar_email=str(raw_adic.get("enviar_email", "N")),
            enviar_pix=str(raw_adic.get("enviar_pix", "N")),
            numero_pedido_cliente=str(raw_adic.get("numero_pedido_cliente", "")).strip(),
            utilizar_emails=str(raw_adic.get("utilizar_emails", "")).strip()
        )

        # 4. Lista de Parcelas
        raw_parcelas_container = self._get_safe_dict(raw_order, "lista_parcelas")
        raw_parcelas_list = raw_parcelas_container.get("parcela", [])
        
        # Normaliza para lista caso a API retorne um único dicionário
        if isinstance(raw_parcelas_list, dict):
            raw_parcelas_list = [raw_parcelas_list]
        elif not isinstance(raw_parcelas_list, list):
            raw_parcelas_list = []
            
        parcelas_refinadas = []
        for p in raw_parcelas_list:
            if isinstance(p, dict):
                parcelas_refinadas.append(Parcela(
                    data_vencimento=str(p.get("data_vencimento", "")),
                    numero_parcela=int(p.get("numero_parcela", 0)),
                    percentual=float(p.get("percentual", 0)),
                    quantidade_dias=int(p.get("quantidade_dias", 0)),
                    valor=p.get("valor", 0) # Entidade converte para Decimal
                ))
            
        lista_parcelas = ListaParcelas(parcela=parcelas_refinadas)

        # 5. Observacoes
        raw_obs = self._get_safe_dict(raw_order, "observacoes")
        observacoes = Observacoes(
            obs_venda=str(raw_obs.get("obs_venda", "")).strip()
        )

        # 6. Total Pedido
        raw_total = self._get_safe_dict(raw_order, "total_pedido")
        total_pedido = TotalPedido(
            valor_total_pedido=raw_total.get("valor_total_pedido", 0)
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