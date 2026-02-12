import unicodedata
import hashlib
import json
from typing import Dict, Any, Union, List, Optional
from decimal import Decimal
from src.domain.entities import (
    PedidoRefinado, Cabecalho, InfoCadastro, InformacoesAdicionais, 
    ListaParcelas, Parcela, Observacoes, TotalPedido, NotaFiscalRefinada,
    ProdutoItem, _to_decimal
)
from src.infrastructure.custom_logging import logger

class BillingDomainService:
    def __init__(self, vendedores_map: Dict[str, Any] = None, categorias_map: Dict[str, str] = None):
        """
        Inicializa o serviço de domínio com mapas de cache para tradução de IDs.
        """
        self.vendedores_map = vendedores_map or {}
        self.categorias_map = categorias_map or {}

    # --- MÉTODOS DE AUXÍLIO ---

    def _get_safe_dict(self, source: Any, key: str) -> dict:
        if not isinstance(source, dict):
            return {}
        value = source.get(key, {})
        return value if isinstance(value, dict) else {}

    def _ensure_list(self, data: Any) -> List[Any]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and data:
            return [data]
        return []

    def normalizar_texto(self, txt: str) -> str:
        if not txt: return ""
        return unicodedata.normalize('NFKD', str(txt)).encode('ASCII', 'ignore').decode('ASCII').upper()

    # --- LIMPEZA DE DADOS (TRANSFORMATION) ---

    def _clean_items(self, items_raw: list) -> List[Dict]:
        """Extrai e limpa os itens do pedido/NF de forma padronizada."""
        cleaned = []
        for item in items_raw:
            prod = item.get("produto", {})
            cleaned.append({
                "codigo": str(prod.get("codigo", "")),
                "descricao": str(prod.get("descricao", "")),
                "ncm": str(prod.get("ncm", "")),
                "cfop": str(prod.get("cfop", "")),
                "unidade": str(prod.get("unidade", "")),
                "quantidade": float(_to_decimal(prod.get("quantidade", 0))),
                "valor_unitario": float(_to_decimal(prod.get("valor_unitario", 0))),
                "valor_total": float(_to_decimal(prod.get("valor_total", 0)))
            })
        return cleaned

    def clean_nf_data(self, nf_raw: dict) -> Dict:
        """
        Extrai apenas os dados essenciais da Nota Fiscal a partir do JSON bruto da Omie (ListarNF).
        """
        if not nf_raw:
            return {}

        cabecalho = nf_raw.get("cabecalho", {})
        info = nf_raw.get("info", {})
        total = nf_raw.get("total", {}).get("ICMSTot", {})

        return {
            "nNF": str(cabecalho.get("nNF", "")),
            "serie": str(cabecalho.get("cSerie", "")),
            "dEmi": str(cabecalho.get("dEmi", "")),
            "hEmi": str(cabecalho.get("hEmi", "")),
            # Tenta pegar chave de acesso de vários lugares possíveis
            "cChaveNFe": str(cabecalho.get("cChaveNFe", "") or info.get("chave_nfe", "")),
            "valor_total_nf": float(_to_decimal(total.get("vNF", 0)))
        }

    def clean_order_data(self, raw_order: Dict[str, Any], nf_data: Optional[Dict] = None, validation_hash: Optional[str] = None) -> PedidoRefinado:
        """
        Refina os dados brutos de um pedido, opcionalmente enriquecendo com dados da NF.
        """
        # 1. Extração de Blocos
        cab_raw = self._get_safe_dict(raw_order, "cabecalho")
        info_raw = self._get_safe_dict(raw_order, "infoCadastro")
        adic_raw = self._get_safe_dict(raw_order, "informacoes_adicionais")
        total_raw = self._get_safe_dict(raw_order, "total_pedido")

        # 2. Cabeçalho
        cabecalho = Cabecalho(
            codigo_pedido=int(cab_raw.get("codigo_pedido", 0)),
            numero_pedido=str(cab_raw.get("numero_pedido", "")),
            codigo_cliente=int(cab_raw.get("codigo_cliente", 0)),
            data_previsao=str(cab_raw.get("data_previsao", "")),
            etapa=str(cab_raw.get("etapa", "")),
            qtde_parcelas=int(cab_raw.get("qtde_parcelas", 0)),
            quantidade_itens=int(cab_raw.get("quantidade_itens", 0))
        )

        # 3. Informações Adicionais (Enriquecimento)
        cod_vend = str(adic_raw.get("codVend", "")).strip()
        nome_vendedor = self.vendedores_map.get(cod_vend, {}).get("nome", adic_raw.get("vendedor_nome", ""))
        
        cod_cat = str(adic_raw.get("codigo_categoria", "")).strip()
        nome_cat = self.categorias_map.get(cod_cat, adic_raw.get("categoria_nome", ""))

        info_adicionais = InformacoesAdicionais(
            codProj=int(adic_raw.get("codProj", 0)),
            codVend=int(cod_vend) if cod_vend.isdigit() else 0,
            vendedor_nome=str(nome_vendedor),
            codigo_categoria=cod_cat,
            categoria_nome=str(nome_cat),
            consumidor_final=str(adic_raw.get("consumidor_final", "")),
            numero_pedido_cliente=str(adic_raw.get("numero_pedido_cliente", "")),
            utilizar_emails=str(adic_raw.get("utilizar_emails", ""))
        )

        # 4. Itens (Simplificado)
        detalhes_raw = self._ensure_list(raw_order.get("det", []))
        itens_limpos = self._clean_items(detalhes_raw)

        # 5. Parcelas
        lista_parcelas_raw = self._get_safe_dict(raw_order, "lista_parcelas")
        raw_parcelas_list = self._ensure_list(lista_parcelas_raw.get("parcela", []))
        
        parcelas_refinadas = [
            Parcela(
                data_vencimento=str(p.get("data_vencimento", "")),
                numero_parcela=int(p.get("numero_parcela", 0)),
                percentual=float(p.get("percentual", 0)),
                valor=float(p.get("valor", 0))
            ) for p in raw_parcelas_list if isinstance(p, dict)
        ]

        # 6. Totais e Obs
        total_pedido = TotalPedido(
            valor_total_pedido=float(total_raw.get("valor_total_pedido", 0))
        )
        observacoes = Observacoes(
            obs_venda=str(self._get_safe_dict(raw_order, "observacoes").get("obs_venda", "")).strip()
        )

        # 7. Nota Fiscal (Merge)
        nota_fiscal_obj = NotaFiscalRefinada()
        status = "PENDENTE"

        if nf_data:
            nota_fiscal_obj = NotaFiscalRefinada(
                nNF=str(nf_data.get("nNF", "")),
                serie=str(nf_data.get("serie", "")),
                dEmi=str(nf_data.get("dEmi", "")),
                hEmi=str(nf_data.get("hEmi", "")),
                cChaveNFe=str(nf_data.get("cChaveNFe", "")),
                valor_total_nf=float(nf_data.get("valor_total_nf", 0))
            )
            status = "FATURADO_COMPLETO"
        elif info_raw.get("faturado") == "S":
            status = "FATURADO_SEM_NF_LOCALIZADA"

        # 8. Montagem Final
        pedido = PedidoRefinado(
            numero_pedido=cabecalho.numero_pedido,
            codigo_pedido=cabecalho.codigo_pedido,
            cabecalho=cabecalho,
            infoCadastro=InfoCadastro(**{k: str(v) for k, v in info_raw.items() if k in InfoCadastro.__annotations__}),
            informacoes_adicionais=info_adicionais,
            det=itens_limpos,
            lista_parcelas=ListaParcelas(parcela=parcelas_refinadas),
            observacoes=observacoes,
            total_pedido=total_pedido,
            nota_fiscal=nota_fiscal_obj,
            status_processo=status,
            hash_integridade=validation_hash
        )

        return pedido

    # --- VALIDAÇÃO E INTEGRIDADE ---

    def validar_integridade(self, nf_raw: Dict, order_raw: Dict) -> Dict[str, Any]:
        """
        Compara o Pedido com a NF para garantir que os valores batem.
        Gera um hash único para auditoria.
        """
        erros = []
        
        # 1. Comparar Valor Total
        vlr_pedido = _to_decimal(order_raw.get("total_pedido", {}).get("valor_total_pedido", 0))
        vlr_nf = _to_decimal(nf_raw.get("total", {}).get("ICMSTot", {}).get("vNF", 0))
        
        # Tolerância de 1 centavo para arredondamentos
        if abs(vlr_pedido - vlr_nf) > Decimal("0.05"):
            erros.append(f"Divergência de Valor: Pedido {vlr_pedido} != NF {vlr_nf}")

        # 2. Gerar Hash de Integridade
        # Combina ID Pedido + Numero NF + Valor para criar impressão digital única
        raw_string = f"{order_raw.get('cabecalho', {}).get('codigo_pedido')}-{nf_raw.get('cabecalho', {}).get('nNF')}-{vlr_nf}"
        hash_md5 = hashlib.md5(raw_string.encode()).hexdigest()

        return {
            "status": "ERRO" if erros else "OK",
            "erros": erros,
            "hash_validacao": hash_md5
        }

    def merge_order_and_invoice(self, order_clean: Dict, nf_clean: Optional[Dict]) -> Dict:
        """
        Função de Helper para unir dicionários caso o processamento seja feito em etapas separadas.
        """
        if nf_clean:
            order_clean["nota_fiscal"] = nf_clean
            order_clean["status_processo"] = "FATURADO_COMPLETO"
        else:
            # Mantém status original definido no clean_order_data
            pass
            
        return order_clean