import unicodedata
import hashlib
from typing import Dict, Any, Union, List, Optional
from src.domain.entities import (
    PedidoRefinado, Cabecalho, InfoCadastro, InformacoesAdicionais, 
    ListaParcelas, Parcela, Observacoes, TotalPedido, NotaFiscalRefinada,
    ItemPedido, ProdutoItem
)

class BillingDomainService:
    def __init__(self, vendedores_map: Dict[str, Any] = None, categorias_map: Dict[str, str] = None):
        """
        Inicializa o serviço de domínio com mapas de cache para tradução de IDs.
        """
        self.vendedores_map = vendedores_map or {}
        self.categorias_map = categorias_map or {}

    # --- MÉTODOS DE AUXÍLIO E NORMALIZAÇÃO ---

    def _get_safe_dict(self, source: Any, key: str) -> dict:
        if not isinstance(source, dict):
            return {}
        value = source.get(key, {})
        return value if isinstance(value, dict) else {}

    def _ensure_list(self, data: Any) -> List[Any]:
        """Garante que o retorno seja sempre uma lista, mesmo se a API devolver um ditado único."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and data:
            return [data]
        return []

    def normalizar_texto(self, txt: str) -> str:
        if not txt:
            return ""
        txt = unicodedata.normalize("NFKD", str(txt))
        txt = "".join(c for c in txt if not unicodedata.combining(c))
        return " ".join(txt.upper().split())

    def _resolve_vendedor_name(self, cod_vend: Any) -> str:
        cod_str = str(cod_vend)
        if not cod_vend or cod_str == "0":
            return "Venda Direta"
        
        entry = self.vendedores_map.get(cod_str)
        if not entry:
            return f"Vendedor {cod_str}"
            
        if isinstance(entry, dict):
            return entry.get("nome") or entry.get("nome_exibicao") or f"Vendedor {cod_str}"
        
        return str(entry)

    # --- LÓGICA DE VALIDAÇÃO E SEGURANÇA ---

    def gerar_hash_item(self, nf: dict, pedido: dict, nf_item: dict, ped_item: dict) -> str:
        """
        Gera um identificador único para o par Item-NF-Pedido.
        """
        base = (
            f"{nf.get('compl', {}).get('nIdPedido', '0')}"
            f"{nf.get('nfDestInt', {}).get('nCodCli', '0')}"
            f"{nf.get('nfEmitInt', {}).get('nCodEmp', '0')}"
            f"{nf_item.get('nfProdInt', {}).get('nCodItem', '')}"
            f"{nf_item.get('nfProdInt', {}).get('nCodProd', '')}"
            f"{nf_item.get('prod', {}).get('qCom', 0)}"
            f"{nf_item.get('prod', {}).get('vTotItem', 0)}"
        )
        return hashlib.sha256(base.encode()).hexdigest()

    def validar_integridade(self, raw_nf: dict, raw_order: dict) -> Dict[str, Any]:
        """
        Realiza o de-para entre os itens da NF e do Pedido.
        """
        relatorio = {"status": "FALHA", "erros": [], "hash_validacao": None}

        # 1. Validação de Cabeçalho
        id_pedido_nf = str(raw_nf.get("compl", {}).get("nIdPedido", ""))
        id_pedido_ped = str(raw_order.get("cabecalho", {}).get("codigo_pedido", ""))

        if id_pedido_nf != id_pedido_ped:
            relatorio["erros"].append(f"Divergência de ID Pedido: NF({id_pedido_nf}) vs Pedido({id_pedido_ped})")

        # 2. Validação de Itens
        nf_items = self._ensure_list(raw_nf.get("det", []))
        ped_items = self._ensure_list(raw_order.get("det", []))

        if not nf_items:
            relatorio["erros"].append("Nota Fiscal não possui itens (det) para validar")
            return relatorio

        matches_encontrados = 0
        ultimo_hash = ""

        for nf_item in nf_items:
            found = False
            for ped_item in ped_items:
                try:
                    q_nf = float(nf_item["prod"]["qCom"])
                    q_ped = float(ped_item["produto"]["quantidade"])
                    v_nf = float(nf_item["prod"]["vUnCom"])
                    v_ped = float(ped_item["produto"]["valor_unitario"])
                    
                    cod_prod_nf = str(nf_item["nfProdInt"]["nCodProd"])
                    cod_prod_ped = str(ped_item["produto"]["codigo_produto"])

                    # Margem de tolerância pequena para float
                    if cod_prod_nf == cod_prod_ped and abs(q_nf - q_ped) < 0.001 and abs(v_nf - v_ped) < 0.01:
                        ultimo_hash = self.gerar_hash_item(raw_nf, raw_order, nf_item, ped_item)
                        found = True
                        matches_encontrados += 1
                        break
                except (KeyError, ValueError, TypeError):
                    continue
            
            if not found:
                relatorio["erros"].append(f"Item NF {nf_item.get('prod',{}).get('cProd')} não encontrado no Pedido")

        if matches_encontrados == len(nf_items) and not relatorio["erros"]:
            relatorio["status"] = "OK"
            relatorio["hash_validacao"] = ultimo_hash
        
        return relatorio

    # --- MÉTODOS DE LIMPEZA E ESTRUTURAÇÃO ---

    def clean_nf_data(self, raw_nf: dict) -> dict:
        ide = self._get_safe_dict(raw_nf, "ide")
        compl = self._get_safe_dict(raw_nf, "compl")
        
        return {
            "nNF": str(ide.get("nNF", "")).strip(),
            "dEmi": str(ide.get("dEmi", "")).strip(),
            "hEmi": str(ide.get("hEmi", "")).strip(),
            "cChaveNFe": str(compl.get("cChaveNFe", "")).strip()
        }

    def clean_order_data(self, raw_order: dict, nf_data: Optional[dict] = None, validation_hash: str = None) -> dict:
        """
        Converte o JSON bruto em uma entidade PedidoRefinado.
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

        # 4. Processamento dos ITENS (Produtos) - FALTAVA ISSO
        raw_items_list = self._ensure_list(raw_order.get("det", []))
        items_refinados = []
        
        for item in raw_items_list:
            if isinstance(item, dict):
                prod = item.get("produto", {})
                ide = item.get("ide", {})
                
                prod_obj = ProdutoItem(
                    codigo=str(prod.get("codigo", "")),
                    codigo_produto=int(prod.get("codigo_produto", 0)),
                    descricao=self.normalizar_texto(prod.get("descricao", "")),
                    ncm=str(prod.get("ncm", "")),
                    cfop=str(prod.get("cfop", "")),
                    unidade=str(prod.get("unidade", "")),
                    quantidade=float(prod.get("quantidade", 0)),
                    valor_unitario=float(prod.get("valor_unitario", 0)),
                    valor_total=float(prod.get("valor_total", 0))
                )
                items_refinados.append(ItemPedido(ide=ide, produto=prod_obj))

        # 5. Lista de Parcelas
        raw_parcelas_list = self._ensure_list(self._get_safe_dict(raw_order, "lista_parcelas").get("parcela", []))
        
        parcelas_refinadas = [
            Parcela(
                data_vencimento=str(p.get("data_vencimento", "")),
                numero_parcela=int(p.get("numero_parcela", 0)),
                percentual=float(p.get("percentual", 0)),
                quantidade_dias=int(p.get("quantidade_dias", 0)),
                valor=float(p.get("valor", 0))
            ) for p in raw_parcelas_list if isinstance(p, dict)
        ]
        lista_parcelas = ListaParcelas(parcela=parcelas_refinadas)

        # 6. Observacoes e Total
        observacoes = Observacoes(obs_venda=str(self._get_safe_dict(raw_order, "observacoes").get("obs_venda", "")).strip())
        total_pedido = TotalPedido(valor_total_pedido=float(self._get_safe_dict(raw_order, "total_pedido").get("valor_total_pedido", 0)))

        # 7. Nota Fiscal (Enriquecimento)
        nota_fiscal_obj = NotaFiscalRefinada()
        if nf_data:
            nota_fiscal_obj = NotaFiscalRefinada(
                nNF=str(nf_data.get("nNF", "")),
                dEmi=str(nf_data.get("dEmi", "")),
                hEmi=str(nf_data.get("hEmi", "")),
                cChaveNFe=str(nf_data.get("cChaveNFe", ""))
            )

        # Montagem Final
        pedido = PedidoRefinado(
            cabecalho=cabecalho,
            infoCadastro=info,
            informacoes_adicionais=adicionais,
            det=items_refinados,  # Agora incluímos os itens na montagem final
            lista_parcelas=lista_parcelas,
            observacoes=observacoes,
            total_pedido=total_pedido,
            nota_fiscal=nota_fiscal_obj,
            hash_integridade=validation_hash
        )

        return pedido.to_dict()