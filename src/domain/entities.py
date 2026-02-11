from dataclasses import dataclass, asdict, field
from typing import List, Optional, Any, Dict, Union
from decimal import Decimal, ROUND_HALF_UP

def _to_decimal(value: Any) -> Decimal:
    """Converte valores para Decimal garantindo precisão monetária de 2 casas."""
    if value is None or value == "":
        return Decimal("0.00")
    try:
        # Garante a conversão de float/str para Decimal e arredonda para 2 casas
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (ValueError, TypeError, ArithmeticError):
        return Decimal("0.00")

@dataclass
class Cabecalho:
    bloqueado: str = "N"
    codigo_cenario_impostos: str = ""
    codigo_cliente: int = 0
    codigo_parcela: str = ""
    codigo_pedido: int = 0
    data_previsao: str = ""
    etapa: str = ""
    numero_pedido: str = ""
    origem_pedido: str = ""
    qtde_parcelas: int = 0
    quantidade_itens: int = 0

    def __post_init__(self):
        # Garante que IDs e Quantidades sejam sempre inteiros
        self.codigo_cliente = int(self.codigo_cliente or 0)
        self.codigo_pedido = int(self.codigo_pedido or 0)
        self.qtde_parcelas = int(self.qtde_parcelas or 0)

@dataclass
class InfoCadastro:
    autorizado: str = "N"
    cImpAPI: str = "N"
    cancelado: str = "N"
    dAlt: str = ""
    dFat: str = ""
    dInc: str = ""
    denegado: str = "N"
    devolvido: str = "N"
    devolvido_parcial: str = "N"
    faturado: str = "N"
    hAlt: str = ""
    hFat: str = ""
    hInc: str = ""
    uAlt: str = ""
    uFat: str = ""
    uInc: str = ""

    @property
    def status_real(self) -> str:
        """Retorna o status consolidado do pedido para o Dashboard."""
        if self.cancelado == "S": return "Cancelado"
        if self.devolvido == "S": return "Devolvido"
        if self.faturado == "S": return "Faturado"
        return "Em Aberto"

@dataclass
class InformacoesAdicionais:
    codProj: int = 0
    codVend: int = 0
    vendedor_nome: str = "N/D"
    codigo_categoria: str = ""
    categoria_nome: str = "N/D"
    codigo_conta_corrente: int = 0
    consumidor_final: str = "N"
    enviar_email: str = "N"
    enviar_pix: str = "N"
    numero_pedido_cliente: str = ""
    utilizar_emails: str = ""

@dataclass
class Parcela:
    data_vencimento: str = ""
    numero_parcela: int = 0
    percentual: float = 0.0
    quantidade_dias: int = 0
    valor: Decimal = field(default_factory=lambda: Decimal("0.00"))

    def __post_init__(self):
        self.valor = _to_decimal(self.valor)
        self.numero_parcela = int(self.numero_parcela or 0)

@dataclass
class ListaParcelas:
    parcela: List[Parcela] = field(default_factory=list)

    def resumo_financeiro(self) -> str:
        """Retorna texto descritivo para o Excel."""
        qtd = len(self.parcela)
        if qtd == 0: return "À Vista / S. Info"
        if qtd == 1: return "1x (À Vista)"
        return f"{qtd}x Parcelado"

@dataclass
class Observacoes:
    obs_venda: str = ""

@dataclass
class TotalPedido:
    valor_total_pedido: Decimal = field(default_factory=lambda: Decimal("0.00"))

    def __post_init__(self):
        self.valor_total_pedido = _to_decimal(self.valor_total_pedido)

@dataclass
class PedidoRefinado:
    cabecalho: Cabecalho = field(default_factory=Cabecalho)
    infoCadastro: InfoCadastro = field(default_factory=InfoCadastro)
    informacoes_adicionais: InformacoesAdicionais = field(default_factory=InformacoesAdicionais)
    lista_parcelas: ListaParcelas = field(default_factory=ListaParcelas)
    observacoes: Observacoes = field(default_factory=Observacoes)
    total_pedido: TotalPedido = field(default_factory=TotalPedido)

    def to_dict(self) -> Dict[str, Any]:
        """Serializa para JSON-safe convertendo Decimals para float."""
        
        def serialize(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: serialize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [serialize(i) for i in obj]
            if isinstance(obj, Decimal):
                return float(obj)
            if hasattr(obj, "__dict__"):
                return serialize(asdict(obj))
            return obj

        return {
            "pedido_venda_produto": serialize(asdict(self))
        }