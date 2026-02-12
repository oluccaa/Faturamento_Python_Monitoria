from dataclasses import dataclass, asdict, field
from typing import List, Optional, Any, Dict, Union
from decimal import Decimal, ROUND_HALF_UP

def _to_decimal(value: Any) -> Decimal:
    """Converte valores para Decimal garantindo precisão monetária de 2 casas."""
    if value is None or value == "":
        return Decimal("0.00")
    try:
        if isinstance(value, (int, float)):
            value = str(value)
        return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (ValueError, TypeError, ArithmeticError):
        return Decimal("0.00")

# --- NOVAS CLASSES PARA ITENS (Necessário para Validação) ---

@dataclass
class ProdutoItem:
    codigo: str = ""
    codigo_produto: int = 0
    descricao: str = ""
    ncm: str = ""
    cfop: str = ""
    unidade: str = ""
    quantidade: Decimal = field(default_factory=lambda: Decimal("0.00"))
    valor_unitario: Decimal = field(default_factory=lambda: Decimal("0.00"))
    valor_total: Decimal = field(default_factory=lambda: Decimal("0.00"))

    def __post_init__(self):
        self.quantidade = _to_decimal(self.quantidade)
        self.valor_unitario = _to_decimal(self.valor_unitario)
        self.valor_total = _to_decimal(self.valor_total)

@dataclass
class ItemPedido:
    ide: dict = field(default_factory=dict) # Pode conter indices como sequencia
    produto: ProdutoItem = field(default_factory=ProdutoItem)

# --- FIM DAS NOVAS CLASSES ---

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
        try:
            self.codigo_cliente = int(self.codigo_cliente or 0)
            self.codigo_pedido = int(self.codigo_pedido or 0)
            self.qtde_parcelas = int(self.qtde_parcelas or 0)
        except ValueError:
            pass

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
        try:
            self.numero_parcela = int(self.numero_parcela or 0)
        except ValueError:
            pass

@dataclass
class ListaParcelas:
    parcela: List[Parcela] = field(default_factory=list)

@dataclass
class Observacoes:
    obs_venda: str = ""

@dataclass
class TotalPedido:
    valor_total_pedido: Decimal = field(default_factory=lambda: Decimal("0.00"))

    def __post_init__(self):
        self.valor_total_pedido = _to_decimal(self.valor_total_pedido)

@dataclass
class NotaFiscalRefinada:
    nNF: str = ""
    dEmi: str = ""
    hEmi: str = ""
    cChaveNFe: str = ""

@dataclass
class PedidoRefinado:
    cabecalho: Cabecalho = field(default_factory=Cabecalho)
    infoCadastro: InfoCadastro = field(default_factory=InfoCadastro)
    informacoes_adicionais: InformacoesAdicionais = field(default_factory=InformacoesAdicionais)
    det: List[ItemPedido] = field(default_factory=list) # ADICIONADO AQUI
    lista_parcelas: ListaParcelas = field(default_factory=ListaParcelas)
    observacoes: Observacoes = field(default_factory=Observacoes)
    total_pedido: TotalPedido = field(default_factory=TotalPedido)
    nota_fiscal: NotaFiscalRefinada = field(default_factory=NotaFiscalRefinada)
    hash_integridade: Optional[str] = None # ADICIONADO PARA O CHECK

    def to_dict(self) -> Dict[str, Any]:
        """Converte a dataclass para dicionário, tratando Decimals."""
        data = asdict(self)
        
        # Função auxiliar interna para varrer o dict gerado pelo asdict e corrigir tipos
        def fix_types(obj):
            if isinstance(obj, dict):
                return {k: fix_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [fix_types(i) for i in obj]
            elif isinstance(obj, Decimal):
                return float(obj)
            return obj

        return fix_types(data)