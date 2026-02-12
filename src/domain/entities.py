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

# --- SUB-ENTIDADES ---

@dataclass
class Cabecalho:
    bloqueado: str = ""
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

@dataclass
class InfoCadastro:
    autorizado: str = ""
    cImpAPI: str = ""
    cancelado: str = ""
    dAlt: str = ""
    dFat: str = ""
    dInc: str = ""
    denegado: str = ""
    devolvido: str = ""
    devolvido_parcial: str = ""
    faturado: str = ""
    hAlt: str = ""
    hFat: str = ""
    hInc: str = ""
    uAlt: str = ""
    uFat: str = ""
    uInc: str = ""

@dataclass
class InformacoesAdicionais:
    codProj: int = 0
    codVend: int = 0
    vendedor_nome: str = ""
    codigo_categoria: str = ""
    categoria_nome: str = ""
    codigo_conta_corrente: int = 0
    consumidor_final: str = ""
    enviar_email: str = ""
    enviar_pix: str = ""
    numero_pedido_cliente: str = ""
    utilizar_emails: str = ""

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
    ide: Dict[str, Any] = field(default_factory=dict)
    produto: ProdutoItem = field(default_factory=ProdutoItem)

@dataclass
class Parcela:
    data_vencimento: str = ""
    numero_parcela: int = 0
    percentual: float = 0.0
    quantidade_dias: int = 0
    valor: float = 0.0

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
    serie: str = ""
    dEmi: str = ""
    hEmi: str = ""
    cChaveNFe: str = ""
    valor_total_nf: Decimal = field(default_factory=lambda: Decimal("0.00"))

    def __post_init__(self):
        self.valor_total_nf = _to_decimal(self.valor_total_nf)

# --- ENTIDADE RAIZ ---

@dataclass
class PedidoRefinado:
    # Identificadores principais para busca rápida
    numero_pedido: str = ""
    codigo_pedido: int = 0
    
    # Blocos de dados
    cabecalho: Cabecalho = field(default_factory=Cabecalho)
    infoCadastro: InfoCadastro = field(default_factory=InfoCadastro)
    informacoes_adicionais: InformacoesAdicionais = field(default_factory=InformacoesAdicionais)
    
    # Detalhes e Itens
    det: List[Dict] = field(default_factory=list) # Lista simplificada de itens
    lista_parcelas: ListaParcelas = field(default_factory=ListaParcelas)
    observacoes: Observacoes = field(default_factory=Observacoes)
    
    # Totais
    total_pedido: TotalPedido = field(default_factory=TotalPedido)
    
    # Dados Fiscais e Controle
    nota_fiscal: NotaFiscalRefinada = field(default_factory=NotaFiscalRefinada)
    
    # Campos de Auditoria do Processo ETL
    status_processo: str = "PENDENTE"  # PENDENTE, FATURADO_COMPLETO, DIVERGENTE, SEM_NF
    hash_integridade: Optional[str] = None # Hash MD5 para validar se Pedido == NF

    def to_dict(self) -> Dict[str, Any]:
        """Converte a dataclass para dicionário, tratando Decimals para JSON serializable."""
        data = asdict(self)
        
        def fix_types(obj):
            if isinstance(obj, dict):
                return {k: fix_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [fix_types(v) for v in obj]
            elif isinstance(obj, Decimal):
                return float(obj)
            return obj
            
        return fix_types(data)