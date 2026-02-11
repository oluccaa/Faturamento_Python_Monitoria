from dataclasses import dataclass, asdict
from typing import List, Optional

@dataclass
class ItemPedido:
    numero_parcela: int
    data_vencimento: str
    valor: float

@dataclass
class PedidoRefinado:
    numero_pedido: str      # Ex: "13090" (Chave do JSON final)
    codigo_pedido: int      # Ex: 10120853337 (Usado para filtro)
    cliente_id: int
    data_emissao: str
    data_faturamento: str
    valor_total: float
    vendedor_id: int
    codigo_categoria: str   # Ex: "1.03.98"
    origem: str
    observacoes: str
    parcelas: List[ItemPedido]

    def to_dict(self):
        return asdict(self)