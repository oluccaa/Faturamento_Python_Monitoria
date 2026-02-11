from dataclasses import dataclass, asdict
from typing import List, Optional

@dataclass
class Cabecalho:
    bloqueado: str
    codigo_cenario_impostos: str
    codigo_cliente: int
    codigo_parcela: str
    codigo_pedido: int
    data_previsao: str
    etapa: str
    numero_pedido: str
    origem_pedido: str
    qtde_parcelas: int
    quantidade_itens: int

@dataclass
class InfoCadastro:
    autorizado: str
    cImpAPI: str
    cancelado: str
    dAlt: str
    dFat: str
    dInc: str
    denegado: str
    devolvido: str
    devolvido_parcial: str
    faturado: str
    hAlt: str
    hFat: str
    hInc: str
    uAlt: str
    uFat: str
    uInc: str

@dataclass
class InformacoesAdicionais:
    codProj: int
    codVend: int
    vendedor_nome: str         # Campo Enriquecido
    codigo_categoria: str
    categoria_nome: str        # Campo Enriquecido
    codigo_conta_corrente: int
    consumidor_final: str
    enviar_email: str
    enviar_pix: str
    numero_pedido_cliente: str
    utilizar_emails: str

@dataclass
class Parcela:
    data_vencimento: str
    numero_parcela: int
    percentual: float
    quantidade_dias: int
    valor: float

@dataclass
class ListaParcelas:
    parcela: List[Parcela]

@dataclass
class Observacoes:
    obs_venda: str

@dataclass
class TotalPedido:
    valor_total_pedido: float

@dataclass
class PedidoRefinado:
    cabecalho: Cabecalho
    infoCadastro: InfoCadastro
    informacoes_adicionais: InformacoesAdicionais
    lista_parcelas: ListaParcelas
    observacoes: Observacoes
    total_pedido: TotalPedido

    def to_dict(self):
        # Retorna na raiz 'pedido_venda_produto' conforme seu exemplo
        return {
            "pedido_venda_produto": asdict(self)
        }