from typing import Dict, Any

class BillingDomainService:
    @staticmethod
    def clean_order_data(p: Dict[str, Any]) -> Dict[str, Any]:
        """Aplica a regra de negócio de extração seletiva."""
        # Extraímos apenas os nós solicitados mantendo a integridade
        return {
            "cabecalho": p.get("cabecalho", {}),
            "infoCadastro": p.get("infoCadastro", {}),
            "informacoes_adicionais": p.get("informacoes_adicionais", {}),
            "lista_parcelas": p.get("lista_parcelas", {}),
            "observacoes": p.get("observacoes", {}),
            "total_pedido": p.get("total_pedido", {})
        }