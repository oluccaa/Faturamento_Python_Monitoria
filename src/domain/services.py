from src.domain.entities import PedidoRefinado, ItemPedido

class BillingDomainService:
    def clean_order_data(self, raw_order: dict) -> dict:
        """
        Recebe o JSON bruto da API (conforme seu exemplo) e retorna 
        apenas os dados refinados.
        """
        # Acessa os blocos principais do JSON bruto
        cabecalho = raw_order.get("cabecalho", {})
        info = raw_order.get("infoCadastro", {})
        adicionais = raw_order.get("informacoes_adicionais", {})
        total = raw_order.get("total_pedido", {})
        lista_parcelas = raw_order.get("lista_parcelas", {}).get("parcela", [])
        obs = raw_order.get("observacoes", {})

        # Processa as parcelas
        parcelas_refinadas = []
        for p in lista_parcelas:
            parcelas_refinadas.append(ItemPedido(
                numero_parcela=p.get("numero_parcela"),
                data_vencimento=p.get("data_vencimento"),
                valor=p.get("valor")
            ))

        # Cria o objeto refinado
        pedido = PedidoRefinado(
            numero_pedido=str(cabecalho.get("numero_pedido", "S/N")),
            codigo_pedido=cabecalho.get("codigo_pedido"),
            cliente_id=cabecalho.get("codigo_cliente"),
            data_emissao=cabecalho.get("data_previsao"),
            data_faturamento=info.get("dFat"),
            valor_total=total.get("valor_total_pedido"),
            vendedor_id=adicionais.get("codVend"),
            codigo_categoria=adicionais.get("codigo_categoria"),
            origem=cabecalho.get("origem_pedido"),
            observacoes=obs.get("obs_venda", ""),
            parcelas=parcelas_refinadas
        )

        return pedido.to_dict()