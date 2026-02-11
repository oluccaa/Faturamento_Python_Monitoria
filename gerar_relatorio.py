import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.chart import (
    BarChart, 
    LineChart, 
    PieChart, 
    DoughnutChart,
    Reference, 
    Series,
    layout
)
from openpyxl.drawing.image import Image

from src.config import CONFIG
from src.infrastructure.custom_logging import logger
from src.infrastructure.repositories import JsonRepository

class ReportGenerator:
    def __init__(self):
        self.repo = JsonRepository(CONFIG.BASE_DIR)
        self.manifesto_set = self.repo.load_filter_set("manifesto.json")

    # --- Helpers de Conversão ---
    def _parse_date(self, date_str: Any) -> Optional[date]:
        if not date_str or not isinstance(date_str, str): return None
        try: return datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError: return None

    def _parse_currency(self, value: Any) -> float:
        try: return float(value)
        except: return 0.0

    def _extract_parcelas(self, lista_parcelas: dict):
        if not lista_parcelas: return None, 0.0, 0, ""
        parcelas = lista_parcelas.get('parcela', [])
        if isinstance(parcelas, dict): parcelas = [parcelas]
        if not parcelas: return None, 0.0, 0, ""

        p1 = parcelas[0]
        dt_venc = self._parse_date(p1.get('data_vencimento'))
        valor_p1 = self._parse_currency(p1.get('valor', 0))
        dias = int(p1.get('quantidade_dias', 0))
        qtd = len(parcelas)
        resumo = f"{qtd}x" if qtd > 1 else "À Vista"
        
        return dt_venc, valor_p1, dias, resumo

    def _flatten_json(self, y: dict) -> dict:
        out = {}
        def flatten(x, name=''):
            if type(x) is dict:
                for a in x: flatten(x[a], name + a + '_')
            elif type(x) is list:
                out[name[:-1]] = str(x)
            else:
                out[name[:-1]] = x
        flatten(y)
        return out

    # --- Core Processing ---
    def process_latest(self):
        target_file = self._get_latest_file()
        if not target_file:
            logger.warning("⚠️ Nenhum arquivo JSON encontrado.")
            return

        logger.info(f"📂 Processando Analytics: {target_file.name}")
        self._process_json_file(target_file)

    def _get_latest_file(self) -> Optional[Path]:
        files = list(CONFIG.OUTPUT_DIR.glob("*.json"))
        if not files: return None
        files.sort(key=os.path.getmtime, reverse=True)
        return files[0]

    def _process_json_file(self, json_file: Path):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                orders = json.load(f)

            all_data = []
            
            for key, content in orders.items():
                details = content.get('pedido_venda_produto', content)
                
                # Blocos
                cab = details.get('cabecalho', {})
                infoc = details.get('infoCadastro', {})
                adic = details.get('informacoes_adicionais', {})
                total = details.get('total_pedido', {})
                parcelas = details.get('lista_parcelas', {})
                nota = details.get('nota_fiscal', {}) # Vindo do enriquecimento
                obs = details.get('observacoes', {})

                # Datas
                d_fat = self._parse_date(infoc.get('dFat'))
                d_inc = self._parse_date(infoc.get('dInc'))
                d_prev = self._parse_date(cab.get('data_previsao'))
                data_ref = d_fat or d_prev or d_inc
                
                if not data_ref: continue

                # Métricas Calculadas
                # Lead Time (Dias entre Inclusão e Faturamento)
                lead_time = (d_fat - d_inc).days if (d_fat and d_inc) else None
                
                # Hora de Inclusão (Para mapa de calor)
                h_inc_str = infoc.get('hInc', '00:00:00')
                hora_pico = int(h_inc_str.split(':')[0]) if h_inc_str else 0

                venc_p1, valor_p1, dias_p1, condicao = self._extract_parcelas(parcelas)

                # Linha Premium
                premium_row = {
                    "_DATA_REF": data_ref,
                    "Mes_Ano": data_ref.strftime("%Y-%m"),
                    "Ano": data_ref.year,
                    
                    # Identificação
                    "Numero Pedido": cab.get('numero_pedido'),
                    "ID Pedido": int(cab.get('codigo_pedido', 0)),
                    "Cliente ID": int(cab.get('codigo_cliente', 0)),
                    
                    # Comercial
                    "Vendedor": adic.get('vendedor_nome', "N/D"),
                    "Categoria": adic.get('categoria_nome', "N/D"),
                    "Valor Total": self._parse_currency(total.get('valor_total_pedido', 0)),
                    "Situação": "Cancelado" if infoc.get('cancelado') == 'S' else ("Faturado" if infoc.get('faturado') == 'S' else "Aberto"),
                    
                    # Inteligência
                    "Lead Time (Dias)": lead_time,
                    "Hora Inclusão (Int)": hora_pico,
                    "Condição Pagto": condicao,
                    
                    # Fiscal
                    "NFe Numero": nota.get('nNF', ''),
                    "NFe Emissão": nota.get('dEmi', ''),
                    "Tem NFe?": "Sim" if nota.get('nNF') else "Não",
                    
                    # Auditoria
                    "Dt. Faturamento": d_fat,
                    "Dt. Inclusão": d_inc,
                    "User Faturam.": infoc.get('uFat'),
                    "Manifesto": "S" if str(cab.get('codigo_pedido')) in self.manifesto_set else "",
                }

                # Dados Brutos (Flatten)
                flat_data = self._flatten_json(details)
                # Remove lixo
                for k in list(flat_data.keys()):
                    if "utilizar_emails" in k or "xml" in k: del flat_data[k]

                all_data.append({**premium_row, **flat_data})

            if not all_data: return

            df = pd.DataFrame(all_data)
            self._generate_excel(df)

        except Exception as e:
            logger.error(f"❌ Erro crítico: {e}")
            raise e

    # --- DASHBOARD ENGINE ---
    
    def _style_card(self, ws, ref, title, value, subtext="", color="1F4E78"):
        """Cria um cartão de KPI visualmente atraente."""
        cell = ws[ref]
        cell.value = value
        cell.font = Font(bold=True, size=20, color=color)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Borda grossa em volta do cartão
        thin = Side(border_style="thin", color="CCCCCC")
        cell.border = Border(top=thin, left=thin, right=thin, bottom=thin)

        # Título
        col = get_column_letter(cell.column)
        r_title = cell.row - 1
        t_cell = ws[f"{col}{r_title}"]
        t_cell.value = title.upper()
        t_cell.font = Font(bold=True, size=9, color="666666")
        t_cell.alignment = Alignment(horizontal="center", vertical="bottom")

        # Subtexto (ex: vs mês passado)
        if subtext:
            r_sub = cell.row + 1
            s_cell = ws[f"{col}{r_sub}"]
            s_cell.value = subtext
            s_cell.font = Font(italic=True, size=8, color="888888")
            s_cell.alignment = Alignment(horizontal="center", vertical="top")

    def _create_dashboard(self, writer, df: pd.DataFrame):
        wb = writer.book
        ws = wb.create_sheet("DASHBOARD 360", 0)
        ws.sheet_view.showGridLines = False
        
        # Título do Dashboard
        ws["B1"] = f"DASHBOARD DE VENDAS & OPERAÇÕES - {datetime.now().year}"
        ws["B1"].font = Font(bold=True, size=18, color="1F4E78")

        # --- 1. CÁLCULO DE MÉTRICAS ---
        faturados = df[df["Situação"] == "Faturado"]
        
        total_fat = df["Valor Total"].sum()
        total_orders = len(df)
        avg_ticket = total_fat / total_orders if total_orders else 0
        
        # Lead Time Médio (apenas dos faturados com data válida)
        lead_times = faturados["Lead Time (Dias)"].dropna()
        avg_lead_time = lead_times.mean() if not lead_times.empty else 0
        
        # % Com Nota Fiscal
        com_nf = len(df[df["Tem NFe?"] == "Sim"])
        perc_fiscal = (com_nf / total_orders * 100) if total_orders else 0

        # --- 2. POSICIONAMENTO DOS CARDS (KPIs) ---
        self._style_card(ws, "B4", "Faturamento Total", total_fat, color="2E7D32") # Verde
        ws["B4"].number_format = 'R$ #,##0.00'
        
        self._style_card(ws, "D4", "Total Pedidos", total_orders)
        
        self._style_card(ws, "F4", "Ticket Médio", avg_ticket)
        ws["F4"].number_format = 'R$ #,##0.00'
        
        self._style_card(ws, "H4", "Lead Time Médio", f"{avg_lead_time:.1f} dias", "Inclusão -> Fat.", color="E65100")
        
        self._style_card(ws, "J4", "Cobertura Fiscal", f"{perc_fiscal:.1f}%", "Pedidos com NFe", color="1565C0")

        # --- 3. PREPARAÇÃO DE DADOS PARA GRÁFICOS (TABELAS AUXILIARES) ---
        # As tabelas auxiliares ficam ocultas ou na lateral (Colunas AA em diante)
        
        # A. Evolução Mensal
        df_time = df.groupby("Mes_Ano")["Valor Total"].sum().reset_index().sort_values("Mes_Ano")
        self._write_chart_data(ws, "AA5", ["Período", "Faturamento"], df_time)
        
        # B. Top 5 Vendedores
        df_vend = df.groupby("Vendedor")["Valor Total"].sum().nlargest(5).sort_values(ascending=True).reset_index()
        self._write_chart_data(ws, "AD5", ["Vendedor", "Total"], df_vend)
        
        # C. Categorias (Pizza)
        df_cat = df.groupby("Categoria")["Valor Total"].sum().nlargest(6).reset_index()
        self._write_chart_data(ws, "AG5", ["Categoria", "Total"], df_cat)
        
        # D. Pico de Vendas (Horário)
        df_hour = df.groupby("Hora Inclusão (Int)")["ID Pedido"].count().reset_index()
        self._write_chart_data(ws, "AJ5", ["Hora", "Qtd Pedidos"], df_hour)

        # --- 4. GERAÇÃO DOS GRÁFICOS ---
        
        # Gráfico 1: Evolução Temporal (Área ou Linha)
        c1 = LineChart()
        c1.title = "Tendência de Faturamento"
        c1.style = 13
        c1.y_axis.title = "Valor (R$)"
        # Referências dinâmicas
        len_time = len(df_time)
        data = Reference(ws, min_col=28, min_row=6, max_row=6+len_time-1) # Col AB
        cats = Reference(ws, min_col=27, min_row=6, max_row=6+len_time-1) # Col AA
        c1.add_data(data, titles_from_data=False)
        c1.set_categories(cats)
        c1.height = 10; c1.width = 18
        ws.add_chart(c1, "B8")

        # Gráfico 2: Top Vendedores (Barras)
        c2 = BarChart()
        c2.type = "bar"
        c2.title = "Top 5 Vendedores (Performance)"
        c2.style = 11
        len_vend = len(df_vend)
        data = Reference(ws, min_col=31, min_row=6, max_row=6+len_vend-1)
        cats = Reference(ws, min_col=30, min_row=6, max_row=6+len_vend-1)
        c2.add_data(data, titles_from_data=False)
        c2.set_categories(cats)
        c2.height = 10; c2.width = 15
        ws.add_chart(c2, "K8")

        # Gráfico 3: Mix de Categorias (Rosca/Doughnut)
        c3 = DoughnutChart()
        c3.title = "Share por Categoria"
        c3.style = 26
        len_cat = len(df_cat)
        data = Reference(ws, min_col=34, min_row=6, max_row=6+len_cat-1)
        cats = Reference(ws, min_col=33, min_row=6, max_row=6+len_cat-1)
        c3.add_data(data, titles_from_data=False)
        c3.set_categories(cats)
        c3.height = 10; c3.width = 15
        ws.add_chart(c3, "B28")

        # Gráfico 4: Pico de Vendas (Colunas) - NOVIDADE
        c4 = BarChart()
        c4.type = "col"
        c4.title = "Horário de Pico (Qtd Pedidos)"
        c4.style = 4
        c4.x_axis.title = "Hora do Dia (0-23h)"
        len_hour = len(df_hour)
        data = Reference(ws, min_col=37, min_row=6, max_row=6+len_hour-1)
        cats = Reference(ws, min_col=36, min_row=6, max_row=6+len_hour-1)
        c4.add_data(data, titles_from_data=False)
        c4.set_categories(cats)
        c4.height = 10; c4.width = 18
        ws.add_chart(c4, "K28")

    def _write_chart_data(self, ws, start_cell, headers, df):
        """Escreve dataframes em áreas auxiliares para alimentar gráficos."""
        col_letter = "".join(filter(str.isalpha, start_cell))
        start_row = int("".join(filter(str.isdigit, start_cell)))
        col_idx = 27 # Começando em AA logicamente, mas usaremos openpyxl coords
        
        # Converte letra da coluna para indice se necessário, ou usa lógica manual
        # Simplificação: Usar loop direto nas células
        
        # Header
        c = ws[start_cell]
        r = c.row
        c_idx = c.column
        
        ws.cell(row=r, column=c_idx, value=headers[0])
        ws.cell(row=r, column=c_idx+1, value=headers[1])
        
        # Data
        for i, row in df.iterrows():
            ws.cell(row=r+1+i, column=c_idx, value=row[0])
            ws.cell(row=r+1+i, column=c_idx+1, value=row[1])

    def _generate_excel(self, df: pd.DataFrame):
        output_file = CONFIG.BASE_DIR / "Relatorio_BI_Avancado.xlsx"
        logger.info(f"📊 Gerando BI Avançado em: {output_file}")

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            self._create_dashboard(writer, df)
            
            # Aba de Dados (Fonte)
            df_sorted = df.sort_values(by='_DATA_REF', ascending=True)
            export_df = df_sorted.drop(columns=['_DATA_REF', 'Mes_Ano', 'Ano', 'Hora Inclusão (Int)'])
            export_df.to_excel(writer, sheet_name="BASE_DE_DADOS", index=False)
        
        self._apply_advanced_styles(output_file)

    def _apply_advanced_styles(self, filename: Path):
        wb = load_workbook(filename)
        currency_fmt = 'R$ #,##0.00'
        date_fmt = 'dd/mm/yyyy'

        # Formata aba de dados
        if "BASE_DE_DADOS" in wb.sheetnames:
            ws = wb["BASE_DE_DADOS"]
            
            # Tabela
            if ws.max_row > 1:
                ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
                tab = Table(displayName="TabDados", ref=ref)
                tab.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
                ws.add_table(tab)
            
            # Colunas
            for col in ws.columns:
                header = str(col[0].value or "")
                col_letter = get_column_letter(col[0].column)
                ws.column_dimensions[col_letter].width = 20 # Padrão
                
                if "Valor" in header:
                    for cell in col[1:]: cell.number_format = currency_fmt
                elif "Dt." in header or "Venc." in header or "Emissão" in header:
                    for cell in col[1:]: cell.number_format = date_fmt
        
        wb.save(filename)
        logger.info("✅ Dashboard de BI (Next Level) gerado com sucesso!")

if __name__ == "__main__":
    gen = ReportGenerator()
    gen.process_latest()