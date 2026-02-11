import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from src.config import CONFIG
from src.infrastructure.custom_logging import logger

# Caminhos dos arquivos auxiliares
VENDEDORES_FILE = CONFIG.BASE_DIR / "vendedores.json"
MANIFESTO_FILE = CONFIG.BASE_DIR / "manifesto.json" 

class ReportGenerator:
    def __init__(self):
        self.vendedores_map = self._load_vendedores()
        self.manifesto_set = self._load_manifesto_list()

    def _load_vendedores(self) -> dict:
        """Carrega o JSON de vendedores para mapear ID -> Nome."""
        if not VENDEDORES_FILE.exists():
            return {}
        try:
            with open(VENDEDORES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {str(v.get('codigo_vendedor')): v.get('nome_exibicao') for v in data}
        except Exception as e:
            logger.error(f"❌ Erro ao ler vendedores: {e}")
            return {}

    def _load_manifesto_list(self) -> set:
        """Carrega a lista de IDs do manifesto (se existir)."""
        if not MANIFESTO_FILE.exists():
            return set()
        try:
            with open(MANIFESTO_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(str(item) for item in data)
        except Exception as e:
            return set()

    def _get_vendedor_name(self, cod_vend):
        if not cod_vend: return "N/D"
        return self.vendedores_map.get(str(cod_vend), str(cod_vend))

    def _check_manifesto(self, cod_pedido):
        if not cod_pedido: return ""
        return "S" if str(cod_pedido) in self.manifesto_set else ""

    def _extract_parcelas(self, lista_parcelas):
        """Extrai dados da primeira parcela e resume as demais."""
        if not lista_parcelas or 'parcela' not in lista_parcelas:
            return "", 0.0, ""
        
        parcelas = lista_parcelas['parcela']
        if isinstance(parcelas, dict): parcelas = [parcelas]
        
        if not parcelas:
            return "", 0.0, ""

        # Pega a primeira parcela
        p1 = parcelas[0]
        data_venc = p1.get('data_vencimento', '')
        valor = float(p1.get('valor', 0))
        
        # Cria um resumo textual
        obs_parcelas = f"{len(parcelas)}x" if len(parcelas) > 1 else "1x"
        
        return data_venc, valor, obs_parcelas

    def process_files(self):
        logger.info("📂 Lendo arquivos JSON e mapeando TODOS os campos...")
        
        all_data = []
        files = list(CONFIG.OUTPUT_DIR.glob("*.json"))
        
        if not files:
            logger.warning("⚠️ Nenhum arquivo JSON encontrado.")
            return

        for json_file in files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                    
                    for pv, details in orders.items():
                        # --- Blocos do JSON ---
                        cab = details.get('cabecalho', {})
                        infoc = details.get('infoCadastro', {})
                        info_add = details.get('informacoes_adicionais', {})
                        total = details.get('total_pedido', {})
                        obs = details.get('observacoes', {})
                        parcelas_raw = details.get('lista_parcelas', {})

                        # --- Definição da Data de Referência (Para as Abas) ---
                        data_ref_str = infoc.get('dFat') or cab.get('data_previsao') or infoc.get('dInc')
                        try:
                            data_ref = datetime.strptime(data_ref_str, "%d/%m/%Y")
                        except:
                            continue 

                        # --- Tratamento de Parcelas ---
                        venc_p1, valor_p1, resumo_parcelas = self._extract_parcelas(parcelas_raw)

                        # --- Mapeamento COMPLETO (Campo a Campo) ---
                        row = {
                            # Controle Interno
                            "_DATA_REF": data_ref,

                            # >>> CABEÇALHO <<<
                            "Numero Pedido": cab.get('numero_pedido'),
                            "Cod. Pedido": cab.get('codigo_pedido'),
                            "Cod. Cliente": cab.get('codigo_cliente'),
                            "Etapa": cab.get('etapa'),
                            "Bloqueado": cab.get('bloqueado'),
                            "Origem": cab.get('origem_pedido'),
                            "Cod. Cenario Impostos": cab.get('codigo_cenario_impostos'),
                            "Cod. Parcela": cab.get('codigo_parcela'),
                            "Qtd. Itens": cab.get('quantidade_itens'),
                            "Qtd. Parcelas": cab.get('qtde_parcelas'),
                            "Data Previsao": cab.get('data_previsao'),

                            # >>> INFO CADASTRO (Datas, Horas e Usuários) <<<
                            "Faturado": infoc.get('faturado'),
                            "Data Faturamento": infoc.get('dFat'),
                            "Hora Faturamento": infoc.get('hFat'),
                            "User Faturamento": infoc.get('uFat'),
                            
                            "Data Inclusao": infoc.get('dInc'),
                            "Hora Inclusao": infoc.get('hInc'),
                            "User Inclusao": infoc.get('uInc'),
                            
                            "Data Alteracao": infoc.get('dAlt'),
                            "Hora Alteracao": infoc.get('hAlt'),
                            "User Alteracao": infoc.get('uAlt'),
                            
                            "Autorizado": infoc.get('autorizado'),
                            "Cancelado": infoc.get('cancelado'),
                            "Denegado": infoc.get('denegado'),
                            "Devolvido": infoc.get('devolvido'),
                            "Devolvido Parcial": infoc.get('devolvido_parcial'),
                            "Imp. API": infoc.get('cImpAPI'), # cImpAPI

                            # >>> INFORMAÇÕES ADICIONAIS <<<
                            "Vendedor (Nome)": self._get_vendedor_name(info_add.get('codVend')),
                            "Cod. Vendedor": info_add.get('codVend'),
                            "Cod. Projeto": info_add.get('codProj'),
                            "Cod. Categoria": info_add.get('codigo_categoria'),
                            "Conta Corrente": info_add.get('codigo_conta_corrente'),
                            "Consumidor Final": info_add.get('consumidor_final'),
                            "Pedido Cliente": info_add.get('numero_pedido_cliente'),
                            "Email Contato": info_add.get('utilizar_emails'),
                            "Enviar Email": info_add.get('enviar_email'),
                            "Enviar PIX": info_add.get('enviar_pix'),

                            # >>> FINANCEIRO / TOTAIS <<<
                            "Valor Total": float(total.get('valor_total_pedido', 0)),
                            "Venc. 1a Parc": venc_p1,
                            "Valor 1a Parc": valor_p1,
                            "Resumo Parc.": resumo_parcelas,

                            # >>> OBSERVAÇÕES <<<
                            "Observacoes": obs.get('obs_venda'),

                            # >>> EXTRA (Manifesto Manual) <<<
                            "Manifesto": self._check_manifesto(cab.get('codigo_pedido')),
                        }
                        all_data.append(row)

            except Exception as e:
                logger.error(f"Erro no arquivo {json_file.name}: {e}")

        if not all_data:
            logger.warning("⚠️ Nenhum dado válido extraído.")
            return

        df = pd.DataFrame(all_data)
        self._generate_excel(df)

    def _apply_styles(self, filename):
        """Aplica estilização profissional ao Excel."""
        wb = load_workbook(filename)
        
        # Cores e Fontes
        header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid") # Azul Petróleo
        header_font = Font(bold=True, color="FFFFFF", name="Calibri", size=11)
        
        # Bordas
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        
        # Destaque Manifesto (Vermelho Claro)
        manifesto_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        manifesto_font = Font(color="9C0006")

        for sheet in wb.worksheets:
            sheet.freeze_panes = "A2" # Congela cabeçalho
            
            # Identificar coluna Manifesto
            manifesto_col_idx = None
            for cell in sheet[1]:
                if cell.value == "Manifesto":
                    manifesto_col_idx = cell.column

            for col in sheet.columns:
                max_length = 0
                column = col[0].column_letter
                
                for cell in col:
                    try:
                        cell.border = thin_border
                        
                        # Aplica cor no Manifesto se for "S"
                        if manifesto_col_idx and cell.column == manifesto_col_idx and cell.value == "S":
                            cell.fill = manifesto_fill
                            cell.font = manifesto_font

                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except: pass
                
                # Largura Inteligente (Min 10, Max 50)
                adjusted_width = min(max_length + 3, 50)
                sheet.column_dimensions[column].width = adjusted_width

            # Estilo do Cabeçalho
            for cell in sheet[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center", vertical="center")

        wb.save(filename)
        logger.info("🎨 Estilos aplicados com sucesso!")

    def _generate_excel(self, df):
        output_file = CONFIG.BASE_DIR / "Relatorio_Faturamento_Completo.xlsx"
        logger.info(f"📊 Gerando Excel estruturado em: {output_file}")

        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 1. Ordenação Global pela data de referência
                df = df.sort_values(by='_DATA_REF')
                
                # 2. Agrupamento por Ano
                groups = df.groupby(df['_DATA_REF'].dt.year)

                for year, group_data in groups:
                    sheet_name = str(year)
                    
                    # Remove coluna auxiliar
                    group_export = group_data.drop(columns=['_DATA_REF'])
                    
                    group_export.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"  ✅ Aba criada: {sheet_name} ({len(group_data)} pedidos)")
            
            self._apply_styles(output_file)
            
        except Exception as e:
            logger.error(f"💥 Falha ao salvar Excel: {e}")

if __name__ == "__main__":
    generator = ReportGenerator()
    generator.process_files()