import json
from pathlib import Path
from src.infrastructure.logging import logger

class JsonRepository:
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)

    def load_filter_set(self, filename: str) -> set:
        """Carrega JSON e retorna um SET de strings para comparação rápida (O(1))"""
        path = self.base_dir / filename
        if not path.exists():
            return set()
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Garante que estamos comparando strings
                return set(str(item) for item in data)
        except Exception as e:
            logger.error(f"❌ Erro ao ler {filename}: {e}")
            return set()

    def update_processed_list(self, filename: str, new_ids: list):
        """Atualiza o arquivo de processados adicionando os novos IDs"""
        path = self.base_dir / filename
        current_ids = self.load_filter_set(filename)
        
        # Une os conjuntos
        updated_ids = current_ids.union(set(str(i) for i in new_ids))
        
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(sorted(list(updated_ids)), f, indent=2)
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar {filename}: {e}")

    def save_refined_json(self, data: dict, date_ref: str):
        filename = f"faturamento_{date_ref.replace('/', '_')}.json"
        path = CONFIG.OUTPUT_DIR / filename # Usa o output do config
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ Arquivo Refinado Salvo: {path}")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar refinado: {e}")