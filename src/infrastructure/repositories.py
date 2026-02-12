import json
import os
from pathlib import Path
from typing import Set, Dict, List, Any, Union
from decimal import Decimal
from datetime import datetime, date
from src.infrastructure.custom_logging import logger

# Configuração de diretórios
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"          # Para JSONs brutos (Cache)
PROCESSED_DIR = DATA_DIR / "processed" # Para JSONs limpos e finais

class JsonRepository:
    """
    Gerencia o salvamento e carregamento de arquivos JSON.
    Cria as pastas necessárias automaticamente.
    """

    def __init__(self, base_dir: Union[str, Path] = "."):
        self.base_dir = Path(base_dir).resolve()
        # Garante que as pastas existam
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    def _encoder(self, obj: Any) -> Any:
        """Serializador para tipos não suportados nativamente pelo JSON."""
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return str(obj)

    def save_raw(self, data: Any, filename: str):
        """Salva dados brutos (cache do passo 1 e 2)."""
        path = RAW_DIR / filename
        self._atomic_write(path, data)
        logger.info(f"💾 Cache Bruto salvo: {path} ({len(data)} registros)")

    def save_refined(self, data: Any, date_ref: str):
        """Salva o JSON final processado."""
        safe_date = date_ref.replace('/', '_').replace('-', '_')
        filename = f"faturamento_{safe_date}.json"
        path = PROCESSED_DIR / filename
        self._atomic_write(path, data)
        logger.info(f"💾 JSON Refinado salvo: {path}")
        return path

    def _atomic_write(self, path: Path, data: Any):
        """Escrita atômica segura."""
        temp_path = path.with_suffix(".tmp")
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False, default=self._encoder)
            
            # Substitui arquivo antigo pelo novo atomicamente
            if path.exists():
                path.unlink()
            os.rename(temp_path, path)
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e

    def load_dict(self, filename: str) -> Dict:
        """Carrega arquivos de configuração (vendedores.json, etc)."""
        path = self.base_dir / filename
        if not path.exists():
            logger.warning(f"⚠️ Arquivo não encontrado: {filename}")
            return {}
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"❌ Erro ao ler {filename}: {e}")
            return {}

    def load_filter_set(self, filename: str) -> Set[str]:
        """Carrega lista de IDs para ignorar (manifestados/processados)."""
        data = self.load_dict(filename)
        if isinstance(data, list):
            return set(map(str, data))
        return set()
        
    def update_processed_list(self, filename: str, new_ids: List[str]):
        """Atualiza a lista de IDs processados no disco."""
        if not new_ids: return
        
        current_set = self.load_filter_set(filename)
        current_set.update(str(x) for x in new_ids)
        
        path = self.base_dir / filename
        self._atomic_write(path, sorted(list(current_set)))