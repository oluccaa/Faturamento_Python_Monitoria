import json
import os
import copy
from pathlib import Path
from typing import Set, Dict, List, Any, Union, Optional
from decimal import Decimal
from datetime import datetime, date

from src.infrastructure.custom_logging import logger
from src.config import CONFIG

class JsonRepository:
    """
    Repositório de alta performance para persistência em JSON.
    Garante atomicidade, suporte a tipos financeiros (Decimal) e cache em memória.
    """

    def __init__(self, base_dir: Union[str, Path] = "."):
        self.base_dir = Path(base_dir)
        self._cache: Dict[str, Any] = {} # Cache para evitar I/O excessivo

    def _encoder(self, obj: Any) -> Any:
        """
        Extensão do serializador JSON para tipos complexos de domínio.
        """
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        raise TypeError(f"Objeto do tipo {type(obj).__name__} não é serializável em JSON")

    def _atomic_write(self, path: Path, data: Any):
        """
        Executa a escrita atômica via substituição de arquivo temporário.
        """
        # Garante estrutura de pastas antes da escrita
        path.parent.mkdir(parents=True, exist_ok=True)

        temp_path = path.with_suffix(f".tmp_{os.getpid()}") # Sufixo com PID para segurança em concorrência
        
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(
                    data, 
                    f, 
                    indent=2, 
                    ensure_ascii=False,
                    default=self._encoder
                )
                f.flush()
                os.fsync(f.fileno())
            
            # Operação atômica no OS (Atomic Rename)
            temp_path.replace(path)
            
            # Atualiza o cache interno se este arquivo for lido novamente
            self._cache[path.name] = copy.deepcopy(data)
            
        except Exception as e:
            if temp_path.exists():
                try: temp_path.unlink()
                except: pass
            logger.error(f"💥 Erro fatal de escrita em {path.name}: {e}")
            raise e

    def load_filter_set(self, filename: str) -> Set[str]:
        """
        Lê uma lista JSON e converte em Set para busca O(1).
        """
        # Tenta recuperar do cache primeiro
        if filename in self._cache and isinstance(self._cache[filename], (list, set)):
            return set(str(i) for i in self._cache[filename])

        path = self.base_dir / filename
        if not path.exists():
            return set()
            
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    return set()
                
                # Sanitização: remove vazios e converte para string
                result = {str(item).strip() for item in data if item is not None}
                self._cache[filename] = list(result)
                return result
                
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"❌ Falha ao carregar conjunto {filename}: {e}")
            return set()

    def load_dict(self, filename: str, required: bool = False) -> Dict[str, Any]:
        """
        Carrega um dicionário JSON com suporte a Cache.
        """
        if filename in self._cache and isinstance(self._cache[filename], dict):
            return copy.deepcopy(self._cache[filename])

        path = self.base_dir / filename
        
        if not path.exists():
            if required:
                logger.critical(f"⛔ Arquivo OBRIGATÓRIO ausente: {filename}")
                raise FileNotFoundError(f"Arquivo {filename} não encontrado.")
            return {}

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    self._cache[filename] = copy.deepcopy(data)
                    return data
                return {}
        except Exception as e:
            logger.error(f"❌ Erro de leitura no JSON {filename}: {e}")
            if required: raise e
            return {}

    def save_data(self, filename: str, data: Any):
        """Persiste dados de forma genérica no diretório base."""
        path = self.base_dir / filename
        self._atomic_write(path, data)

    def update_processed_list(self, filename: str, new_ids: List[str]):
        """
        Atualiza incrementalmente a lista de IDs ignorados (Histórico).
        """
        if not new_ids:
            return

        try:
            current_ids = self.load_filter_set(filename)
            updated_ids = current_ids.union({str(i).strip() for i in new_ids})
            
            # Salva de forma ordenada para facilitar auditoria visual
            self.save_data(filename, sorted(list(updated_ids)))
            
        except Exception as e:
            logger.error(f"❌ Falha ao atualizar histórico {filename}: {e}")

    def save_refined_json(self, data: Dict[str, Any], date_ref: str):
        """
        Salva o output processado particionado por data.
        """
        # Normaliza a data para o nome do arquivo (YYYY-MM-DD ou DD_MM_YYYY)
        safe_date = date_ref.replace('/', '_').replace('-', '_')
        filename = f"faturamento_{safe_date}.json"
        path = CONFIG.OUTPUT_DIR / filename
        
        try:
            self._atomic_write(path, data)
            logger.info(f"💾 Backup JSON consolidado: {path.name}")
        except Exception as e:
            logger.error(f"❌ Falha ao persistir faturamento refinado: {e}")
            raise e