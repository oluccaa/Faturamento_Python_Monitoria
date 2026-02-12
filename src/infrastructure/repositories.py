import json
import os
import copy
from pathlib import Path
from typing import Set, Dict, List, Any, Union, Optional
from decimal import Decimal
from datetime import datetime, date

# Tenta importar logger e config, com fallback para testes isolados
try:
    from src.infrastructure.custom_logging import logger
    from src.config import CONFIG
except ImportError:
    import logging
    logger = logging.getLogger("RepoFallback")
    class ConfigMock:
        OUTPUT_DIR = Path("data/output")
    CONFIG = ConfigMock()

class JsonRepository:
    """
    Repositório de alta performance para persistência em JSON.
    Garante atomicidade, suporte a tipos financeiros (Decimal) e cache em memória.
    """

    def __init__(self, base_dir: Union[str, Path] = "."):
        self.base_dir = Path(base_dir).resolve()
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
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"❌ Falha ao criar diretório {path.parent}: {e}")
            raise

        # Nome temporário seguro
        temp_path = path.with_suffix(f".tmp_{os.getpid()}_{datetime.now().microsecond}") 
        
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
            
            # Atualiza o cache interno se a chave for conhecida (nome do arquivo)
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
        Usa Cache de Memória se disponível.
        """
        # 1. Tenta recuperar do cache primeiro (Fast Path)
        if filename in self._cache:
            cached_data = self._cache[filename]
            if isinstance(cached_data, set):
                return cached_data
            if isinstance(cached_data, list):
                return set(str(i) for i in cached_data)

        # 2. Caminho do arquivo
        path = self.base_dir / filename
        if not path.exists():
            return set()
            
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                if not isinstance(data, list):
                    logger.warning(f"⚠️ Arquivo {filename} deveria ser uma lista, mas é {type(data)}. Ignorando.")
                    return set()
                
                # Sanitização: remove vazios e converte para string
                result = {str(item).strip() for item in data if item is not None}
                
                # Atualiza Cache como SET para performance
                self._cache[filename] = result
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
                
                # Se for lista mas esperava dict (ex: vendedores.json as vezes vem como lista)
                if isinstance(data, list):
                     logger.warning(f"⚠️ Arquivo {filename} é uma lista, retornando vazio em load_dict.")
                     return {} # Caller deve tratar se quiser lista

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
        Lê do disco (ou cache), faz o append e salva de volta.
        """
        if not new_ids:
            return

        try:
            # Carrega o SET atual
            current_ids = self.load_filter_set(filename)
            
            # Adiciona novos IDs
            updated_ids = current_ids.union({str(i).strip() for i in new_ids})
            
            # Atualiza Cache Imediatamente
            self._cache[filename] = updated_ids
            
            # Salva no disco como LISTA ordenada
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
        
        # Usa o diretório de output definido na CONFIG, não o base_dir do repo
        path = CONFIG.OUTPUT_DIR / filename
        
        try:
            self._atomic_write(path, data)
            logger.info(f"💾 Backup JSON consolidado salvo em: {path}")
        except Exception as e:
            logger.error(f"❌ Falha ao persistir faturamento refinado: {e}")
            raise e