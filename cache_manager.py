"""
Gestor de caché LRU para documentos
===================================
"""

from collections import OrderedDict
from logger_config import logger
from config import CACHE_MAX_DOCS, CACHE_MAX_SIZE_MB

class LRUCache:
    """Caché LRU con límite de documentos y tamaño total."""
    
    def __init__(self, max_docs=CACHE_MAX_DOCS, max_size_mb=CACHE_MAX_SIZE_MB):
        self.max_docs = max_docs
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.cache = OrderedDict()
        self.total_size = 0
    
    def get(self, key):
        """Obtiene valor y lo mueve al final (más reciente)."""
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]
    
    def put(self, key, value):
        """Inserta o actualiza, evictando si es necesario."""
        # Si ya existe, actualizamos
        if key in self.cache:
            old_size = len(self.cache[key].encode('utf-8')) if isinstance(self.cache[key], str) else 0
            self.total_size -= old_size
            del self.cache[key]
        
        # Calcular tamaño del nuevo valor
        new_size = len(value.encode('utf-8')) if isinstance(value, str) else 0
        
        # Evictar si es necesario
        while (len(self.cache) >= self.max_docs or 
               self.total_size + new_size > self.max_size_bytes) and self.cache:
            oldest_key, oldest_val = self.cache.popitem(last=False)
            old_size = len(oldest_val.encode('utf-8')) if isinstance(oldest_val, str) else 0
            self.total_size -= old_size
            logger.debug(f"Caché: evicted {oldest_key} (size: {old_size} bytes)")
        
        # Insertar nuevo
        self.cache[key] = value
        self.total_size += new_size
        logger.debug(f"Caché: stored {key} ({new_size} bytes, total: {self.total_size / 1024 / 1024:.1f}MB)")
    
    def clear(self):
        """Limpia la caché."""
        self.cache.clear()
        self.total_size = 0
        logger.debug("Caché: cleared")
    
    def stats(self):
        """Retorna estadísticas."""
        return {
            "docs": len(self.cache),
            "size_mb": self.total_size / 1024 / 1024,
            "max_docs": self.max_docs,
            "max_size_mb": self.max_size_bytes / 1024 / 1024,
        }

__all__ = ["LRUCache"]