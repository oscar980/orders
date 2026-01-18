"""
Módulo para manejar llamadas a la API mock.
Incluye retry logic y manejo de errores.
"""
import json
import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class APIClient:
    """Cliente para interactuar con la API mock de órdenes."""
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: int = 30
    ):
        """
        Inicializa el cliente API.
        
        Args:
            base_url: URL base de la API (opcional, si es None usa archivo local)
            max_retries: Número máximo de reintentos
            retry_delay: Segundos de espera entre reintentos
            timeout: Timeout en segundos para las peticiones
        """
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
    
    def fetch_orders(
        self,
        endpoint: Optional[str] = None,
        file_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Obtiene órdenes desde la API o archivo local.
        
        Args:
            endpoint: Endpoint de la API (ej: '/api/v1/orders')
            file_path: Ruta al archivo JSON local (si se usa modo mock)
        
        Returns:
            Lista de órdenes en formato dict
        
        Raises:
            Exception: Si falla después de todos los reintentos
        """
        if file_path:
            return self._fetch_from_file(file_path)
        
        if not self.base_url or not endpoint:
            raise ValueError("Se requiere base_url y endpoint o file_path")
        
        return self._fetch_from_api(endpoint)
    
    def _fetch_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Lee órdenes desde un archivo JSON local."""
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Leídas {len(data)} órdenes desde {file_path}")
            return data
        
        except json.JSONDecodeError as e:
            logger.error(f"Error parseando JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Error leyendo archivo: {e}")
            raise
    
    def _fetch_from_api(self, endpoint: str) -> List[Dict[str, Any]]:
        """Obtiene órdenes desde la API con retry logic."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Llamando a API: {url} (intento {attempt}/{self.max_retries})")
                
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Órdenes obtenidas exitosamente: {len(data)}")
                return data
            
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout en intento {attempt}/{self.max_retries}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error en intento {attempt}/{self.max_retries}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise
        
        raise Exception(f"Falló después de {self.max_retries} intentos")

