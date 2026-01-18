"""
Módulo para manejar conexiones y operaciones con bases de datos.
Soporta CSV (modo local) y MSSQL (opcional).
"""
import logging
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Cliente para interactuar con fuentes de datos SQL/CSV."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Inicializa el cliente de base de datos.
        
        Args:
            connection_string: String de conexión para MSSQL (opcional)
        """
        self.connection_string = connection_string
    
    def load_users(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Carga usuarios desde CSV o MSSQL.
        
        Args:
            file_path: Ruta al archivo CSV (si se usa modo local)
        
        Returns:
            DataFrame con usuarios
        """
        if file_path:
            return self._load_from_csv(file_path, 'users')
        
        if self.connection_string:
            return self._load_from_mssql('users')
        
        raise ValueError("Se requiere file_path o connection_string")
    
    def load_products(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Carga productos desde CSV o MSSQL.
        
        Args:
            file_path: Ruta al archivo CSV (si se usa modo local)
        
        Returns:
            DataFrame con productos
        """
        if file_path:
            return self._load_from_csv(file_path, 'products')
        
        if self.connection_string:
            return self._load_from_mssql('products')
        
        raise ValueError("Se requiere file_path o connection_string")
    
    def _load_from_csv(self, file_path: str, table_name: str) -> pd.DataFrame:
        """Carga datos desde archivo CSV."""
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
            
            df = pd.read_csv(path)
            logger.info(f"Cargados {len(df)} registros de {table_name} desde CSV")
            return df
        
        except Exception as e:
            logger.error(f"Error cargando {table_name} desde CSV: {e}")
            raise
    
    def _load_from_mssql(self, table_name: str) -> pd.DataFrame:
        """Carga datos desde MSSQL (requiere pyodbc)."""
        try:
            import pyodbc
            
            conn = pyodbc.connect(self.connection_string)
            query = f"SELECT * FROM dbo.{table_name}"
            df = pd.read_sql(query, conn)
            conn.close()
            
            logger.info(f"Cargados {len(df)} registros de {table_name} desde MSSQL")
            return df
        
        except ImportError:
            logger.error("pyodbc no está instalado. Instala con: pip install pyodbc")
            raise
        except Exception as e:
            logger.error(f"Error cargando {table_name} desde MSSQL: {e}")
            raise

