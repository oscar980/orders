"""
Job principal ETL.
Orquesta la ejecución del pipeline completo: extracción, transformación y carga.
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd

from src.api_client import APIClient
from src.db import DatabaseClient
from src.transforms import DataTransformer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_job.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLJob:
    """Clase principal para ejecutar el pipeline ETL."""
    
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        since_date: Optional[str] = None,
        last_processed: bool = False
    ):
        """
        Inicializa el job ETL.
        
        Args:
            input_dir: Directorio con datos de entrada
            output_dir: Directorio para datos de salida
            since_date: Fecha mínima para procesar (YYYY-MM-DD)
            last_processed: Si True, usa el último timestamp procesado
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.since_date = since_date
        self.last_processed = last_processed
        
        # Crear directorios de salida
        self.raw_dir = self.output_dir / 'raw'
        self.curated_dir = self.output_dir / 'curated'
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
        
        # Archivo de estado para idempotencia
        self.state_file = self.output_dir / '.etl_state.json'
    
    def run(self):
        """Ejecuta el pipeline ETL completo."""
        logger.info("=" * 60)
        logger.info("Iniciando pipeline ETL")
        logger.info("=" * 60)
        
        try:
            # 1. Extracción
            orders, users_df, products_df = self._extract()
            
            # 2. Guardar raw
            self._save_raw(orders)
            
            # 3. Determinar fecha de filtrado
            filter_date = self._get_filter_date()
            
            # 4. Transformación
            transformed = self._transform(orders, users_df, products_df, filter_date)
            
            # 5. Carga (guardar curated)
            self._load(transformed)
            
            # 6. Actualizar estado
            self._update_state()
            
            logger.info("=" * 60)
            logger.info("Pipeline ETL completado exitosamente")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"Error en pipeline ETL: {e}", exc_info=True)
            raise
    
    def _extract(self):
        """Extrae datos de las fuentes."""
        logger.info("Fase 1: Extracción")
        
        # Extraer órdenes desde API mock (archivo JSON)
        api_client = APIClient()
        orders_file = self.input_dir / 'api_orders.json'
        orders = api_client.fetch_orders(file_path=str(orders_file))
        logger.info(f"Extraídas {len(orders)} órdenes desde API")
        
        # Extraer usuarios y productos desde CSV
        db_client = DatabaseClient()
        users_file = self.input_dir / 'users.csv'
        products_file = self.input_dir / 'products.csv'
        
        users_df = db_client.load_users(file_path=str(users_file))
        products_df = db_client.load_products(file_path=str(products_file))
        
        logger.info(f"Extraídos {len(users_df)} usuarios y {len(products_df)} productos")
        
        return orders, users_df, products_df
    
    def _save_raw(self, orders):
        """Guarda datos raw (copia de entrada)."""
        logger.info("Guardando datos raw...")
        
        # Guardar órdenes raw en JSON
        raw_file = self.raw_dir / f'orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(orders, f, indent=2, default=str)
        
        logger.info(f"Datos raw guardados en {raw_file}")
    
    def _get_filter_date(self) -> Optional[str]:
        """Determina la fecha de filtrado para procesamiento incremental."""
        if self.since_date:
            return self.since_date
        
        if self.last_processed and self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    last_date = state.get('last_processed_date')
                    if last_date:
                        logger.info(f"Usando última fecha procesada: {last_date}")
                        return last_date
            except Exception as e:
                logger.warning(f"Error leyendo estado: {e}")
        
        return None
    
    def _transform(
        self,
        orders,
        users_df: pd.DataFrame,
        products_df: pd.DataFrame,
        filter_date: Optional[str]
    ):
        """Transforma datos a modelos dimensionales."""
        logger.info("Fase 2: Transformación")
        
        transformer = DataTransformer()
        try:
            transformed = transformer.transform_orders(
                orders=orders,
                users_df=users_df,
                products_df=products_df,
                since_date=filter_date
            )
            return transformed
        finally:
            transformer.close()
    
    def _load(self, transformed):
        """Carga datos transformados en archivos particionados."""
        logger.info("Fase 3: Carga")
        
        dim_user = transformed['dim_user']
        dim_product = transformed['dim_product']
        fact_order = transformed['fact_order']
        
        # Guardar dimensiones (sin particionar)
        if not dim_user.empty:
            user_file = self.curated_dir / 'dim_user.parquet'
            dim_user.to_parquet(user_file, index=False, engine='pyarrow')
            logger.info(f"Guardada dim_user: {len(dim_user)} registros en {user_file}")
        
        if not dim_product.empty:
            product_file = self.curated_dir / 'dim_product.parquet'
            dim_product.to_parquet(product_file, index=False, engine='pyarrow')
            logger.info(f"Guardada dim_product: {len(dim_product)} registros en {product_file}")
        
        # Guardar fact_order particionado por fecha
        if not fact_order.empty:
            self._save_partitioned_fact_order(fact_order)
        else:
            logger.warning("fact_order está vacío, no se guardará nada")
    
    def _save_partitioned_fact_order(self, fact_order: pd.DataFrame):
        """
        Guarda fact_order particionado por fecha.
        Implementa idempotencia sobrescribiendo particiones completas.
        """
        logger.info("Guardando fact_order particionado por fecha...")
        
        # Agrupar por fecha
        fact_order['order_date_str'] = fact_order['order_date'].astype(str)
        
        for date_str, group in fact_order.groupby('order_date_str'):
            # Crear directorio de partición
            partition_dir = self.curated_dir / 'fact_order' / f'order_date={date_str}'
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            # Guardar partición (sobrescribe si existe - idempotencia)
            partition_file = partition_dir / 'data.parquet'
            group.drop('order_date_str', axis=1).to_parquet(
                partition_file,
                index=False,
                engine='pyarrow'
            )
            
            logger.info(
                f"Partición {date_str}: {len(group)} registros guardados en {partition_file}"
            )
        
        # También guardar versión completa sin particionar (para referencia)
        full_file = self.curated_dir / 'fact_order.parquet'
        fact_order.drop('order_date_str', axis=1).to_parquet(
            full_file,
            index=False,
            engine='pyarrow'
        )
        logger.info(f"Versión completa guardada: {full_file}")
    
    def _update_state(self):
        """Actualiza el archivo de estado con el último timestamp procesado."""
        try:
            # Obtener la fecha máxima de las órdenes procesadas
            fact_file = self.curated_dir / 'fact_order.parquet'
            if fact_file.exists():
                fact_df = pd.read_parquet(fact_file)
                if not fact_df.empty and 'created_at' in fact_df.columns:
                    max_date = pd.to_datetime(fact_df['created_at']).max()
                    max_date_str = max_date.strftime('%Y-%m-%d')
                else:
                    max_date_str = datetime.now().strftime('%Y-%m-%d')
            else:
                max_date_str = datetime.now().strftime('%Y-%m-%d')
            
            state = {
                'last_processed_date': max_date_str,
                'last_run': datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.info(f"Estado actualizado: última fecha procesada = {max_date_str}")
        
        except Exception as e:
            logger.warning(f"Error actualizando estado: {e}")


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(description='Pipeline ETL para procesar órdenes')
    parser.add_argument(
        '--input-dir',
        type=str,
        default='sample_data',
        help='Directorio con datos de entrada'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Directorio para datos de salida'
    )
    parser.add_argument(
        '--since',
        type=str,
        default=None,
        help='Fecha mínima para procesar (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--last-processed',
        action='store_true',
        help='Procesar desde la última fecha procesada'
    )
    
    args = parser.parse_args()
    
    # Validar argumentos
    if args.since and args.last_processed:
        logger.error("No se pueden usar --since y --last-processed simultáneamente")
        sys.exit(1)
    
    # Ejecutar job
    job = ETLJob(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        since_date=args.since,
        last_processed=args.last_processed
    )
    
    job.run()


if __name__ == '__main__':
    main()

