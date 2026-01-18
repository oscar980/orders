"""
Módulo de transformaciones de datos.
Convierte datos raw en modelos dimensionales (dim_user, dim_product, fact_order).
"""
import logging
import pandas as pd
import duckdb
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class DataTransformer:
    """Clase para realizar transformaciones de datos."""
    
    def __init__(self):
        """Inicializa el transformador."""
        self.conn = duckdb.connect()
    
    def transform_orders(
        self,
        orders: List[Dict[str, Any]],
        users_df: pd.DataFrame,
        products_df: pd.DataFrame,
        since_date: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Transforma órdenes raw en modelos dimensionales.
        
        Args:
            orders: Lista de órdenes desde la API
            users_df: DataFrame de usuarios
            products_df: DataFrame de productos
            since_date: Fecha mínima para filtrar (formato YYYY-MM-DD)
        
        Returns:
            Dict con dim_user, dim_product, fact_order
        """
        # Convertir órdenes a DataFrame
        orders_df = self._normalize_orders(orders, since_date)
        
        if orders_df.empty:
            logger.warning("No hay órdenes para procesar después del filtrado")
            return {
                'dim_user': pd.DataFrame(),
                'dim_product': pd.DataFrame(),
                'fact_order': pd.DataFrame()
            }
        
        # Deduplicar órdenes 
        orders_df = self._deduplicate_orders(orders_df)
        
        # Expandir items de órdenes
        fact_order_items = self._expand_order_items(orders_df)
        
        # Crear dimensiones
        dim_user = self._create_dim_user(orders_df, users_df)
        dim_product = self._create_dim_product(fact_order_items, products_df)
        fact_order = self._create_fact_order(orders_df, fact_order_items)
        
        return {
            'dim_user': dim_user,
            'dim_product': dim_product,
            'fact_order': fact_order
        }
    
    def _normalize_orders(
        self,
        orders: List[Dict[str, Any]],
        since_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Normaliza y valida órdenes."""
        normalized = []
        
        for order in orders:
            try:
                # Validar campos requeridos
                if not order.get('order_id') or not order.get('user_id'):
                    logger.warning(f"Orden sin order_id o user_id: {order.get('order_id')}")
                    continue
                
                # Validar created_at
                created_at = order.get('created_at')
                if not created_at:
                    logger.warning(f"Orden {order.get('order_id')} sin created_at, omitiendo")
                    continue
                
                # Parsear fecha
                try:
                    dt = pd.to_datetime(created_at, utc=True)
                except:
                    logger.warning(f"Fecha inválida en orden {order.get('order_id')}: {created_at}")
                    continue
                
                # Filtrar por fecha si se especifica
                if since_date:
                    since_dt = pd.to_datetime(since_date, utc=True)
                    if dt < since_dt:
                        continue
                
                # Validar items
                items = order.get('items', [])
                if not items:
                    logger.warning(f"Orden {order.get('order_id')} sin items, omitiendo")
                    continue
                
                # Calcular amount si no está presente o es inválido
                amount = order.get('amount')
                if amount is None or pd.isna(amount):
                    # Calcular desde items
                    total = sum(
                        item.get('qty', 0) * (item.get('price') or 0)
                        for item in items
                    )
                    amount = total if total > 0 else None
                
                normalized.append({
                    'order_id': order['order_id'],
                    'user_id': order['user_id'],
                    'amount': amount,
                    'currency': order.get('currency', 'USD'),
                    'created_at': dt,
                    'items': items,
                    'metadata': order.get('metadata', {})
                })
            
            except Exception as e:
                logger.error(f"Error normalizando orden {order.get('order_id')}: {e}")
                continue
        
        if not normalized:
            return pd.DataFrame()
        
        df = pd.DataFrame(normalized)
        logger.info(f"Normalizadas {len(df)} órdenes válidas de {len(orders)} totales")
        return df
    
    def _deduplicate_orders(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Deduplica órdenes manteniendo la más reciente por order_id."""
        if orders_df.empty:
            return orders_df
        
        original_count = len(orders_df)
        
        # Ordenar por created_at descendente y mantener el primero de cada order_id
        orders_df = orders_df.sort_values('created_at', ascending=False)
        orders_df = orders_df.drop_duplicates(subset=['order_id'], keep='first')
        
        duplicates_removed = original_count - len(orders_df)
        if duplicates_removed > 0:
            logger.info(f"Removidas {duplicates_removed} órdenes duplicadas")
        
        return orders_df.reset_index(drop=True)
    
    def _expand_order_items(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Expande items de órdenes en filas individuales."""
        expanded = []
        
        for _, order in orders_df.iterrows():
            items = order['items']
            for item in items:
                # Validar item
                if not item.get('sku'):
                    continue
                
                price = item.get('price')
                if price is None:
                    logger.warning(f"Item sin price en orden {order['order_id']}, usando 0")
                    price = 0.0
                
                expanded.append({
                    'order_id': order['order_id'],
                    'sku': item['sku'],
                    'qty': item.get('qty', 0),
                    'price': price,
                    'created_at': order['created_at'],
                    'user_id': order['user_id']
                })
        
        if not expanded:
            return pd.DataFrame()
        
        return pd.DataFrame(expanded)
    
    def _create_dim_user(
        self,
        orders_df: pd.DataFrame,
        users_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Crea dimensión de usuarios."""
        # Obtener usuarios únicos de órdenes
        order_users = orders_df[['user_id']].drop_duplicates()
        
        # Hacer merge con usuarios existentes
        dim_user = order_users.merge(
            users_df,
            on='user_id',
            how='left'
        )
        
        # Rellenar valores faltantes
        dim_user['email'] = dim_user['email'].fillna('unknown@example.com')
        dim_user['created_at'] = pd.to_datetime(dim_user['created_at'], errors='coerce')
        dim_user['country'] = dim_user['country'].fillna('UNKNOWN')
        
        # Agregar campos calculados (primera fecha de orden por usuario)
        first_orders = orders_df.groupby('user_id')['created_at'].min().reset_index()
        first_orders.columns = ['user_id', 'first_order_date']
        dim_user = dim_user.merge(first_orders, on='user_id', how='left')
        
        logger.info(f"Creada dim_user con {len(dim_user)} usuarios")
        return dim_user[['user_id', 'email', 'created_at', 'country', 'first_order_date']]
    
    def _create_dim_product(
        self,
        fact_order_items: pd.DataFrame,
        products_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Crea dimensión de productos."""
        if fact_order_items.empty:
            return pd.DataFrame()
        
        # Obtener productos únicos de órdenes
        order_products = fact_order_items[['sku']].drop_duplicates()
        
        # Hacer merge con productos existentes
        dim_product = order_products.merge(
            products_df,
            on='sku',
            how='left'
        )
        
        # Rellenar valores faltantes
        dim_product['name'] = dim_product['name'].fillna('Unknown Product')
        dim_product['category'] = dim_product['category'].fillna('UNKNOWN')
        dim_product['price'] = pd.to_numeric(dim_product['price'], errors='coerce').fillna(0.0)
        
        logger.info(f"Creada dim_product con {len(dim_product)} productos")
        return dim_product[['sku', 'name', 'category', 'price']]
    
    def _create_fact_order(
        self,
        orders_df: pd.DataFrame,
        fact_order_items: pd.DataFrame
    ) -> pd.DataFrame:
        """Crea tabla de hechos de órdenes."""
        if fact_order_items.empty:
            return pd.DataFrame()
        
        # Agregar información de la orden
        fact_order = fact_order_items.merge(
            orders_df[['order_id', 'amount', 'currency', 'created_at', 'metadata']],
            on=['order_id', 'created_at'],
            how='left'
        )
        
        # Calcular campos derivados
        fact_order['line_total'] = fact_order['qty'] * fact_order['price']
        fact_order['order_date'] = pd.to_datetime(fact_order['created_at']).dt.date
        fact_order['order_year'] = pd.to_datetime(fact_order['created_at']).dt.year
        fact_order['order_month'] = pd.to_datetime(fact_order['created_at']).dt.month
        fact_order['order_day'] = pd.to_datetime(fact_order['created_at']).dt.day
        
        # Extraer promo de metadata
        fact_order['promo_code'] = fact_order['metadata'].apply(
            lambda x: x.get('promo') if isinstance(x, dict) else None
        )
        
        logger.info(f"Creada fact_order con {len(fact_order)} registros")
        return fact_order[[
            'order_id',
            'user_id',
            'sku',
            'qty',
            'price',
            'line_total',
            'amount',
            'currency',
            'order_date',
            'order_year',
            'order_month',
            'order_day',
            'created_at',
            'promo_code'
        ]]
    
    def close(self):
        """Cierra la conexión de DuckDB."""
        if self.conn:
            self.conn.close()

