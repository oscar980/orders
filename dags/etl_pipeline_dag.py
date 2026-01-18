"""
DAG de Airflow para el pipeline ETL.
Cada método del ETLJob se convierte en una tarea de Airflow.
"""
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Importar las clases del proyecto
import sys
sys.path.append(str(Path(__file__).parent.parent))

from src.api_client import APIClient
from src.db import DatabaseClient
from src.transforms import DataTransformer
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

# Configuración del DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_orders_pipeline',
    default_args=default_args,
    description='Pipeline ETL para procesar órdenes',
    schedule_interval=timedelta(hours=1),  # Ejecutar cada hora
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'orders', 'data-pipeline'],
)

# Variables de configuración 
INPUT_DIR = '/path/to/sample_data'
OUTPUT_DIR = '/path/to/output'


# ============================================================================
# TAREAS DE EXTRACCIÓN
# ============================================================================

def extract_orders(**context):
    """Extrae órdenes desde la API."""
    api_client = APIClient()
    orders_file = Path(INPUT_DIR) / 'api_orders.json'
    orders = api_client.fetch_orders(file_path=str(orders_file))
    
    # Guardar en XCom para pasar a otras tareas
    context['ti'].xcom_push(key='orders', value=orders)
    return len(orders)


def extract_users(**context):
    """Extrae usuarios desde CSV."""
    db_client = DatabaseClient()
    users_file = Path(INPUT_DIR) / 'users.csv'
    users_df = db_client.load_users(file_path=str(users_file))
    
    # Guardar como JSON serializable (o usar Parquet en S3/XCom)
    users_dict = users_df.to_dict('records')
    context['ti'].xcom_push(key='users', value=users_dict)
    context['ti'].xcom_push(key='users_df_path', value=str(users_file))
    return len(users_df)


def extract_products(**context):
    """Extrae productos desde CSV."""
    db_client = DatabaseClient()
    products_file = Path(INPUT_DIR) / 'products.csv'
    products_df = db_client.load_products(file_path=str(products_file))
    
    products_dict = products_df.to_dict('records')
    context['ti'].xcom_push(key='products', value=products_dict)
    context['ti'].xcom_push(key='products_df_path', value=str(products_file))
    return len(products_df)


# Tareas de extracción (se ejecutan en paralelo)
task_extract_orders = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

task_extract_users = PythonOperator(
    task_id='extract_users',
    python_callable=extract_users,
    dag=dag,
)

task_extract_products = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products,
    dag=dag,
)


# ============================================================================
# TAREA: GUARDAR RAW
# ============================================================================

def save_raw_data(**context):
    """Guarda datos raw (copia de entrada)."""
    # Obtener datos de las tareas anteriores
    orders = context['ti'].xcom_pull(task_ids='extract_orders', key='orders')
    
    # Guardar raw
    output_dir = Path(OUTPUT_DIR) / 'raw'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    raw_file = output_dir / f'orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(orders, f, indent=2, default=str)
    
    return str(raw_file)


task_save_raw = PythonOperator(
    task_id='save_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)


# ============================================================================
# TAREA: DETERMINAR FECHA DE FILTRADO
# ============================================================================

def get_filter_date(**context):
    """Determina la fecha de filtrado basado en parametros del DAG."""
    # Leer parametros del DAG run (se envian al triggerar el DAG)
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    # Si se envia 'last_processed': True, usar el estado
    use_last_processed = conf.get('last_processed', False)
    
    if use_last_processed:
        state_file = Path(OUTPUT_DIR) / '.etl_state.json'
        
        if state_file.exists():
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    last_date = state.get('last_processed_date')
                    if last_date:
                        context['ti'].xcom_push(key='filter_date', value=last_date)
                        logger.info(f"Procesando desde ultima fecha: {last_date}")
                        return last_date
            except Exception as e:
                logger.warning(f"Error leyendo estado: {e}")
        
        # Si no hay estado pero se pidio last_processed, procesar todo
        logger.warning("Se pidio last_processed pero no hay estado, procesando todo")
        context['ti'].xcom_push(key='filter_date', value=None)
        return None
    
    # Si se envia 'since': fecha, usar esa fecha
    since_date = conf.get('since')
    if since_date:
        context['ti'].xcom_push(key='filter_date', value=since_date)
        logger.info(f"Procesando desde fecha: {since_date}")
        return since_date
    
    # Si no se envia nada, procesar todo
    context['ti'].xcom_push(key='filter_date', value=None)
    logger.info("No se especifico filtro, procesando todo")
    return None


task_get_filter_date = PythonOperator(
    task_id='get_filter_date',
    python_callable=get_filter_date,
    dag=dag,
)


# ============================================================================
# TAREA: TRANSFORMACIÓN
# ============================================================================

def transform_data(**context):
    """Transforma datos a modelos dimensionales."""
    # Obtener datos de las tareas anteriores
    orders = context['ti'].xcom_pull(task_ids='extract_orders', key='orders')
    users_path = context['ti'].xcom_pull(task_ids='extract_users', key='users_df_path')
    products_path = context['ti'].xcom_pull(task_ids='extract_products', key='products_df_path')
    filter_date = context['ti'].xcom_pull(task_ids='get_filter_date', key='filter_date')
    
    # Cargar DataFrames
    users_df = pd.read_csv(users_path)
    products_df = pd.read_csv(products_path)
    
    # Transformar
    transformer = DataTransformer()
    try:
        transformed = transformer.transform_orders(
            orders=orders,
            users_df=users_df,
            products_df=products_df,
            since_date=filter_date
        )
        
        # Guardar DataFrames transformados en archivos temporales
        # (En producción, podrías usar S3 o almacenamiento compartido)
        temp_dir = Path(OUTPUT_DIR) / 'temp'
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        dim_user_path = temp_dir / 'dim_user.parquet'
        dim_product_path = temp_dir / 'dim_product.parquet'
        fact_order_path = temp_dir / 'fact_order.parquet'
        
        transformed['dim_user'].to_parquet(dim_user_path, index=False)
        transformed['dim_product'].to_parquet(dim_product_path, index=False)
        transformed['fact_order'].to_parquet(fact_order_path, index=False)
        
        # Guardar paths en XCom
        context['ti'].xcom_push(key='dim_user_path', value=str(dim_user_path))
        context['ti'].xcom_push(key='dim_product_path', value=str(dim_product_path))
        context['ti'].xcom_push(key='fact_order_path', value=str(fact_order_path))
        
        return {
            'dim_user_rows': len(transformed['dim_user']),
            'dim_product_rows': len(transformed['dim_product']),
            'fact_order_rows': len(transformed['fact_order']),
        }
    finally:
        transformer.close()


task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


# ============================================================================
# TAREAS DE CARGA (se ejecutan en paralelo)
# ============================================================================

def load_dim_user(**context):
    """Carga dimensión de usuarios."""
    dim_user_path = context['ti'].xcom_pull(task_ids='transform_data', key='dim_user_path')
    dim_user_df = pd.read_parquet(dim_user_path)
    
    output_file = Path(OUTPUT_DIR) / 'curated' / 'dim_user.parquet'
    output_file.parent.mkdir(parents=True, exist_ok=True)
    dim_user_df.to_parquet(output_file, index=False)
    
    return str(output_file)


def load_dim_product(**context):
    """Carga dimensión de productos."""
    dim_product_path = context['ti'].xcom_pull(task_ids='transform_data', key='dim_product_path')
    dim_product_df = pd.read_parquet(dim_product_path)
    
    output_file = Path(OUTPUT_DIR) / 'curated' / 'dim_product.parquet'
    output_file.parent.mkdir(parents=True, exist_ok=True)
    dim_product_df.to_parquet(output_file, index=False)
    
    return str(output_file)


def load_fact_order(**context):
    """Carga tabla de hechos (particionado por fecha)."""
    fact_order_path = context['ti'].xcom_pull(task_ids='transform_data', key='fact_order_path')
    fact_order_df = pd.read_parquet(fact_order_path)
    
    # Particionar por fecha
    curated_dir = Path(OUTPUT_DIR) / 'curated' / 'fact_order'
    fact_order_df['order_date_str'] = fact_order_df['order_date'].astype(str)
    
    for date_str, group in fact_order_df.groupby('order_date_str'):
        partition_dir = curated_dir / f'order_date={date_str}'
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        partition_file = partition_dir / 'data.parquet'
        group.drop('order_date_str', axis=1).to_parquet(partition_file, index=False)
    
    # También guardar versión completa
    full_file = Path(OUTPUT_DIR) / 'curated' / 'fact_order.parquet'
    fact_order_df.drop('order_date_str', axis=1).to_parquet(full_file, index=False)
    
    return str(full_file)


task_load_dim_user = PythonOperator(
    task_id='load_dim_user',
    python_callable=load_dim_user,
    dag=dag,
)

task_load_dim_product = PythonOperator(
    task_id='load_dim_product',
    python_callable=load_dim_product,
    dag=dag,
)

task_load_fact_order = PythonOperator(
    task_id='load_fact_order',
    python_callable=load_fact_order,
    dag=dag,
)


# ============================================================================
# TAREA: ACTUALIZAR ESTADO
# ============================================================================

def update_state(**context):
    """Actualiza el archivo de estado."""
    fact_order_path = Path(OUTPUT_DIR) / 'curated' / 'fact_order.parquet'
    
    if fact_order_path.exists():
        fact_df = pd.read_parquet(fact_order_path)
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
    
    state_file = Path(OUTPUT_DIR) / '.etl_state.json'
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)
    
    return max_date_str


task_update_state = PythonOperator(
    task_id='update_state',
    python_callable=update_state,
    dag=dag,
)


# ============================================================================
# DEFINIR DEPENDENCIAS (ORDEN DE EJECUCIÓN)
# ============================================================================

# Extracción (en paralelo)
[task_extract_orders, task_extract_users, task_extract_products] >> task_save_raw

# Después de extracción, determinar filtro y transformar
task_save_raw >> task_get_filter_date >> task_transform

# Después de transformar, cargar (en paralelo)
task_transform >> [task_load_dim_user, task_load_dim_product, task_load_fact_order]

# Después de cargar, actualizar estado
[task_load_dim_user, task_load_dim_product, task_load_fact_order] >> task_update_state

