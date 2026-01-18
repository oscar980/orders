# Integración con Apache Airflow

## ¿Por qué usar Airflow?

Airflow permite:
- ✅ **Orquestación visual**: Ver el pipeline como un DAG (grafo dirigido acíclico)
- ✅ **Ejecución programada**: Ejecutar automáticamente cada hora/día
- ✅ **Retries automáticos**: Si una tarea falla, reintenta automáticamente
- ✅ **Monitoreo**: UI web para ver estado, logs, y métricas
- ✅ **Paralelización**: Ejecutar tareas independientes en paralelo
- ✅ **Dependencias**: Definir qué tareas dependen de otras
- ✅ **Alertas**: Notificaciones por email/Slack cuando falla algo

## Comparación: Código Actual vs Airflow

### Código Actual (Secuencial)

```python
# etl_job.py - run()
def run(self):
    # 1. Extracción (secuencial)
    orders, users_df, products_df = self._extract()
    
    # 2. Guardar raw
    self._save_raw(orders)
    
    # 3. Transformación
    transformed = self._transform(...)
    
    # 4. Carga (secuencial)
    self._load(transformed)
    
    # 5. Actualizar estado
    self._update_state()
```

**Problemas:**
- Todo se ejecuta secuencialmente
- Si falla un paso, todo se detiene
- No hay visibilidad del progreso
- Difícil de monitorear en producción

### Con Airflow (Paralelo y Monitoreado)

```python
# dags/etl_pipeline_dag.py

# Extracción en PARALELO
[extract_orders, extract_users, extract_products] 
    ↓
save_raw 
    ↓
get_filter_date 
    ↓
transform 
    ↓
[load_dim_user, load_dim_product, load_fact_order]  # En PARALELO
    ↓
update_state
```

**Ventajas:**
- Tareas independientes se ejecutan en paralelo
- Si una tarea falla, las demás pueden continuar (dependiendo de la configuración)
- UI visual para ver el estado
- Logs separados por tarea
- Retries automáticos

## Estructura del DAG

### Tareas del DAG

1. **extract_orders** - Extrae órdenes desde API
2. **extract_users** - Extrae usuarios desde CSV
3. **extract_products** - Extrae productos desde CSV
4. **save_raw_data** - Guarda datos raw
5. **get_filter_date** - Determina fecha de filtrado
6. **transform_data** - Transforma datos
7. **load_dim_user** - Guarda dim_user
8. **load_dim_product** - Guarda dim_product
9. **load_fact_order** - Guarda fact_order (particionado)
10. **update_state** - Actualiza estado

### Dependencias (Orden de Ejecución)

```
┌─────────────────────────────────────────────────────────┐
│ EXTRACCIÓN (Paralelo)                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐│
│  │extract_orders │  │extract_users │  │extract_products││
│  └───────┬───────┘  └───────┬──────┘  └───────┬───────┘│
│          └──────────┬───────────────────────────┘        │
└─────────────────────┼───────────────────────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │  save_raw_data │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │get_filter_date │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │transform_data  │
              └───────┬───────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
        ▼             ▼             ▼
┌───────────┐ ┌───────────┐ ┌───────────┐
│load_dim_  │ │load_dim_   │ │load_fact_  │
│user       │ │product     │ │order       │
└─────┬─────┘ └─────┬──────┘ └─────┬──────┘
      └─────────────┼──────────────┘
                    │
                    ▼
            ┌───────────────┐
            │ update_state  │
            └───────────────┘
```

## Cómo Funciona XCom (Comunicación entre Tareas)

Airflow usa **XCom** para pasar datos entre tareas:

```python
# Tarea 1: Guarda datos
def extract_orders(**context):
    orders = [...]  # Lista de órdenes
    context['ti'].xcom_push(key='orders', value=orders)
    return len(orders)

# Tarea 2: Lee datos
def transform_data(**context):
    orders = context['ti'].xcom_pull(task_ids='extract_orders', key='orders')
    # Usa orders para transformar
```

**Nota:** XCom tiene límites de tamaño. Para datos grandes, usa:
- Archivos temporales en disco compartido
- S3 / GCS / Azure Blob
- Base de datos compartida

## Instalación y Configuración

### 1. Instalar Airflow

```bash
# Agregar a requirements.txt
apache-airflow==2.7.0
apache-airflow-providers-amazon  # Para S3 (opcional)
```

### 2. Configurar Airflow

```bash
# Inicializar base de datos
airflow db init

# Crear usuario admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Iniciar webserver
airflow webserver --port 8080

# Iniciar scheduler (en otra terminal)
airflow scheduler
```

### 3. Copiar DAG

```bash
# Copiar el DAG a la carpeta de Airflow
cp dags/etl_pipeline_dag.py ~/airflow/dags/
```

### 4. Acceder a UI

Abrir navegador: `http://localhost:8080`
- Usuario: `admin`
- Password: `admin`

## Variables de Airflow

En lugar de hardcodear paths, usa Variables de Airflow:

```python
from airflow.models import Variable

INPUT_DIR = Variable.get("etl_input_dir", default_var="/path/to/sample_data")
OUTPUT_DIR = Variable.get("etl_output_dir", default_var="/path/to/output")
```

Configurar en UI: Admin → Variables

## Mejoras Adicionales

### 1. Sensors (Esperar por Datos)

```python
from airflow.sensors.filesystem import FileSensor

wait_for_orders = FileSensor(
    task_id='wait_for_orders',
    filepath=f'{INPUT_DIR}/api_orders.json',
    poke_interval=60,  # Revisar cada 60 segundos
    timeout=3600,  # Timeout de 1 hora
    dag=dag,
)
```

### 2. Task Groups (Organización Visual)

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("extraction_group") as extraction:
    extract_orders = PythonOperator(...)
    extract_users = PythonOperator(...)
    extract_products = PythonOperator(...)
```

### 3. Notificaciones

```python
from airflow.operators.email import EmailOperator

send_notification = EmailOperator(
    task_id='send_notification',
    to=['data-team@example.com'],
    subject='ETL Pipeline Completed',
    html_content='Pipeline ejecutado exitosamente',
    dag=dag,
)

task_update_state >> send_notification
```

### 4. Integración con S3

```python
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator

upload_to_s3 = S3FileTransformOperator(
    task_id='upload_to_s3',
    source_s3_key='s3://bucket/raw/orders.json',
    dest_s3_key='s3://bucket/curated/dim_user.parquet',
    dag=dag,
)
```

## Ventajas de la Separación en Tareas

1. **Debugging más fácil**: Si falla `transform_data`, puedes re-ejecutar solo esa tarea
2. **Retries selectivos**: Configurar diferentes retries por tipo de tarea
3. **Monitoreo granular**: Ver qué tarea específica está tardando
4. **Paralelización**: Tareas independientes corren en paralelo
5. **Alertas específicas**: Notificar solo cuando fallan tareas críticas

## Ejemplo de Configuración de Retries

```python
# Tareas críticas: más retries
task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    retries=5,  # 5 reintentos
    retry_delay=timedelta(minutes=10),
    dag=dag,
)

# Tareas menos críticas: menos retries
task_save_raw = PythonOperator(
    task_id='save_raw_data',
    python_callable=save_raw_data,
    retries=2,  # 2 reintentos
    retry_delay=timedelta(minutes=5),
    dag=dag,
)
```

## Resumen

- ✅ **Cada método** del `ETLJob` se convierte en una **tarea de Airflow**
- ✅ Las tareas se ejecutan según **dependencias definidas**
- ✅ Tareas independientes pueden ejecutarse en **paralelo**
- ✅ Airflow maneja **retries, logging, y monitoreo** automáticamente
- ✅ UI visual para ver el estado del pipeline
- ✅ Programación automática (cada hora, día, etc.)

