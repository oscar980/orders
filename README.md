# Pipeline ETL - Prueba Técnica

Pipeline ETL reproducible y ejecutable localmente para procesar órdenes desde una API mock y fuentes SQL opcionales.

## Estructura del Proyecto

```
etl-test/
├─ README.md
├─ requirements.txt
├─ sample_data/
│ ├─ api_orders.json
│ ├─ users.csv
│ └─ products.csv
├─ src/
│ ├─ etl_job.py
│ ├─ transforms.py
│ ├─ api_client.py
│ └─ db.py
├─ sql/
│ └─ redshift-ddl.sql
├─ tests/
│ └─ test_transforms.py
├─ output/ # (resultados generados)
└─ docs/
│ ├─ design_notes.md
│ └─ AIRFLOW_INTEGRATION.md
```

## Requisitos Previos

- Python 3.9 o superior
- pip (gestor de paquetes de Python)

## Instalación

1. Crear un entorno virtual (recomendado):

```bash
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Ejecución

### Ejecución Completa (desde el inicio)

```bash
python -m src.etl_job --input-dir sample_data --output-dir output
```

### Ejecución Incremental

Para procesar solo datos desde una fecha específica:

```bash
python -m src.etl_job --input-dir sample_data --output-dir output --since 2025-08-20
```

### Ejecución con Último Procesado

Para procesar desde el último timestamp procesado (lee de un archivo de estado):

```bash
python -m src.etl_job --input-dir sample_data --output-dir output --last-processed
```

## Tests

Ejecutar todos los tests:

```bash
pytest tests/ -v
```

Ejecutar tests con cobertura:

```bash
pytest tests/ -v --cov=src --cov-report=html
```

## Salidas Generadas

- `output/raw/`: Archivos JSON originales (copia de entrada)
- `output/curated/`: Archivos Parquet/CSV particionados por fecha
  - `dim_user.parquet`
  - `dim_product.parquet`
  - `fact_order.parquet` (particionado por fecha)

## Características

-  Idempotencia: Ejecutar múltiples veces no duplica registros
-  Procesamiento incremental por fecha
-  Manejo de errores y retries en llamadas API
-  Deduplicación de registros
-  Validación y limpieza de datos
-  Particionado por fecha para optimización

## Notas

- El pipeline es idempotente usando particionado por fecha y sobrescritura de particiones
- Los datos malformados se registran en logs y se excluyen del procesamiento
- Ver `docs/design_notes.md` para decisiones de diseno y arquitectura

