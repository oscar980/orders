#!/bin/bash

echo "=== EJECUTANDO PIPELINE ETL (INCREMENTAL) ==="
source venv/bin/activate
echo "================================"
python -m src.etl_job --input-dir sample_data --output-dir output --since 2025-08-20

# Cambiar fecha según necesidad:
# python -m src.etl_job --input-dir sample_data --output-dir output --since 2025-08-21

