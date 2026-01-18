#!/bin/bash

echo "=== EJECUTANDO PIPELINE ETL ==="
source venv/bin/activate
echo "================================"
python -m src.etl_job --input-dir sample_data --output-dir output

# Ejecutar con procesamiento incremental desde fecha:
# python -m src.etl_job --input-dir sample_data --output-dir output --since 2025-08-20

# Continuar desde último procesado:
# python -m src.etl_job --input-dir sample_data --output-dir output --last-processed
