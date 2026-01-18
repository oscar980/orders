#!/bin/bash

echo "=== EJECUTANDO PIPELINE ETL (DESDE ÚLTIMO PROCESADO) ==="
source venv/bin/activate
echo "================================"
python -m src.etl_job --input-dir sample_data --output-dir output --last-processed

