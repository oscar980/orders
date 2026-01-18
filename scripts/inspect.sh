#!/bin/bash

echo "=== INSPECCIONANDO RESULTADOS ==="
source venv/bin/activate
echo "================================"
python -c "
import pandas as pd
from pathlib import Path

print('\n=== RESUMEN ===')
raw_count = len(list(Path('output/raw').glob('*.json'))) if Path('output/raw').exists() else 0
curated_count = len(list(Path('output/curated').glob('*.parquet'))) if Path('output/curated').exists() else 0
print(f'Archivos raw (JSON): {raw_count}')
print(f'Archivos curated (Parquet): {curated_count}')

if Path('output/curated/dim_user.parquet').exists():
    print('\n=== dim_user ===')
    df = pd.read_parquet('output/curated/dim_user.parquet')
    print(df.to_string(index=False))

if Path('output/curated/fact_order.parquet').exists():
    print('\n=== fact_order (primeras 5 filas) ===')
    df = pd.read_parquet('output/curated/fact_order.parquet')
    print(df.head(5).to_string(index=False))
    print(f'\nTotal registros: {len(df)}')
"

