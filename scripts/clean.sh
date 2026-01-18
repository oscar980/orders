#!/bin/bash

echo "=== LIMPIANDO OUTPUT ==="
echo "================================"
rm -rf output/
echo "Output limpiado"

# Limpiar también cache de Python:
# find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
# find . -type f -name "*.pyc" -delete 2>/dev/null
