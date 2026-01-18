#!/bin/bash

echo "=== EJECUTANDO TESTS ==="
source venv/bin/activate
echo "================================"
pytest tests/ -v

# Ejecutar con cobertura:
# pytest tests/ -v --cov=src --cov-report=html

# Ejecutar solo tests de transformaciones:
# pytest tests/ -v -k transform
