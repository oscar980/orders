#!/bin/bash
echo "=== GENERANDO VISUALIZACIONES ==="
source venv/bin/activate
echo "================================"
python scripts/visualize.py --output-dir output --save-dir output/visualizations

