#!/bin/bash
# Script para inspeccionar los resultados del pipeline

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_help() {
    echo "Uso: ./inspect_output.sh [OPCIONES]"
    echo ""
    echo "Opciones:"
    echo "  -h, --help              Mostrar esta ayuda"
    echo "  -o, --output-dir DIR     Directorio de salida (default: output)"
    echo "  -s, --summary           Mostrar solo resumen"
    echo "  -d, --dimensions        Mostrar solo dimensiones"
    echo "  -f, --facts             Mostrar solo facts"
    echo "  -r, --raw               Mostrar solo raw"
    echo "  -v, --venv PATH         Ruta al venv (default: venv)"
    echo ""
    echo "Ejemplos:"
    echo "  ./inspect_output.sh              # Todo"
    echo "  ./inspect_output.sh --summary    # Solo resumen"
    echo "  ./inspect_output.sh --dimensions # Solo dimensiones"
}

OUTPUT_DIR="output"
SHOW_SUMMARY=true
SHOW_DIMENSIONS=true
SHOW_FACTS=true
SHOW_RAW=true
VENV_PATH="venv"

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -s|--summary)
            SHOW_DIMENSIONS=false
            SHOW_FACTS=false
            SHOW_RAW=false
            shift
            ;;
        -d|--dimensions)
            SHOW_SUMMARY=false
            SHOW_FACTS=false
            SHOW_RAW=false
            shift
            ;;
        -f|--facts)
            SHOW_SUMMARY=false
            SHOW_DIMENSIONS=false
            SHOW_RAW=false
            shift
            ;;
        -r|--raw)
            SHOW_SUMMARY=false
            SHOW_DIMENSIONS=false
            SHOW_FACTS=false
            shift
            ;;
        -v|--venv)
            VENV_PATH="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Error: Opción desconocida: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Activar venv
if [[ -f "$VENV_PATH/bin/activate" ]]; then
    source "$VENV_PATH/bin/activate"
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Inspección de Output${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Resumen
if [[ "$SHOW_SUMMARY" == true ]]; then
    echo -e "${BLUE}=== RESUMEN ===${NC}"
    echo ""
    
    # Contar archivos
    RAW_COUNT=$(find "$OUTPUT_DIR/raw" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
    CURATED_COUNT=$(find "$OUTPUT_DIR/curated" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    PARTITIONS=$(find "$OUTPUT_DIR/curated/fact_order" -type d -name "order_date=*" 2>/dev/null | wc -l | tr -d ' ')
    
    echo "Archivos raw (JSON): $RAW_COUNT"
    echo "Archivos curated (Parquet): $CURATED_COUNT"
    echo "Particiones de fact_order: $PARTITIONS"
    echo ""
    
    # Tamaño total
    if [[ -d "$OUTPUT_DIR" ]]; then
        TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)
        echo "Tamaño total: $TOTAL_SIZE"
    fi
    echo ""
fi

# Raw
if [[ "$SHOW_RAW" == true && -d "$OUTPUT_DIR/raw" ]]; then
    echo -e "${BLUE}=== RAW (JSON) ===${NC}"
    ls -lh "$OUTPUT_DIR/raw/"*.json 2>/dev/null | awk '{print $9, "(" $5 ")"}'
    echo ""
fi

# Dimensiones
if [[ "$SHOW_DIMENSIONS" == true && -d "$OUTPUT_DIR/curated" ]]; then
    echo -e "${BLUE}=== DIMENSIONES ===${NC}"
    
    if [[ -f "$OUTPUT_DIR/curated/dim_user.parquet" ]]; then
        python3 -c "
import pandas as pd
df = pd.read_parquet('$OUTPUT_DIR/curated/dim_user.parquet')
print('dim_user: {} registros'.format(len(df)))
print(df.to_string(index=False))
" 2>/dev/null || echo "Error leyendo dim_user.parquet"
        echo ""
    fi
    
    if [[ -f "$OUTPUT_DIR/curated/dim_product.parquet" ]]; then
        python3 -c "
import pandas as pd
df = pd.read_parquet('$OUTPUT_DIR/curated/dim_product.parquet')
print('dim_product: {} registros'.format(len(df)))
print(df.to_string(index=False))
" 2>/dev/null || echo "Error leyendo dim_product.parquet"
        echo ""
    fi
fi

# Facts
if [[ "$SHOW_FACTS" == true && -f "$OUTPUT_DIR/curated/fact_order.parquet" ]]; then
    echo -e "${BLUE}=== FACT ORDER ===${NC}"
    
    python3 -c "
import pandas as pd
df = pd.read_parquet('$OUTPUT_DIR/curated/fact_order.parquet')
print('Total registros: {}'.format(len(df)))
print('')
print('Primeras 10 filas:')
print(df.head(10).to_string(index=False))
print('')
print('Resumen por fecha:')
print(df.groupby('order_date').size().to_string())
" 2>/dev/null || echo "Error leyendo fact_order.parquet"
    echo ""
    
    # Particiones
    if [[ -d "$OUTPUT_DIR/curated/fact_order" ]]; then
        echo -e "${BLUE}Particiones:${NC}"
        for partition in "$OUTPUT_DIR/curated/fact_order"/order_date=*/; do
            if [[ -d "$partition" ]]; then
                date=$(basename "$partition" | cut -d= -f2)
                count=$(python3 -c "import pandas as pd; df = pd.read_parquet('$partition/data.parquet'); print(len(df))" 2>/dev/null || echo "0")
                echo "  $date: $count registros"
            fi
        done
    fi
    echo ""
fi

# Estado
if [[ -f "$OUTPUT_DIR/.etl_state.json" ]]; then
    echo -e "${BLUE}=== ESTADO ===${NC}"
    cat "$OUTPUT_DIR/.etl_state.json" | python3 -m json.tool 2>/dev/null || cat "$OUTPUT_DIR/.etl_state.json"
    echo ""
fi

echo -e "${GREEN}Inspección completada${NC}"

