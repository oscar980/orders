#!/bin/bash

echo "=== CONFIGURANDO ENTORNO ==="
echo "================================"

# Crear venv si no existe
if [ ! -d "venv" ]; then
    echo "Creando entorno virtual..."
    python3 -m venv venv
fi

# Activar y instalar dependencias
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Configuración completada"
