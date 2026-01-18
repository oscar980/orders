# Scripts de Utilidad

Scripts bash para facilitar la ejecución y gestión del pipeline ETL.

## Scripts Disponibles

### 1. `setup.sh` - Configuración Inicial

Configura el entorno del proyecto (crea venv, instala dependencias, etc.)

```bash
./scripts/setup.sh
```

**Qué hace:**
- Crea entorno virtual si no existe
- Instala/actualiza dependencias
- Crea directorios necesarios
- Da permisos de ejecución a scripts

---

### 2. `run_etl.sh` - Ejecutar Pipeline ETL

Ejecuta el pipeline ETL con múltiples opciones.

```bash
# Ejecución básica
./scripts/run_etl.sh

# Con opciones
./scripts/run_etl.sh --input-dir sample_data --output-dir output

# Procesamiento incremental desde fecha
./scripts/run_etl.sh --since 2025-08-20

# Continuar desde último procesado
./scripts/run_etl.sh --last-processed

# Limpiar y ejecutar
./scripts/run_etl.sh --clean

# Ejecutar y luego tests
./scripts/run_etl.sh --test

# Ver qué se ejecutaría (sin ejecutar)
./scripts/run_etl.sh --dry-run
```

**Opciones:**
- `-h, --help` - Mostrar ayuda
- `-i, --input-dir DIR` - Directorio de entrada (default: sample_data)
- `-o, --output-dir DIR` - Directorio de salida (default: output)
- `-s, --since DATE` - Procesar desde fecha (YYYY-MM-DD)
- `-l, --last-processed` - Continuar desde última fecha procesada
- `-v, --venv PATH` - Ruta al venv (default: venv)
- `-c, --clean` - Limpiar output antes de ejecutar
- `-t, --test` - Ejecutar tests después del ETL
- `-d, --dry-run` - Mostrar comando sin ejecutar

**Ejemplos combinados:**
```bash
# Limpiar, ejecutar y hacer tests
./scripts/run_etl.sh --clean --test

# Ejecutar desde fecha específica con venv personalizado
./scripts/run_etl.sh --since 2025-08-21 --venv my_venv
```

---

### 3. `run_tests.sh` - Ejecutar Tests

Ejecuta los tests con diferentes opciones.

```bash
# Todos los tests
./scripts/run_tests.sh

# Con cobertura
./scripts/run_tests.sh --coverage

# Solo tests que contengan "transform"
./scripts/run_tests.sh --keyword transform

# Solo un archivo de test
./scripts/run_tests.sh --file test_transforms

# Modo watch (re-ejecuta al cambiar archivos)
./scripts/run_tests.sh --watch
```

**Opciones:**
- `-h, --help` - Mostrar ayuda
- `-v, --venv PATH` - Ruta al venv (default: venv)
- `-c, --coverage` - Ejecutar con cobertura
- `-w, --watch` - Modo watch (re-ejecutar al cambiar archivos)
- `-k, --keyword KEYWORD` - Ejecutar solo tests que contengan KEYWORD
- `--verbose` - Modo verbose
- `-f, --file FILE` - Ejecutar solo un archivo de test

---

### 4. `clean.sh` - Limpiar Archivos

Limpia archivos generados (output, cache, logs).

```bash
# Limpiar solo output (default)
./scripts/clean.sh

# Limpiar todo
./scripts/clean.sh --all

# Limpiar solo cache de Python
./scripts/clean.sh --cache

# Limpiar solo logs
./scripts/clean.sh --logs

# Ver qué se eliminaría sin eliminar
./scripts/clean.sh --dry-run
```

**Opciones:**
- `-h, --help` - Mostrar ayuda
- `-o, --output` - Limpiar solo output/
- `-c, --cache` - Limpiar solo __pycache__ y .pyc
- `-l, --logs` - Limpiar solo logs
- `-a, --all` - Limpiar todo (output, cache, logs)
- `-d, --dry-run` - Mostrar qué se eliminaría sin eliminar

---

### 5. `inspect_output.sh` - Inspeccionar Resultados

Inspecciona y muestra los resultados del pipeline.

```bash
# Ver todo
./scripts/inspect_output.sh

# Solo resumen
./scripts/inspect_output.sh --summary

# Solo dimensiones
./scripts/inspect_output.sh --dimensions

# Solo facts
./scripts/inspect_output.sh --facts

# Solo raw
./scripts/inspect_output.sh --raw
```

**Opciones:**
- `-h, --help` - Mostrar ayuda
- `-o, --output-dir DIR` - Directorio de salida (default: output)
- `-s, --summary` - Mostrar solo resumen
- `-d, --dimensions` - Mostrar solo dimensiones
- `-f, --facts` - Mostrar solo facts
- `-r, --raw` - Mostrar solo raw
- `-v, --venv PATH` - Ruta al venv (default: venv)

---

## Flujo de Trabajo Típico

### Primera vez (setup)
```bash
# 1. Configurar entorno
./scripts/setup.sh

# 2. Ejecutar pipeline
./scripts/run_etl.sh

# 3. Ver resultados
./scripts/inspect_output.sh
```

### Desarrollo iterativo
```bash
# 1. Limpiar y ejecutar
./scripts/run_etl.sh --clean

# 2. Ejecutar tests
./scripts/run_tests.sh --coverage

# 3. Inspeccionar resultados
./scripts/inspect_output.sh --summary
```

### Procesamiento incremental
```bash
# Ejecutar desde fecha específica
./scripts/run_etl.sh --since 2025-08-21

# O continuar desde último procesado
./scripts/run_etl.sh --last-processed
```

---

## Notas

- Todos los scripts requieren permisos de ejecución (ya están configurados)
- Los scripts activan automáticamente el venv si existe
- Usan colores para mejor legibilidad
- Incluyen validación de argumentos y mensajes de error claros

## Troubleshooting

**Error: "Permission denied"**
```bash
chmod +x scripts/*.sh
```

**Error: "venv no encontrado"**
```bash
./scripts/setup.sh
```

**Error: "Dependencias no instaladas"**
```bash
source venv/bin/activate
pip install -r requirements.txt
```

