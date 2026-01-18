# Notas de Diseno - Pipeline ETL

## Decisiones de Arquitectura

### Stack Tecnologico

**Eleccion: pandas + DuckDB**

Para un pipeline local y reproducible, pandas + DuckDB ofrece(es tambien una propuesta valida por parte del ejercicio):
- Simplicidad de instalacion y uso
- Buen rendimiento para datasets medianos (< 100GB)
- Soporte nativo para Parquet y particionado
- Compatibilidad con SQL estandar
- Menor overhead que PySpark para este caso de uso

**Alternativa considerada (PySpark)**: Mas adecuado para datasets muy grandes (> 100GB) o procesamiento distribuido, pero innecesario para este ejercicio.

### Particionado

**Estrategia**: Particionado por fecha (`order_date`) en formato Hive-style (`order_date=YYYY-MM-DD`)

Ventajas:
- Permite procesamiento incremental eficiente
- Compatible con COPY de Redshift
- Facilita queries por rango de fechas
- Soporta idempotencia a nivel de particion

Implementacion: Cada particion se guarda como `fact_order/order_date=YYYY-MM-DD/data.parquet`

**Carga a Redshift**: 
```sql
COPY fact_order FROM 's3://bucket/fact_order/order_date=2025-08-20/'
IAM_ROLE 'arn:aws:iam::...'
FORMAT PARQUET
PARTITION BY (order_date);
```

### Idempotencia

**Tecnica**: Sobrescritura de particiones completas

Como funciona:
1. El pipeline procesa todas las ordenes de una fecha
2. Sobrescribe la particion completa de esa fecha
3. Ejecutar multiples veces no duplica datos (reprocesa y reemplaza)

Ventajas:
- Simple de implementar
- Garantiza consistencia por particion
- Permite reprocesamiento seguro

Limitacion: No soporta actualizaciones parciales dentro de una particion (requeriria merge/upsert mas complejo)

### Claves Primarias y Modelado

**dim_user**:
- PK: `user_id` (VARCHAR)
- SCD Type 1 (sobrescribe valores historicos)
- Justificacion: Datos de usuario relativamente estaticos

**dim_product**:
- PK: `sku` (VARCHAR)
- SCD Type 1
- Justificacion: Catalogo de productos, cambios infrecuentes

**fact_order**:
- PK compuesta: `(order_id, sku)`
- Granularidad: Una fila por item de orden
- Justificacion: Permite analisis a nivel de linea de orden

**Redshift Distribution Keys**:
- `dim_user`: DISTKEY en `user_id` (tabla pequena, distribucion uniforme)
- `dim_product`: DISTKEY en `sku` (tabla pequena)
- `fact_order`: DISTKEY en `user_id` (tabla grande, co-localiza con dim_user para joins)

### Procesamiento Incremental

**Parametros**:
- `--since YYYY-MM-DD`: Procesa desde fecha especifica
- `--last-processed`: Lee ultima fecha procesada de `.etl_state.json`

**Implementacion**:
1. Filtra ordenes por `created_at >= since_date`
2. Procesa solo ordenes nuevas/modificadas
3. Actualiza estado con fecha maxima procesada

Ventaja: Reduce tiempo de procesamiento en ejecuciones incrementales.

### Manejo de Errores y Datos Malformados

**Estrategia**: Validacion y logging exhaustivo

1. **Validacion de campos requeridos**: Rechaza ordenes sin `order_id`, `user_id`, `created_at` o `items`
2. **Datos faltantes**: 
   - `amount` nulo: Calcula desde items
   - `price` nulo en items: Usa 0.0 y registra warning
3. **Deduplicacion**: Mantiene la version mas reciente por `order_id`
4. **Logging**: Todos los rechazos y warnings se registran en `etl_job.log`

**Retries en API**:
- 3 intentos con backoff exponencial
- Timeout de 30 segundos
- Logs de cada intento

### Monitorizacion y Alertas (Produccion)

**Logs**:
- Archivo: `etl_job.log` (rotacion diaria recomendada)
- Niveles: INFO (progreso), WARNING (datos rechazados), ERROR (fallos criticos)

**Metricas clave**:
- Numero de ordenes procesadas
- Numero de ordenes rechazadas (y razon)
- Tiempo de ejecucion por fase
- Tamaño de archivos generados

**Alertas recomendadas**:
- Tasa de rechazo > 10%
- Tiempo de ejecucion > umbral esperado
- Errores en llamadas API
- Archivos de salida vacios

### Carga a Redshift

**Metodo**: COPY desde S3

1. Subir Parquet particionados a S3
2. Ejecutar COPY con particiones especificas
3. Validar conteos y checksums

**Ejemplo**:
```sql
COPY fact_order 
FROM 's3://bucket/fact_order/order_date=2025-08-20/'
IAM_ROLE 'arn:aws:iam::account:role/redshift-role'
FORMAT PARQUET
PARTITION BY (order_date);
```

**Optimizaciones**:
- Usar DISTKEY y SORTKEY segun patrones de consulta
- Vacuum y Analyze despues de cargas grandes
- Considerar compresion de columnas

### Trade-offs y Limitaciones

**Trade-offs elegidos**:
- Simplicidad sobre escalabilidad horizontal (pandas vs PySpark)
- Idempotencia por particion sobre actualizaciones granulares
- Validacion estricta sobre tolerancia a datos malformados

**Limitaciones conocidas**:
- No escala a datasets > 100GB sin optimizaciones
- No soporta actualizaciones parciales dentro de particiones
- Requiere reprocesamiento completo de particion para corregir errores
- No maneja SCD Type 2 (solo Type 1)

### Mejoras Futuras

**Corto plazo**:
- Agregar validacion de PKs faltantes (left anti join)
- Implementar alineacion de schemas dinamica
- Agregar metricas de calidad de datos

**Mediano plazo**:
- Migrar a PySpark si el volumen crece
- Implementar SCD Type 2 para dimensiones
- Agregar tests de integracion end-to-end

**Largo plazo**:
- Integracion con Airflow para orquestacion
- Carga automatica a Redshift desde S3
- Dashboard de monitoreo y alertas

