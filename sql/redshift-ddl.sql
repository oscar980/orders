-- DDL para Redshift
-- Modelo dimensional: dim_user, dim_product, fact_order

-- Dimensión de Usuarios
CREATE TABLE IF NOT EXISTS dim_user (
    user_id VARCHAR(64) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP,
    country VARCHAR(8),
    first_order_date TIMESTAMP,
    PRIMARY KEY (user_id)
)
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (user_id, created_at);

-- Dimensión de Productos
CREATE TABLE IF NOT EXISTS dim_product (
    sku VARCHAR(64) NOT NULL,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(12,2),
    PRIMARY KEY (sku)
)
DISTSTYLE KEY
DISTKEY (sku)
SORTKEY (sku);

-- Tabla de Hechos de Órdenes
CREATE TABLE IF NOT EXISTS fact_order (
    order_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    qty INTEGER,
    price DECIMAL(12,2),
    line_total DECIMAL(12,2),
    amount DECIMAL(12,2),
    currency VARCHAR(8),
    order_date DATE,
    order_year INTEGER,
    order_month INTEGER,
    order_day INTEGER,
    created_at TIMESTAMP,
    promo_code VARCHAR(100),
    PRIMARY KEY (order_id, sku),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (sku) REFERENCES dim_product(sku)
)
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (order_date, order_id);

-- Ejemplo de consulta: Deduplicación de órdenes
-- Obtener la última versión de cada orden
SELECT 
    order_id,
    user_id,
    MAX(created_at) as last_updated
FROM fact_order
GROUP BY order_id, user_id;

-- Ejemplo de consulta:=========================================================
-- Ventas totales por día
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(*) as total_items,
    SUM(line_total) as total_revenue,
    AVG(amount) as avg_order_value
FROM fact_order
GROUP BY order_date
ORDER BY order_date DESC;

-- Ejemplo de consulta: Top productos por categoría
SELECT 
    dp.category,
    dp.sku,
    dp.name,
    SUM(fo.qty) as total_quantity_sold,
    SUM(fo.line_total) as total_revenue
FROM fact_order fo
JOIN dim_product dp ON fo.sku = dp.sku
GROUP BY dp.category, dp.sku, dp.name
ORDER BY dp.category, total_revenue DESC;

-- Ejemplo de consulta: Análisis de usuarios
SELECT 
    du.country,
    COUNT(DISTINCT du.user_id) as total_users,
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.line_total) as total_revenue,
    AVG(fo.amount) as avg_order_value
FROM dim_user du
LEFT JOIN fact_order fo ON du.user_id = fo.user_id
GROUP BY du.country
ORDER BY total_revenue DESC;

