-- =========================================================
-- 1. Consultar todas las órdenes con información de cliente
-- =========================================================
SELECT
    o.order_id,
    o.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    o.date,
    o.product_id,
    o.quantity
FROM processed_orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
ORDER BY o.date DESC
LIMIT 100;


-- =========================================================
-- 2. Total de productos por cliente
-- =========================================================
SELECT
    o.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.quantity) AS total_products
FROM processed_orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
GROUP BY o.customer_id, c.first_name, c.last_name
ORDER BY total_products DESC
LIMIT 50;


-- =========================================================
-- 3. Productos más vendidos
-- =========================================================
SELECT
    product_id,
    SUM(quantity) AS total_sold
FROM processed_orders
GROUP BY product_id
ORDER BY total_sold DESC
LIMIT 20;


-- =========================================================
-- 4. Pedidos por día
-- =========================================================
SELECT
    date,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_items_sold
FROM processed_orders
GROUP BY date
ORDER BY date;


-- =========================================================
-- 5. Clientes activos por país
-- =========================================================
SELECT
    c.country,
    COUNT(DISTINCT o.customer_id) AS active_customers
FROM processed_orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
GROUP BY c.country
ORDER BY active_customers DESC;