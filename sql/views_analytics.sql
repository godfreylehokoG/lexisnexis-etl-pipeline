-- ============================================================
-- Analytics Views (Task 3)
-- ============================================================

-- 1. Daily metrics: date, orders_count, total_revenue, avg_order_value
CREATE OR REPLACE VIEW v_daily_metrics AS
WITH daily AS (
    SELECT
        DATE(order_ts AT TIME ZONE 'UTC') AS order_date,
        COUNT(*)                           AS orders_count,
        SUM(total_amount)                  AS total_revenue,
        AVG(total_amount)                  AS average_order_value
    FROM orders
    GROUP BY DATE(order_ts AT TIME ZONE 'UTC')
)
SELECT
    order_date,
    orders_count,
    ROUND(total_revenue, 2)        AS total_revenue,
    ROUND(average_order_value, 2)  AS average_order_value
FROM daily
ORDER BY order_date;


-- 2. Top 10 customers by lifetime spend
CREATE OR REPLACE VIEW v_top_customers_by_spend AS
WITH customer_spend AS (
    SELECT
        o.customer_id,
        c.full_name,
        c.email,
        COUNT(o.order_id)       AS total_orders,
        SUM(o.total_amount)     AS lifetime_spend,
        MAX(o.order_ts)         AS last_order_ts,
        ROW_NUMBER() OVER (ORDER BY SUM(o.total_amount) DESC) AS spend_rank
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.full_name, c.email
)
SELECT
    spend_rank,
    customer_id,
    full_name,
    email,
    total_orders,
    ROUND(lifetime_spend, 2)  AS lifetime_spend,
    last_order_ts
FROM customer_spend
WHERE spend_rank <= 10
ORDER BY spend_rank;


-- 3. Top 10 SKUs by revenue and units sold
CREATE OR REPLACE VIEW v_top_skus AS
WITH sku_metrics AS (
    SELECT
        oi.sku,
        oi.category,
        SUM(oi.quantity)                    AS total_units_sold,
        SUM(oi.quantity * oi.unit_price)    AS total_revenue,
        COUNT(DISTINCT oi.order_id)         AS order_count,
        ROW_NUMBER() OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS revenue_rank
    FROM order_items oi
    GROUP BY oi.sku, oi.category
)
SELECT
    revenue_rank,
    sku,
    category,
    total_units_sold,
    ROUND(total_revenue, 2) AS total_revenue,
    order_count
FROM sku_metrics
WHERE revenue_rank <= 10
ORDER BY revenue_rank;