-- ============================================================
-- Data Quality Views (Task 3)
-- ============================================================

-- REQUIRED: Duplicate customers by lowercase email
CREATE OR REPLACE VIEW v_dq_duplicate_emails AS
SELECT
    LOWER(email)     AS normalized_email,
    COUNT(*)         AS duplicate_count,
    ARRAY_AGG(customer_id ORDER BY signup_date) AS customer_ids,
    ARRAY_AGG(full_name ORDER BY signup_date)   AS full_names,
    MIN(signup_date) AS earliest_signup,
    MAX(signup_date) AS latest_signup
FROM customers
GROUP BY LOWER(email)
HAVING COUNT(*) > 1;


-- REQUIRED: Orders referencing missing customers
CREATE OR REPLACE VIEW v_dq_orphaned_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_ts,
    o.status,
    o.total_amount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


-- OPTIONAL: Order items with non-positive quantities or unit prices
CREATE OR REPLACE VIEW v_dq_invalid_items AS
SELECT
    oi.order_id,
    oi.line_no,
    oi.sku,
    oi.quantity,
    oi.unit_price,
    CASE
        WHEN oi.quantity <= 0 AND oi.unit_price <= 0 THEN 'non_positive_quantity_and_price'
        WHEN oi.quantity <= 0 THEN 'non_positive_quantity'
        WHEN oi.unit_price <= 0 THEN 'non_positive_unit_price'
    END AS issue
FROM order_items oi
WHERE oi.quantity <= 0 OR oi.unit_price <= 0;


-- OPTIONAL: Orders with status outside allowed set
CREATE OR REPLACE VIEW v_dq_invalid_status AS
SELECT
    order_id,
    customer_id,
    order_ts,
    status,
    total_amount
FROM orders
WHERE status NOT IN ('placed', 'shipped', 'cancelled', 'refunded');