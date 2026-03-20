-- ============================================================
-- Schema: Orders Data Pipeline
-- Idempotent: safe to run multiple times (DROP IF EXISTS)
-- ============================================================

-- Drop in reverse dependency order
DROP VIEW IF EXISTS v_dq_invalid_status CASCADE;
DROP VIEW IF EXISTS v_dq_invalid_items CASCADE;
DROP VIEW IF EXISTS v_dq_orphaned_orders CASCADE;
DROP VIEW IF EXISTS v_dq_duplicate_emails CASCADE;
DROP VIEW IF EXISTS v_top_skus CASCADE;
DROP VIEW IF EXISTS v_top_customers_by_spend CASCADE;
DROP VIEW IF EXISTS v_daily_metrics CASCADE;

DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- ============================================================
-- 1. CUSTOMERS
-- ============================================================
CREATE TABLE customers (
    customer_id     INTEGER      PRIMARY KEY,
    email           TEXT         NOT NULL,
    full_name       TEXT         NOT NULL,
    signup_date     DATE         NOT NULL,
    country_code    CHAR(2),              -- nullable: not all customers have a country
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,

    -- Email stored lowercase, enforce uniqueness on that value
    -- No extension needed: we store it lowercase in ETL
    CONSTRAINT uq_customers_email UNIQUE (email)
);

-- ============================================================
-- 2. ORDERS
-- ============================================================
CREATE TABLE orders (
    order_id        BIGINT       PRIMARY KEY,
    customer_id     INTEGER      NOT NULL,
    order_ts        TIMESTAMP WITH TIME ZONE NOT NULL,
    status          TEXT         NOT NULL,
    total_amount    NUMERIC(12,2) NOT NULL,
    currency        CHAR(3)      NOT NULL,

    -- Only allowed status values
    CONSTRAINT chk_orders_status
        CHECK (status IN ('placed', 'shipped', 'cancelled', 'refunded')),

    -- Referential integrity to customers
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

-- Index for common lookups by customer
CREATE INDEX idx_orders_customer_id ON orders (customer_id);

-- ============================================================
-- 3. ORDER_ITEMS
-- ============================================================
CREATE TABLE order_items (
    order_id        BIGINT       NOT NULL,
    line_no         INTEGER      NOT NULL,
    sku             TEXT         NOT NULL,
    quantity        INTEGER      NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    category        TEXT         NOT NULL,

    -- Composite primary key
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, line_no),

    -- Referential integrity to orders
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders (order_id),

    -- Business rules: must be positive
    CONSTRAINT chk_order_items_quantity
        CHECK (quantity > 0),
    CONSTRAINT chk_order_items_unit_price
        CHECK (unit_price > 0)
);

-- Index for SKU-based analytics
CREATE INDEX idx_order_items_sku ON order_items (sku);