# Complete SOLUTION.md

```markdown
# Solution: Design Decisions and Trade-offs

## Overview

This pipeline follows a modular Extract → Transform → Load architecture with a
quarantine layer for rejected data. Each module has a single responsibility,
making the pipeline testable, maintainable, and operationally transparent.

The pipeline processes three messy source files (CSV and JSONL), validates them
against business rules and referential constraints, loads clean data into
PostgreSQL, and surfaces analytics through SQL views.

## Data Analysis (Before Writing Code)

Before writing any code, I traced every row through the pipeline to understand
the edge cases and predict the exact output. This analysis drove every design
decision.

### Source Data Summary

| File | Format | Rows | Issues Found |
|------|--------|------|--------------|
| customers.csv | CSV | 6 | Invalid email, duplicate emails, missing country |
| orders.jsonl | JSONL | 10 | Unknown customers, invalid status, mixed timezone formats |
| order_items.csv | CSV | 12 | Orphaned orders, zero quantities, zero prices |

### Edge Cases Identified

| Row | Issue | Impact |
|-----|-------|--------|
| Customer 5 | Duplicate email (same as Customer 4 after lowercase) | Cascades to Order 1006 and its items |
| Customer 6 | Invalid email — no @ sign | Cascades to Order 1007 and its items |
| Order 1003 | References customer_id=999 (never existed) | Cascades to Item (1003,1) |
| Order 1004 | Status "processing" not in allowed set | Cascades to Items (1004,1) and (1004,2) |
| Order 1005 | Timestamp uses slash format, no timezone | Needs parsing and UTC assumption |
| Item (1004,2) | quantity=0 | Violates positive constraint |
| Item (1005,1) | unit_price=0.00 | Violates positive constraint |
| Item (1008,2) | unit_price=0.00 | Violates positive constraint |

### Predicted Output (Confirmed by Pipeline)

| Table | Input | Loaded | Quarantined |
|-------|-------|--------|-------------|
| customers | 6 | 4 | 2 |
| orders | 10 | 6 | 4 |
| order_items | 12 | 5 | 7 |

### Cascading Rejection Map

```
Customer 5 → rejected (duplicate email)
  └── Order 1006 → rejected (orphaned customer)
       └── Item (1006,1) → rejected (orphaned order)

Customer 6 → rejected (invalid email)
  └── Order 1007 → rejected (orphaned customer)
       └── Item (1007,1) → rejected (orphaned order)

Customer 999 → never existed
  └── Order 1003 → rejected (unknown customer)
       └── Item (1003,1) → rejected (orphaned order)

Order 1004 → rejected (invalid status "processing")
  └── Item (1004,1) → rejected (orphaned order)
  └── Item (1004,2) → rejected (orphaned order + qty=0)

Item (1005,1) → rejected (unit_price=0.00)
Item (1008,2) → rejected (unit_price=0.00)
```

## Schema Decisions

### Table: customers

| Column | Constraint | Reasoning |
|--------|-----------|-----------|
| customer_id | PRIMARY KEY | Unique identifier, implicitly NOT NULL |
| email | NOT NULL, UNIQUE | Core identifier. Stored lowercase. Uniqueness enforced on stored value — no extensions needed |
| full_name | NOT NULL | Pipeline reliability — always need a name |
| signup_date | NOT NULL | Required for deduplication logic (earliest signup wins) |
| country_code | NULLABLE | Customer 3 has no country. Missing data is different from incorrect data. I'd rather have a NULL than an invented value |
| is_active | NOT NULL, DEFAULT TRUE | Boolean with sensible default |

### Table: orders

| Column | Constraint | Reasoning |
|--------|-----------|-----------|
| order_id | PRIMARY KEY | Unique identifier |
| customer_id | NOT NULL, FK → customers | Enforces referential integrity |
| order_ts | TIMESTAMP WITH TIME ZONE, NOT NULL | Source has mixed timezones. TIMESTAMPTZ stores UTC correctly and handles display conversion |
| status | NOT NULL, CHECK IN (placed, shipped, cancelled, refunded) | Named constraint makes error messages readable |
| total_amount | NUMERIC(12,2), NOT NULL | Even 0.00 is valid (cancelled orders) |
| currency | CHAR(3), NOT NULL | All data is ZAR but schema supports multi-currency |

### Table: order_items

| Column | Constraint | Reasoning |
|--------|-----------|-----------|
| (order_id, line_no) | Composite PRIMARY KEY | One line per item per order |
| order_id | FK → orders | Cascading referential integrity |
| quantity | NOT NULL, CHECK > 0 | Business rule from assignment spec |
| unit_price | NUMERIC(12,2), NOT NULL, CHECK > 0 | Business rule from assignment spec |
| category | NOT NULL | Required for SKU analytics |

### Additional Schema Choices

- **Named constraints**: Every constraint has an explicit name (e.g., `chk_orders_status`,
  `fk_orders_customer`). When violations occur, error messages immediately identify the problem.
- **Indexes**: Added on `orders.customer_id` and `order_items.sku` to support analytics view
  performance.
- **Idempotent DDL**: Schema uses DROP IF EXISTS + CASCADE, so `python main.py init` can be
  run repeatedly without errors.
- **Drop order**: Tables dropped child-first (order_items → orders → customers) to respect
  foreign key dependencies.

## ETL Decisions

### Invalid Emails → Quarantine

Customer 6 has email `bademail` with no @ sign. I quarantine rather than attempt
to fix, because inventing an email would corrupt the data. A basic regex validates
the format: must have characters before @, after @, and after a dot.

**Alternative considered**: Accept all emails and let the application layer validate.
Rejected because bad emails in the database would violate the spirit of the unique
email constraint.

### Duplicate Emails → Keep Earliest Signup

Customers 4 and 5 have the same email after normalization (`dup.email@example.com`).
The assignment suggests keeping the earliest signup. Customer 4 (2024-01-01) is
retained, Customer 5 (2024-02-01) is quarantined.

**Cascading impact**: Order 1006 references Customer 5, which was removed by
deduplication. This order becomes an orphan and is quarantined. Its order item
(1006, 1) cascades to quarantine as well.

**Alternative considered**: Remap Order 1006's customer_id from 5 to 4, since they
represent the same person. I chose not to because silent ID remapping in a pipeline
can cause subtle downstream bugs and makes the pipeline harder to audit. Better to
quarantine and let a human decide.

### Invalid Status → Quarantine (Not Remap)

Order 1004 has status `processing`, not in the allowed set. I quarantine rather
than mapping to a default (e.g., `placed`) because:

- Silently changing business data is a common source of downstream analytics errors
- The pipeline should surface data quality issues, not hide them
- A human or business rule engine should decide how to resolve unknown statuses
- There is no way to know what "processing" was intended to mean in the source system

### Timezone-Naive Timestamps → Assume UTC

Orders 1004 and 1005 have no timezone information. I assume UTC and document this
assumption. The pipeline uses `pd.to_datetime(utc=True, format="mixed")` to handle
the variety of formats in the source data.

**Alternative considered**: Assume SAST (+02:00) since most data has South African
context. I chose UTC because it's the safer default — assuming a local timezone
without confirmation from the source team could introduce errors.

In production, I would clarify with the data source team whether naive timestamps
are UTC or local time.

### Non-Positive Quantities/Prices → Quarantine the Item, Not the Order

Order 1008 has two line items: one valid (H-654, price=110.00) and one invalid
(H-655, price=0.00). I quarantine only the invalid item, keeping the order with
its remaining valid items.

This preserves maximum data while enforcing constraints. The order's total_amount
in the orders table still reflects the original amount, which is a known
inconsistency — in production, I would recalculate or flag this.

**Alternative considered**: Quarantine the entire order when any item is invalid.
Rejected because it would unnecessarily discard valid transaction data.

### Client-Side COPY for Loading

I use psycopg v3's `cursor.copy()` with CSV format for bulk loading. This is
the recommended approach for batch ETL workloads and is significantly faster
than row-by-row inserts.

Data is streamed from a StringIO buffer in 8KB chunks, keeping memory usage
low even for larger datasets.

**Trade-off**: COPY doesn't support ON CONFLICT (UPSERT) logic. If a constraint
violation occurs, the entire COPY fails. I mitigate this by thoroughly cleaning
data in the transform step before loading. The ETL does the hard work so the
database load is simple and fast.

## Quarantine Strategy

Rejected rows are written to CSV files in `data/quarantine/` with a
`_rejection_reason` column. This approach:

- **Preserves rejected data** for human review and reprocessing
- **Documents reasons** — exactly why each row was rejected
- **Provides an audit trail** for data governance
- **Enables debugging** — analysts can open the CSV and immediately understand
- **Surfaces source issues** — if quarantine counts spike, investigate the source

In production, I would extend this to:
- Write quarantine records to a database table
- Add timestamps and pipeline run IDs
- Set up alerting when quarantine counts exceed thresholds
- Build a reprocessing workflow for quarantined rows

## Data Quality Views

The DQ views (`v_dq_duplicate_emails`, `v_dq_orphaned_orders`) return zero rows
after a clean pipeline run. This is the expected and correct behavior — the ETL
catches issues before they reach the database.

These views serve as a **post-load safety net** for production monitoring. If bad
data ever bypasses the ETL validation (e.g., from a direct database insert or a
bug in the transform logic), these views will detect it.

## Analytics Views

### v_daily_metrics
Groups orders by date with aggregated counts, revenue, and average order value.
Uses a CTE for clean separation of aggregation and formatting.

### v_top_customers_by_spend
Joins orders to customers, aggregates spending, and ranks using `ROW_NUMBER()`
window function. Limited to top 10.

### v_top_skus
Groups order items by SKU, calculates revenue as `quantity * unit_price`, and
ranks using `ROW_NUMBER()` window function. Limited to top 10.

All views use `CREATE OR REPLACE VIEW` for idempotent creation and CTEs with
window functions as suggested by the assignment.

## Configuration Design

Configuration is split into two layers:

| File | Contains | In Git? | Why |
|------|----------|---------|-----|
| config.yaml | File paths, pipeline behavior, business rules | Yes | Safe to share and version track |
| .env | Database credentials | No | Secrets stay out of Git |
| .env.example | Template for .env | Yes | Shows reviewer what's needed |

Pipeline behavior (duplicate strategy, invalid status handling, valid statuses)
is configurable in `config.yaml`, acknowledging that business rules change over
time. A new status value can be added without touching Python code.

Environment variables can override database settings, supporting deployment
to different environments (dev, staging, production).

## Testing Strategy

Unit tests cover the transform layer, which contains all business logic:

- Email normalization
- Invalid email rejection
- Duplicate email deduplication
- Missing country code handling
- Valid order passthrough
- Unknown customer rejection
- Invalid status rejection
- Timestamp UTC standardization
- Valid item passthrough
- Orphaned order item rejection
- Zero quantity rejection
- Zero price rejection
- Partial order preservation (bad item dropped, good item kept)

Tests use isolated DataFrames — no database required. This makes them fast,
reliable, and CI-friendly.

## What I Would Improve for Production

### Idempotent Runs
Add TRUNCATE before COPY so the pipeline can be run multiple times safely without
duplicate data errors.

### Transaction Safety
Wrap all three table loads in a single database transaction. If order items fail
to load, roll back customers and orders too. All-or-nothing prevents partial data.

### Logging to File
Add a file handler to the logger for persistent log storage. Production pipelines
need searchable log history for debugging.

### Incremental Loading
Instead of full reload, use watermarks (e.g., last modified timestamp) or change
data capture for efficiency at scale with millions of rows.

### Schema Migrations
Replace DROP + CREATE with a migration tool like Alembic for incremental, reversible
schema changes that preserve existing data.

### Alerting
Integrate with monitoring (e.g., PagerDuty, Slack) to notify when quarantine counts
exceed thresholds or the pipeline fails.

### Database-Backed Quarantine
Move quarantine from CSV files to a database table with pipeline run IDs, timestamps,
and a reprocessing workflow.

### Data Reconciliation
After loading, verify row counts and checksums match between source and destination
to catch silent data loss.
```

---
