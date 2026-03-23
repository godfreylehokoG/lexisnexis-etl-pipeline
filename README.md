# Orders Data Pipeline

A maintainable ETL pipeline that ingests messy raw data files, validates and
transforms them, loads clean data into PostgreSQL, and surfaces analytics
through SQL views.

Built with Python 3.10+, pandas, psycopg v3, and PostgreSQL 17.

## 📹 Video Walkthrough

[Data_Pipeline Walk Through-pt1](https://www.loom.com/share/b4fb50ba67b746e98a9eb82226c08075)
[Data_Pipeline Walk Through-pt2](https://www.loom.com/share/d60ee4ee1a7049c0a6403bcac2502373)
[Data_Pipeline Walk Through-pt3](https://www.loom.com/share/20c4ee19760c4a91a357f09e2845d02d)
[Data_Pipeline Walk Through-pt4](https://www.loom.com/share/1d1912452eca42489af215f20b4491e2)

## Project Structure

```
lexisnexis-etl-pipeline/
├── main.py                  # CLI entry point (init / run / all)
├── config.yaml              # Pipeline configuration
├── .env.example             # Database credentials template
├── requirements.txt         # Python dependencies
├── run.sh                   # Single command to run everything
├── SOLUTION.md              # Design decisions & trade-offs
├── REPORT.md                # Generated pipeline report
├── data/
│   ├── raw/                 # Input files (CSV, JSONL)
│   │   ├── customers.csv
│   │   ├── orders.jsonl
│   │   └── order_items.csv
│   └── quarantine/          # Rejected rows with reasons
├── sql/
│   ├── schema.sql           # Table DDL with constraints
│   ├── views_analytics.sql  # Analytics views (Task 3)
│   └── views_quality.sql    # Data quality views (Task 3)
├── pipeline/
│   ├── config.py            # Configuration loader
│   ├── logger.py            # Logging and step timing
│   ├── db.py                # Database connection helper
│   ├── extract.py           # Raw file ingestion
│   ├── transform.py         # Validation and cleaning
│   ├── load.py              # psycopg v3 COPY loader
│   ├── quarantine.py        # Rejected row output
│   └── report.py            # Report generation
└── tests/
    └── test_transform.py    # Unit tests for transform logic
```

## Prerequisites

- Python 3.10+
- PostgreSQL 14+

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/godfreylehokoG/lexisnexis-etl-pipeline.git
cd lexisnexis-etl-pipeline
```

### 2. Create and activate virtual environment

```bash
python -m venv .venv

# Windows (Git Bash)
source .venv/Scripts/activate

# Windows (CMD)
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the database

```bash
psql -U postgres
```

```sql
CREATE DATABASE orders_pipeline;
\q
```

### 5. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=orders_pipeline
DB_USER=postgres
DB_PASSWORD=your_password
```

## Usage

### Quick Start (Single Command)

```bash
source .venv/Scripts/activate
./run.sh
```

This will:
1. Install dependencies
2. Run unit tests
3. Initialize the database schema
4. Run the full ETL pipeline
5. Generate REPORT.md

### Individual Commands

```bash
# Initialize schema (create tables and views)
python main.py init

# Run the ETL pipeline
python main.py run

# Run everything (init + run)
python main.py all
```

### Run Tests

```bash
python -m pytest tests/ -v
```

## Pipeline Flow

```
EXTRACT                    TRANSFORM                   LOAD
─────────────────────     ─────────────────────      ─────────────────
customers.csv (6 rows)  → Clean + validate         → 4 rows loaded
                           ├── Normalize emails       2 quarantined
                           ├── Validate email format
                           └── Deduplicate (earliest signup wins)
                               │
                               ├── valid_customer_ids {1,2,3,4}
                               ▼
orders.jsonl (10 rows)  → Clean + validate         → 6 rows loaded
                           ├── Check customer exists   4 quarantined
                           ├── Validate status
                           └── Standardize timestamps to UTC
                               │
                               ├── valid_order_ids {1001,1002,1005,1008,1009,1010}
                               ▼
order_items.csv (12 rows) → Clean + validate       → 5 rows loaded
                             ├── Check order exists    7 quarantined
                             ├── Validate quantity > 0
                             └── Validate unit_price > 0
```

## Pipeline Output

### Expected Results

| Table       | Input Rows | Loaded | Quarantined | Survival Rate |
|-------------|-----------|--------|-------------|---------------|
| customers   | 6         | 4      | 2           | 67%           |
| orders      | 10        | 6      | 4           | 60%           |
| order_items | 12        | 5      | 7           | 42%           |

### Quarantine Files

Rejected rows are written to `data/quarantine/` with a `_rejection_reason` column:

- `customers.csv` — invalid emails, duplicate emails
- `orders.csv` — unknown customers, invalid statuses, orphaned by customer dedup
- `order_items.csv` — orphaned orders, non-positive quantities/prices

### Cascading Rejections

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

### Analytics Views

Query these after running the pipeline:

```sql
-- Daily order metrics
SELECT * FROM v_daily_metrics;

-- Top 10 customers by lifetime spend
SELECT * FROM v_top_customers_by_spend;

-- Top 10 SKUs by revenue
SELECT * FROM v_top_skus;
```

### Data Quality Views

```sql
-- Duplicate customers by email (safety net — should return 0 after clean ETL)
SELECT * FROM v_dq_duplicate_emails;

-- Orders referencing missing customers (safety net)
SELECT * FROM v_dq_orphaned_orders;

-- Items with non-positive quantity or price
SELECT * FROM v_dq_invalid_items;

-- Orders with invalid status
SELECT * FROM v_dq_invalid_status;
```

## Project Trade-Offs

### COPY vs Batched Inserts
I chose psycopg v3 client-side COPY for bulk loading. COPY is 10-100x faster than
row-by-row inserts, which matters at scale. The trade-off is that COPY doesn't
support UPSERT (ON CONFLICT) logic — if a constraint is violated, the entire COPY
fails. I mitigate this by cleaning all data in the transform step before loading.
The ETL does the hard work so the database load is simple and fast.

### Quarantine vs Remap
I quarantine invalid data rather than silently remapping it. For example, status
"processing" is rejected rather than mapped to "placed". The trade-off is fewer
rows loaded (15 out of 28). But the data that does load is trustworthy. In data
engineering, the golden rule is: never silently change data. If something is wrong,
reject it, record why, and let a human decide.

### DROP + CREATE vs Migrations
The schema setup drops and recreates tables every run. This is simple and
guarantees a clean state for the reviewer. In production with real data, you would
never DROP tables — you'd use a migration tool like Alembic for incremental schema
changes.

### Full Reload vs Incremental
The pipeline does a full reload every run. This guarantees consistency — same input
always produces the same output. At scale with millions of rows, you'd need
incremental loading with watermarks or change data capture. For this dataset size,
full reload is the right choice.

### Strict Constraints vs Flexibility
I enforce strict constraints — quantity and unit_price must be positive. This means
items for cancelled orders with price=0.00 get rejected, which might be valid
business logic. In production, I'd discuss with the business team whether cancelled
orders should have different constraints.

### Regular Views vs Materialized Views
I use regular views for analytics. They compute on every query, which is fine for
this data size. Materialized views would cache results for faster reads but need
manual refreshing. The assignment asked for regular views, and at this scale there
is no performance concern.

## AI Usage

This project was developed with the assistance of AI (Claude) as a collaborative
tool. Here is how AI was used and, importantly, how it was NOT used:

### How AI Was Used
- **Architecture discussion**: Brainstormed project structure, discussed trade-offs
  between different approaches (e.g., COPY vs inserts, quarantine vs remap)
- **Edge case analysis**: Traced every row through the pipeline before writing code
  to map out cascading rejections and expected output counts
- **Code generation**: AI helped generate boilerplate code for configuration loading,
  logging setup, and database connection helpers
- **SQL review**: Discussed constraint naming, index placement, and view design
- **Documentation**: AI helped structure the README, SOLUTION.md, and REPORT.md

### How AI Was NOT Used
- **Decision making**: Every design decision (quarantine strategy, nullable columns,
  timestamp assumptions) was discussed, weighed, and chosen by me — not blindly
  accepted from AI output
- **Understanding**: I traced the data flow and edge cases manually before any code
  was written. I understand every line and can explain why it exists.
- **Blind copy-paste**: Code was reviewed, understood, and adapted — not dumped in
  wholesale

### My Philosophy on AI
AI is a powerful tool for accelerating development, but the engineer must own the
decisions. AI doesn't know your business context, your team's conventions, or your
production constraints. I used AI the same way I'd use Stack Overflow or a senior
colleague — as a resource for discussion, not a replacement for thinking.
```





