"""
Orders Data Pipeline — Main Entry Point

Usage:
    python main.py init    Create/recreate database schema and views
    python main.py run     Run the full ETL pipeline
    python main.py all     Run init + run in sequence
"""

import argparse
import sys

from pipeline.config import load_config
from pipeline.logger import setup_logger, log_step
from pipeline.db import get_connection, execute_sql_file
from pipeline.extract import extract_customers, extract_orders, extract_order_items
from pipeline.transform import transform_customers, transform_orders, transform_order_items
from pipeline.quarantine import write_quarantine
from pipeline.load import load_customers, load_orders, load_order_items
from pipeline.report import generate_report


def cmd_init(config: dict, logger):
    """Create/recreate database schema and views."""

    with log_step(logger, "Database schema setup"):
        conn = get_connection(config["database"])
        try:
            execute_sql_file(conn, "sql/schema.sql")
            logger.info("Tables created: customers, orders, order_items")

            execute_sql_file(conn, "sql/views_analytics.sql")
            logger.info("Analytics views created")

            execute_sql_file(conn, "sql/views_quality.sql")
            logger.info("Data quality views created")
        finally:
            conn.close()


def cmd_run(config: dict, logger):
    """Run the full ETL pipeline."""

    files = config["files"]
    quarantine_dir = config["quarantine"]["output_dir"]

    # ── EXTRACT ──────────────────────────────────────────────
    with log_step(logger, "Extract"):
        raw_customers = extract_customers(files["customers"])
        logger.info(f"  Extracted {len(raw_customers)} customers")

        raw_orders = extract_orders(files["orders"])
        logger.info(f"  Extracted {len(raw_orders)} orders")

        raw_items = extract_order_items(files["order_items"])
        logger.info(f"  Extracted {len(raw_items)} order items")

    # ── TRANSFORM ────────────────────────────────────────────
    with log_step(logger, "Transform"):

        # Customers
        clean_customers, rejected_customers = transform_customers(raw_customers)
        logger.info(f"  Customers — clean: {len(clean_customers)}, rejected: {len(rejected_customers)}")

        # Orders (needs valid customer IDs from previous step)
        valid_customer_ids = set(clean_customers["customer_id"].tolist())
        clean_orders, rejected_orders = transform_orders(raw_orders, valid_customer_ids)
        logger.info(f"  Orders    — clean: {len(clean_orders)}, rejected: {len(rejected_orders)}")

        # Order items (needs valid order IDs from previous step)
        valid_order_ids = set(clean_orders["order_id"].tolist())
        clean_items, rejected_items = transform_order_items(raw_items, valid_order_ids)
        logger.info(f"  Items     — clean: {len(clean_items)}, rejected: {len(rejected_items)}")

    # ── QUARANTINE ───────────────────────────────────────────
    with log_step(logger, "Quarantine"):
        q1 = write_quarantine(rejected_customers, quarantine_dir, "customers.csv")
        q2 = write_quarantine(rejected_orders, quarantine_dir, "orders.csv")
        q3 = write_quarantine(rejected_items, quarantine_dir, "order_items.csv")
        logger.info(f"  Quarantined — customers: {q1}, orders: {q2}, items: {q3}")

    # ── LOAD ─────────────────────────────────────────────────
    with log_step(logger, "Load"):
        conn = get_connection(config["database"])
        try:
            n1 = load_customers(conn, clean_customers)
            logger.info(f"  Loaded {n1} customers")

            n2 = load_orders(conn, clean_orders)
            logger.info(f"  Loaded {n2} orders")

            n3 = load_order_items(conn, clean_items)
            logger.info(f"  Loaded {n3} order items")
        finally:
            conn.close()

    # ── SUMMARY ──────────────────────────────────────────────
    logger.info("=" * 50)
    logger.info("PIPELINE SUMMARY")
    logger.info(f"  Customers:   {len(raw_customers)} in → {n1} loaded, {q1} quarantined")
    logger.info(f"  Orders:      {len(raw_orders)} in → {n2} loaded, {q2} quarantined")
    logger.info(f"  Order Items: {len(raw_items)} in → {n3} loaded, {q3} quarantined")
    logger.info("=" * 50)

    # ── REPORT ───────────────────────────────────────────────
    with log_step(logger, "Report generation"):
        conn = get_connection(config["database"])
        try:
            llm_config = config.get("llm", {})
            generate_report(conn, quarantine_dir, llm_config=llm_config)
            logger.info("  Generated REPORT.md")
        finally:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Orders Data Pipeline")
    parser.add_argument(
        "command",
        choices=["init", "run", "all"],
        help="Command to execute: init (schema), run (ETL), all (init + run)",
    )
    args = parser.parse_args()

    # Load config and setup logger
    config = load_config()
    logger = setup_logger()

    logger.info(f"Command: {args.command}")
    logger.info(f"Database: {config['database']['dbname']}@{config['database']['host']}")

    try:
        if args.command == "init":
            cmd_init(config, logger)

        elif args.command == "run":
            cmd_run(config, logger)

        elif args.command == "all":
            cmd_init(config, logger)
            cmd_run(config, logger)

        logger.info("Pipeline finished successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()