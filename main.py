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
    # We'll build this next
    logger.info("ETL pipeline not yet implemented")


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