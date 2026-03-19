"""
Configuration loader.
Merges config.yaml (pipeline settings) with .env (database credentials).
Environment variables always take priority.
"""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv


def load_config(config_path: str = "config.yaml", env_path: str = ".env") -> dict:
    """Load and merge configuration from YAML and environment variables."""

    # Load .env file into environment
    load_dotenv(env_path)

    # Load YAML config
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Build database config from environment variables
    config["database"] = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "orders_pipeline"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }

    # Validate that required file paths exist
    for key, path in config["files"].items():
        if not Path(path).exists():
            raise FileNotFoundError(f"Data file not found: {path} (config key: files.{key})")

    # Ensure quarantine output directory exists
    quarantine_dir = Path(config["quarantine"]["output_dir"])
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    return config