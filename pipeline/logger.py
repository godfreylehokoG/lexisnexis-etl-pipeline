"""
Logging setup with step timing.
Provides a configured logger and a timing context manager.
"""

import logging
import time
from contextlib import contextmanager


def setup_logger(name: str = "pipeline", level: str = "INFO") -> logging.Logger:
    """Configure and return a logger with console output."""

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Avoid duplicate handlers if called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


@contextmanager
def log_step(logger: logging.Logger, step_name: str):
    """Context manager that logs step start, end, and duration."""

    logger.info(f"STEP START: {step_name}")
    start = time.time()

    try:
        yield
    except Exception as e:
        duration = time.time() - start
        logger.error(f"STEP FAILED: {step_name} after {duration:.2f}s — {e}")
        raise
    else:
        duration = time.time() - start
        logger.info(f"STEP DONE:  {step_name} ({duration:.2f}s)")