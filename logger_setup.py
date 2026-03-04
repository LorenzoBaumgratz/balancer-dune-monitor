"""
Centralised logging configuration.
Import `get_logger` everywhere instead of calling logging.getLogger directly.
"""

import logging
import logging.handlers
from config import LOG_LEVEL, LOG_FORMAT, LOG_FILE


def setup_logging() -> None:
    """Configure the root logger once at process startup."""
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    fmt = logging.Formatter(LOG_FORMAT)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Rotating file handler – keep 7 × 10 MB files
    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=7, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
