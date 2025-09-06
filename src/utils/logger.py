"""
Logger Setup Module
-------------------

Provides a utility function to create a logger that logs messages
to both a file and the console. Log files are stored in the
directory defined by `LOGS_DIR` in `config.paths`.
"""

import logging
from pathlib import Path
from config.paths import LOGS_DIR

def setup_logger(name: str, level: str = "INFO", log_filename: str = "rag_reviews.log") -> logging.Logger:
    """
    Create and configure a logger with file and console output.

    Parameters
    ----------
    name : str
        Unique name for the logger.
    level : str, optional
        Logging level: 'DEBUG', 'INFO', 'WARNING', 'ERROR', or 'CRITICAL'.
        Default is 'INFO'.
    log_filename : str, optional
        Name of the log file stored in LOGS_DIR. Default is 'rag_reviews.log'.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger

    # Ensure log directory exists
    log_path = Path(LOGS_DIR) / log_filename
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert level string to logging constant
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Define log format
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # File handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger