"""
Logger Setup Module
===================

This module provides a utility function to create and configure a logger
that outputs messages to both a file and the console.

Features
--------
- Logs messages to a file located in LOGS_DIR (defined in config.paths).
- Logs messages to the console (stdout).
- Ensures the log directory exists before writing.
- Prevents adding duplicate handlers if the logger already exists.
- Supports standard logging levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.
- Configurable log file name and logging level per logger instance.

Log Format
----------
%(asctime)s [%(levelname)s] %(message)s

Example
-------
from utils.logger import setup_logger

logger = setup_logger("my_logger", level="DEBUG", log_filename="my_project.log")
logger.info("This is an info message.")
logger.error("This is an error message.")
"""

import logging
from pathlib import Path
from config.paths import LOGS_DIR

def setup_logger(name: str, level: str = "INFO", log_filename: str = "project.log") -> logging.Logger:
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
        Name of the log file stored in LOGS_DIR. Default is 'project.log'.

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
