# utils/logging_utils.py
"""
Logging Utilities Module

Enhanced logging configuration and monitoring utilities.
"""

import logging
import sys
from typing import Dict, Any


def setup_logging(config: Dict[str, Any], verbose: bool = False) -> None:
    """
    Set up logging configuration based on config and command line options.
    
    Args:
        config: Configuration dictionary
        verbose: Enable verbose logging
    """
    # Set log level
    if verbose:
        log_level = logging.DEBUG
    else:
        log_level = getattr(logging, config.get('log_level', 'INFO').upper())
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if configured
    log_file = config.get('log_file')
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Reduce noise from some libraries
    logging.getLogger('distributed').setLevel(logging.WARNING)
    logging.getLogger('dask').setLevel(logging.WARNING)
    logging.getLogger('tornado').setLevel(logging.WARNING)