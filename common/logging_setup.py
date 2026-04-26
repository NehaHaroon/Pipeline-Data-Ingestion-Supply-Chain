"""
Centralized logging configuration to avoid duplication across modules.
"""

import logging
import os
from datetime import datetime


def setup_logging(module_name: str, log_dir: str = "logs", log_level: str = "INFO") -> logging.Logger:
    """
    Set up logging with consistent format across the project.
    
    Args:
        module_name: Name of the module (typically __name__)
        log_dir: Directory to store log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure root logger with both console and file handlers
    log_format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Set up root logger (only if not already configured)
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        root_logger.setLevel(getattr(logging, log_level))
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        root_logger.addHandler(console_handler)
        
        # File handler
        # filename = f"{module_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        filename = f"run_production_{datetime.now().strftime('%Y%m%d_%H')}.log"
        log_file = os.path.join(log_dir, filename)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        root_logger.addHandler(file_handler)
    
    # Get module-specific logger
    logger = logging.getLogger(module_name)
    logger.log_file = getattr(root_logger, "log_file", None)
    return logger
