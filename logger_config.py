import logging
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name='crypto_arbitrage_bot', log_file='bot.log'):
    """Configure and return a logger with both file and console handlers."""
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )

    # Create and configure file handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10485760, backupCount=5  # 10MB per file, keep 5 backup files
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Create and configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger