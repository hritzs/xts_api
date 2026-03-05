"""
Logger Configuration - WITH FILE LOGGING
"""
import logging
import sys
from datetime import datetime
from pathlib import Path


# Create logs directory
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Log file path
LOG_FILE = LOGS_DIR / f"trading_{datetime.now().strftime('%Y%m%d')}.log"


def setup_logger(name: str = "dashboard", level: int = logging.INFO):
    """
    Setup logger with console and file output
    
    Logs saved to: logs/trading_YYYYMMDD.log
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # ══════════════════════════════════════════════════════════════════════════
    # FORMATTER
    # ══════════════════════════════════════════════════════════════════════════
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ══════════════════════════════════════════════════════════════════════════
    # CONSOLE HANDLER (colored)
    # ══════════════════════════════════════════════════════════════════════════
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # ══════════════════════════════════════════════════════════════════════════
    # FILE HANDLER (daily rotation)
    # ══════════════════════════════════════════════════════════════════════════
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info("="*80)
    logger.info(f"📝 Logger initialized - Console + File: {LOG_FILE}")
    logger.info("="*80)
    
    return logger


# Create default logger
logger = setup_logger()
