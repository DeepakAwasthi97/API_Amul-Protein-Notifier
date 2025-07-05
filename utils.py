import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from config import LOG_FILE, MAX_FILE_SIZE, MAX_OF_DAYS
import time
import random
import hashlib
import os

def setup_logging():
    # Remove all handlers associated with the root logger object (to avoid duplicate logs)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Rotating by size and by time (days)
    class SizeAndTimeRotatingHandler(TimedRotatingFileHandler):
        def __init__(self, filename, maxBytes, backupCount, when, interval, encoding=None, delay=False, utc=False, atTime=None):
            super().__init__(filename, when=when, interval=interval, backupCount=backupCount, encoding=encoding, delay=delay, utc=utc, atTime=atTime)
            self.maxBytes = maxBytes

        def shouldRollover(self, record):
            # Time-based rollover
            if super().shouldRollover(record):
                return 1
            # Size-based rollover
            if self.maxBytes > 0:
                self.stream = self.stream or self._open()
                if os.path.getsize(self.baseFilename) >= self.maxBytes:
                    return 1
            return 0

    handler = SizeAndTimeRotatingHandler(
        LOG_FILE,
        maxBytes=MAX_FILE_SIZE,
        backupCount=5,  # Keep last 5 logs
        when='D',
        interval=MAX_OF_DAYS,
        encoding='utf-8',
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            handler,
        ],
    )
    return logging.getLogger(__name__)

def mask(value, visible=2):
    value = str(value)
    if len(value) <= visible * 2:
        return "*" * len(value)
    return value[:visible] + "*" * (len(value) - 2 * visible) + value[-visible:]

def is_product_in_stock(product_data, substore_id):
    """
    Robustly determine if a product is in stock:
    - available == 1
    - substore_id is in seller_substore_ids
    """
    try:
        available = int(product_data.get("available", 0))
    except Exception:
        available = 0
    seller_substore_ids = product_data.get("seller_substore_ids", [])
    return available == 1 and substore_id in seller_substore_ids
