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
        backupCount=1,
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
    - Returns a tuple: (in_stock: bool, inventory_quantity: int)
    """
    logger = logging.getLogger(__name__)
    product_name = product_data.get("name", "Unknown")
    product_alias = product_data.get("alias", "Unknown")

    # Validate available
    available_raw = product_data.get("available", "0")
    try:
        available = int(available_raw) if available_raw is not None else 0
    except (ValueError, TypeError):
        available = 0
        logger.warning(f"Invalid 'available' field '{available_raw}' for product '{product_name}' (alias: {product_alias})")

    # Validate seller_substore_ids
    seller_substore_ids = product_data.get("seller_substore_ids", [])
    if not isinstance(seller_substore_ids, list):
        logger.warning(f"Invalid 'seller_substore_ids' type {type(seller_substore_ids)} for product '{product_name}' (alias: {product_alias}), defaulting to []")
        seller_substore_ids = []

    # Handle substore_id
    substore_ids = [id.strip() for id in substore_id.split(',') if id.strip()] if ',' in substore_id else [substore_id]
    in_stock = available == 1 and any(sid in seller_substore_ids for sid in substore_ids)

    # Validate inventory_quantity
    inventory_quantity_raw = product_data.get("inventory_quantity", "0")
    try:
        inventory_quantity = int(inventory_quantity_raw) if inventory_quantity_raw is not None else 0
        if inventory_quantity < 0:
            inventory_quantity = 0
            logger.warning(f"Negative inventory_quantity {inventory_quantity_raw} for product '{product_name}' (alias: {product_alias}), set to 0")
    except (ValueError, TypeError):
        inventory_quantity = 0
        logger.warning(f"Invalid inventory_quantity '{inventory_quantity_raw}' for product '{product_name}' (alias: {product_alias}), set to 0")

    return in_stock, inventory_quantity