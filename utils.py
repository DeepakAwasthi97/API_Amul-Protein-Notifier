import logging
from logging.handlers import RotatingFileHandler
try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
    _HAS_CONCURRENT_HANDLER = True
except Exception:
    ConcurrentRotatingFileHandler = None
    _HAS_CONCURRENT_HANDLER = False
from config import LOG_FILE, MAX_FILE_SIZE, MAX_OF_DAYS
import time
import random
import hashlib
import os


def setup_logging():
    # Remove all handlers associated with the root logger object (to avoid duplicate logs)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Prefer a process-safe rotating handler for multi-process writes
    if _HAS_CONCURRENT_HANDLER:
        handler = ConcurrentRotatingFileHandler(
            LOG_FILE, maxBytes=MAX_FILE_SIZE, backupCount=1, encoding="utf-8"
        )
    else:
        # Fallback: use RotatingFileHandler (not safe across multiple processes)
        handler = RotatingFileHandler(
            LOG_FILE, maxBytes=MAX_FILE_SIZE, backupCount=1, encoding="utf-8"
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
        logger.warning(
            f"Invalid 'available' field '{available_raw}' for product '{product_name}' (alias: {product_alias})"
        )

    # Validate seller_substore_ids
    seller_substore_ids = product_data.get("seller_substore_ids", [])
    if not isinstance(seller_substore_ids, list):
        logger.warning(
            f"Invalid 'seller_substore_ids' type {type(seller_substore_ids)} for product '{product_name}' (alias: {product_alias}), defaulting to []"
        )
        seller_substore_ids = []

    # Handle substore_id
    substore_ids = (
        [id.strip() for id in substore_id.split(",") if id.strip()]
        if "," in substore_id
        else [substore_id]
    )
    in_stock = available == 1 and any(
        sid in seller_substore_ids for sid in substore_ids
    )

    # Validate inventory_quantity
    inventory_quantity_raw = product_data.get("inventory_quantity", "0")
    try:
        inventory_quantity = (
            int(inventory_quantity_raw) if inventory_quantity_raw is not None else 0
        )
        if inventory_quantity < 0:
            inventory_quantity = 0
            logger.warning(
                f"Negative inventory_quantity {inventory_quantity_raw} for product '{product_name}' (alias: {product_alias}), set to 0"
            )
    except (ValueError, TypeError):
        inventory_quantity = 0
        logger.warning(
            f"Invalid inventory_quantity '{inventory_quantity_raw}' for product '{product_name}' (alias: {product_alias}), set to 0"
        )

    return in_stock, inventory_quantity
