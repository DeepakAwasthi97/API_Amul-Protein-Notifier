import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from utils import mask
from common import PRODUCT_NAME_MAP
import logging
logger = logging.getLogger(__name__)

async def send_telegram_notification_for_user(app, chat_id, pincode, products_to_check, notify_products, max_retries=3):
    logger.info(f"Attempting to send notification to chat_id {chat_id} for pincode {pincode}")
    logger.debug(f"Products to check: {products_to_check}")
    logger.debug(f"Notify products: {notify_products}")
    
    if not notify_products:
        logger.info(f"No products to notify for chat_id {chat_id}")
        return True  # Return True as this is a valid case

    check_all_products = len(products_to_check) == 1 and products_to_check[0].strip().lower() == "any"

    in_stock_products = [(name, status, quantity) for name, status, quantity in notify_products if status == "In Stock"]
    if not in_stock_products:
        logger.info(f"All products Sold Out for chat_id {chat_id}, PINCODE {pincode}")
        return True  # Return True as this is a valid case

    # Simplified message construction
    message = f"Available Amul Protein Products for PINCODE {pincode}:\n\n"
    relevant_products = in_stock_products if check_all_products else [
        (name, status, quantity) for name, status, quantity in in_stock_products if name in products_to_check
    ]
    for name, _, quantity in relevant_products:
        short_name = PRODUCT_NAME_MAP.get(name, name)
        message += f"- {short_name} \n(Quantity Left: {quantity})\n"

    if not check_all_products:
        message += '\nUse /unfollow to stop notifications for specific products.'

    logger.info(f"Sending notification to chat_id {chat_id}: {len(relevant_products)} products")
    
    # Add retry logic with timeouts
    for attempt in range(max_retries):
        try:
            async with asyncio.timeout(10):  # 10 second timeout per attempt
                await app.bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
                logger.info(f"Successfully sent notification to chat_id {chat_id}")
                return True  # Successfully sent
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} timed out for chat_id {chat_id}, retrying...")
                await asyncio.sleep(1)  # Small delay between retries
            else:
                logger.error(f"Timeout sending notification to chat_id {chat_id} after {max_retries} attempts")
                return False
        except Exception as e:
            logger.error(f"Error sending notification to chat_id {chat_id}: {str(e)}")
            return False
