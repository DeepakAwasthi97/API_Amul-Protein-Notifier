import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import mask
from common import PRODUCT_NAME_MAP
import logging
logger = logging.getLogger(__name__)

async def send_telegram_notification_for_user(app, chat_id, pincode, products_to_check, notify_products):
    try:
        import asyncio
        async with asyncio.timeout(10):
            if not notify_products:
                logger.info(f"No products found to notify for chat_id {chat_id}")
                return

            check_all_products = len(products_to_check) == 1 and products_to_check[0].strip().lower() == "any"

            if check_all_products:
                in_stock_products = [(name, status, quantity) for name, status, quantity in notify_products if status == "In Stock"]
                logger.info(f"In Stock products for 'Any' for chat_id {chat_id}: {len(in_stock_products)} items")
                if not in_stock_products:
                    message = f"None of the Amul Protein items have been available for a while now for your PINCODE: {pincode}"
                    logger.info(f"All products Sold Out for chat_id {chat_id}, PINCODE {pincode}")
                    # await app.bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
                else:
                    message = f"Available Amul Protein Products for PINCODE {pincode}:\n\n"
                    for name, _, quantity in in_stock_products:
                        short_name = PRODUCT_NAME_MAP.get(name, name)
                        message += f"- {short_name} \n(Quantity Left: {quantity})\n"
                    logger.info(f"Sending 'Any' notification to chat_id {chat_id}: {len(in_stock_products)} products")
                    await app.bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
            else:
                in_stock_products = [(name, status, quantity) for name, status, quantity in notify_products if status == "In Stock"]
                relevant_products = [(name, status, quantity) for name, status, quantity in in_stock_products
                                     if any(p.lower() in name.lower() for p in products_to_check)]
                if relevant_products:
                    message = f"Available Amul Protein Products for PINCODE {pincode}:\n\n"
                    for name, _, quantity in relevant_products:
                        short_name = PRODUCT_NAME_MAP.get(name, name)
                        message += f"- {short_name} \n(Quantity Left: {quantity})\n"
                    logger.info(f"Sending specific notification to chat_id {chat_id}: {len(relevant_products)} products")
                    await app.bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
                else:
                    logger.info(f"No relevant 'In Stock' products to notify for chat_id {chat_id}")
    except asyncio.TimeoutError:
        logger.error(f"Timeout sending notification to chat_id {chat_id} for pincode {pincode}")
    except Exception as e:
        logger.error(f"Error sending notification to chat_id {chat_id}: {str(e)}")
