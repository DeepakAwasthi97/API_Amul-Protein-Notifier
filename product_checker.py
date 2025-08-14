from api_client import get_tid_and_substore, fetch_product_data_for_alias_async, product_api_rate_limiter
from substore_mapping import load_substore_mapping, save_substore_mapping
from cache import substore_cache, substore_pincode_map, pincode_cache
from utils import is_product_in_stock, mask
from notifier import send_telegram_notification_for_user
import asyncio
import sys
import os
from config import TELEGRAM_BOT_TOKEN, SEMAPHORE_LIMIT, USE_SUBSTORE_CACHE, FALLBACK_TO_PINCODE_CACHE, NOTIFICATION_CONCURRENCY_LIMIT
import logging
from datetime import datetime, timedelta
from telegram.ext import Application
from common import PRODUCT_NAME_MAP, PRODUCT_ALIAS_MAP
import cloudscraper
import aiohttp
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger(__name__)

# Global locks for user-level synchronization
user_locks = {}  # chat_id -> asyncio.Lock

async def should_notify_user(user, product_name, status, state_alias, db, is_restock):
    """Determine if a notification should be sent based on user preference."""
    chat_id = user.get('chat_id', 'unknown')
    logger.info(f"Checking notification criteria for user {chat_id}, product '{product_name}', status '{status}'")
    
    if not isinstance(user, dict):
        logger.error(f"Invalid user data type for chat_id {chat_id}: {type(user)}")
        return False

    # Don't notify if product is not in stock
    if status != "In Stock":
        return False

    # Don't notify if user is not active
    if not user.get("active", False):
        return False

    # Get notification preference and last notified info
    preference = user.get("notification_preference", "until_stop")
    last_notified = user.get("last_notified", {})
    
    # Try to decode if it's a JSON string
    if isinstance(last_notified, str):
        try:
            last_notified = json.loads(last_notified)
        except json.JSONDecodeError:
            last_notified = {}

    if preference == "once_and_stop":
        # For all products tracking ("Any"), handle all available products
        check_all_products = len(user.get("products", [])) == 1 and user.get("products", [""])[0].lower() == "any"
        
        # For new tracking (empty last_notified):
        # 1. For specific products: notify if product is in stock
        # 2. For "Any": notify for any in-stock product
        if not last_notified:
            logger.info(f"First-time check for user {user.get('chat_id')}, first notification for {product_name}")
            return True
            
        # For subsequent checks:
        # Only notify if we've never notified for this product before
        # This is the core behavior of once_and_stop - one notification per product, ever
        if product_name not in last_notified:
            logger.info(f"First notification for product {product_name}")
            return True
            
        logger.debug(f"Already notified for {product_name}, skipping (once_and_stop)")
        return False
        
    elif preference == "once_per_restock":
        check_all_products = len(user.get("products", [])) == 1 and user.get("products", [""])[0].lower() == "any"
        
        # Handle first-time tracking specially
        if not last_notified:
            # For first check, notify about in-stock products
            logger.info(f"First-time check for user {user.get('chat_id')}, product {product_name}")
            return True

        # For all subsequent checks, ONLY notify if:
        # 1. It's an actual restock event (status changed from not-in-stock to in-stock)
        # 2. Or it's a completely new product we've never seen before
        if is_restock:
            if product_name in last_notified:
                last_time = datetime.fromisoformat(last_notified[product_name])
                time_since_last = datetime.now() - last_time
                # Prevent duplicate notifications for the same restock event
                # (in case our 5-minute check catches the same restock multiple times)
                if time_since_last.total_seconds() < 300:  # 5 minutes
                    logger.info(f"Skipping notification for {product_name} - too soon since last notification")
                    return False
            logger.info(f"Notifying for restock of {product_name}")
            return True
            
        return False
        
    elif preference == "until_stop":
        # Always notify while in stock
        return True
        
    return False

# In product_checker.py, update_user_notification_tracking
async def update_user_notification_tracking(user, product_name, db):
    """Update last_notified timestamp for a product using partial update."""
    if not isinstance(user, dict):
        logger.error(f"Invalid user data type for chat_id {user.get('chat_id', 'unknown')}: {type(user)}")
        return

    try:
        chat_id = int(user["chat_id"])
        preference = user.get("notification_preference", "until_stop")

        # For once_and_stop, record the notification
        # Update notification tracking for both once_and_stop and once_per_restock
        if preference in ["once_and_stop", "once_per_restock"]:
            now_iso = datetime.now().isoformat()
            last_notified = user.get("last_notified", {})
            if isinstance(last_notified, str):
                try:
                    last_notified = json.loads(last_notified)
                except json.JSONDecodeError:
                    last_notified = {}
                    
            last_notified[product_name] = now_iso
            path = ['last_notified']
            await db.update_user_partial(chat_id, path, json.dumps(last_notified))
            logging.debug(f"Updated once_and_stop notification tracking for user {chat_id} - {product_name}")

        # For once_per_restock, we only track the last notification time
        elif preference == "once_per_restock":
            now_iso = datetime.now().isoformat()
            path = ['last_notified', product_name]
            await db.update_user_partial(chat_id, path, json.dumps(now_iso))
            logging.debug(f"Updated once_per_restock notification tracking for user {chat_id} - {product_name}")

        # For until_stop, we don't need to track notifications
        elif preference == "until_stop":
            logging.debug(f"No tracking needed for until_stop preference - user {chat_id}")
            return

    except Exception as e:
        logger.error(f"Error updating notification tracking for user {user.get('chat_id', 'unknown')}: {e}")

async def get_products_availability_api_only_async(pincode, max_concurrent_products=SEMAPHORE_LIMIT):
    logger.info(f"Fetching availability for pincode: {pincode}")
    try:
        sync_session = cloudscraper.create_scraper()
        tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
        async with aiohttp.ClientSession(cookies=cookies) as session:
            semaphore = asyncio.Semaphore(max_concurrent_products)
            tasks = [
                (product_name, PRODUCT_ALIAS_MAP[product_name], fetch_product_data_for_alias_async(session, tid, substore_id, PRODUCT_ALIAS_MAP[product_name], semaphore, cookies=cookies))
                for product_name in PRODUCT_ALIAS_MAP.keys()
            ]
            product_status = []
            results = await asyncio.gather(*[task for _, _, task in tasks], return_exceptions=True)
            for (product_name, alias, _), data in zip(tasks, results):
                if isinstance(data, Exception):
                    logger.error(f"Error fetching data for {product_name}: {data}")
                    continue
                if data is None:
                    logger.warning(f"Session expired for {product_name}. Refreshing session...")
                    sync_session = cloudscraper.create_scraper()
                    tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
                    async with aiohttp.ClientSession(cookies=cookies) as new_session:
                        data = await fetch_product_data_for_alias_async(new_session, tid, substore_id, alias, semaphore, cookies=cookies)
                if data:
                    in_stock, quantity = is_product_in_stock(data[0], substore_id)
                    product_status.append((product_name, "In Stock" if in_stock else "Sold Out", quantity))
                else:
                    product_status.append((product_name, "Sold Out", 0))
            return product_status, substore_id, substore
    except Exception as e:
        logger.error(f"Error in get_products_availability_api_only_async: {e}")
        return [], None, None

async def check_product_availability_for_state(state_alias, sample_pincode, db):
    logger.info(f"Checking state {state_alias} with pincode: {sample_pincode}")
    try:
        if USE_SUBSTORE_CACHE:
            cached_status = substore_cache.get(state_alias)
            if cached_status:
                logger.info(f"Cache hit for state {state_alias}")
                for product_name, status, inventory_quantity in cached_status:
                    await db.record_state_change(state_alias, product_name, status, inventory_quantity)
                return cached_status, {}
        product_status, substore_id, substore = await get_products_availability_api_only_async(sample_pincode)
        restock_info = {}
        if product_status:
            for product_name, status, inventory_quantity in product_status:
                previous_state = await db.record_state_change(state_alias, product_name, status, inventory_quantity)
                is_restock = await db.is_restock_event(state_alias, product_name, status, previous_state)
                restock_info[product_name] = is_restock
        if USE_SUBSTORE_CACHE:
            substore_cache[state_alias] = product_status
        return product_status, restock_info
    except Exception as e:
        logger.error(f"Error checking state {state_alias}: {e}")
        return [], {}

async def check_products_for_users(db):
    logger.info("Starting product check for all users")
    try:
        await db.cleanup_state_history()
        users = await db.get_all_users()
        if not users:
            logger.warning("No users found in database")
            return
        state_groups = {}
        pincode_to_state = {}
        unmapped_users = []
        substore_info = load_substore_mapping() if USE_SUBSTORE_CACHE else []
        for user in users:
            if not isinstance(user, dict):
                logger.error(f"Invalid user data type: {type(user)}")
                continue
            pincode = user.get("pincode")
            if not pincode:
                logger.warning(f"User {user.get('chat_id')} has no pincode")
                continue
            state_alias = pincode_to_state.get(pincode)
            if state_alias:
                state_groups[state_alias].append(user)
                continue
            for sub in substore_info:
                if str(pincode) in sub.get('pincodes', []):
                    state_alias = sub['alias']
                    break
            if not state_alias and FALLBACK_TO_PINCODE_CACHE:
                state_alias = pincode_cache.get(pincode)
            if not state_alias:
                try:
                    sync_session = cloudscraper.create_scraper()
                    _, substore, substore_id, _ = get_tid_and_substore(sync_session, pincode)
                    state_alias = substore.get("alias", f"unknown-{pincode}") if isinstance(substore, dict) else str(substore)
                    # Use substore_id if available, fallback to _id from substore object
                    new_id = substore_id or (substore.get("_id", "") if isinstance(substore, dict) else "")
                    
                    # Check if an entry with this alias already exists
                    existing_sub = next((sub for sub in substore_info if sub["alias"] == state_alias), None)
                    
                    if existing_sub:
                        need_update = False
                        # Add the new pincode if not already there
                        if str(pincode) not in existing_sub["pincodes"]:
                            existing_sub["pincodes"].append(str(pincode))
                            need_update = True
                            logger.info(f"Added pincode {pincode} to existing substore {state_alias}")
                        
                        # Update _id if it's empty and we have a new one
                        if not existing_sub["_id"] and new_id:
                            existing_sub["_id"] = new_id
                            need_update = True
                            logger.info(f"Updated empty _id for substore {state_alias} to {new_id}")
                        
                        if need_update:
                            save_substore_mapping(substore_info)
                    else:
                        # Create new entry only if alias doesn't exist
                        new_sub = {
                            "alias": state_alias,
                            "_id": new_id,
                            "name": substore.get("name", state_alias.title()) if isinstance(substore, dict) else state_alias.title(),
                            "pincodes": [str(pincode)]
                        }
                        substore_info.append(new_sub)
                        logger.info(f"Created new substore entry for {state_alias} with pincode {pincode}")
                        save_substore_mapping(substore_info)
                    
                    pincode_to_state[pincode] = state_alias
                except Exception as e:
                    logger.error(f"Error mapping pincode {pincode}: {e}")
                    unmapped_users.append(user)
                    continue
            state_groups.setdefault(state_alias, []).append(user)

        states_to_check = list(state_groups.keys())
        logger.info(f"Checking {len(states_to_check)} states")

        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await app.initialize()
        try:
            state_tasks = [check_product_availability_for_state(state, state_groups[state][0]['pincode'], db) for state in states_to_check]
            results = await asyncio.gather(*state_tasks, return_exceptions=True)

            notification_semaphore = asyncio.Semaphore(NOTIFICATION_CONCURRENCY_LIMIT)
            notification_tasks = []
            for idx, state_alias in enumerate(states_to_check):
                if isinstance(results[idx], Exception):
                    logger.error(f"Error processing state {state_alias}: {results[idx]}")
                    continue
                product_status, restock_info = results[idx]
                if not product_status:
                    logger.warning(f"No product status for state {state_alias}")
                    continue
                for user in state_groups[state_alias]:
                    if not isinstance(user, dict):
                        logger.error(f"Invalid user data type for state {state_alias}")
                        continue
                    chat_id = user.get("chat_id")
                    products_to_check = user.get("products", [])
                    if not chat_id or not products_to_check:
                        continue
                    check_all_products = len(products_to_check) == 1 and products_to_check[0].strip().lower() == "any"
                    notify_products = [
                        (name, status, qty) for name, status, qty in product_status
                        if (check_all_products or name in products_to_check) and
                           await should_notify_user(user, name, status, state_alias, db, restock_info.get(name, False))
                    ]
                    products_notified = [name for name, _, _ in notify_products]
                    if notify_products:
                        logger.info(f"Preparing to notify user {chat_id} about products: {products_notified}")
                        if chat_id not in user_locks:
                            user_locks[chat_id] = asyncio.Lock()
                        async def locked_send():
                            async with user_locks[chat_id]:
                                async with notification_semaphore:
                                    logger.info(f"Starting notification process for user {chat_id}")
                                    success = await send_telegram_notification_for_user(
                                        app, 
                                        chat_id, 
                                        user.get('pincode'), 
                                        products_to_check, 
                                        notify_products
                                    )
                                    # Only update last_notified if notification was successful
                                    if success:
                                        for product_name in products_notified:
                                            await update_user_notification_tracking(user, product_name, db)
                                        logger.info(f"Successfully notified user {chat_id} for {len(products_notified)} products")
                                    else:
                                        logger.error(f"Failed to notify user {chat_id}, not updating notification tracking")
                        notification_tasks.append(locked_send())

            await asyncio.gather(*notification_tasks, return_exceptions=True)

            for state_alias, users in state_groups.items():
                for user in users:
                    if not isinstance(user, dict):
                        continue
                    chat_id = user.get("chat_id")
                    products_to_check = user.get("products", [])
                    check_all_products = len(products_to_check) == 1 and products_to_check[0].strip().lower() == "any"
                    if user.get("notification_preference") == "once_and_stop":
                        last_notified = user.get("last_notified", {})
                        if check_all_products:
                            if len(last_notified) >= len(PRODUCT_ALIAS_MAP):
                                user["active"] = False
                                await db.update_user(chat_id, user)
                                await app.bot.send_message(chat_id=chat_id, text="Notified about all products. Notifications stopped. Use /start to reactivate.", parse_mode="Markdown")
                        else:
                            remaining = [p for p in products_to_check if p not in last_notified]
                            if not remaining:
                                user["active"] = False
                                await db.update_user(chat_id, user)
                                await app.bot.send_message(chat_id=chat_id, text="Notified for all tracked products. Notifications stopped. Use /start to reactivate.", parse_mode="Markdown")

        finally:
            await app.shutdown()
            logger.info("Telegram application shutdown completed")
    finally:
        await db.close()
        logger.info("Database connection closed")
    logger.info("Product check completed")