from api_client import get_tid_and_substore, fetch_product_data_for_alias, fetch_product_data_for_alias_async, product_api_rate_limiter
from substore_mapping import load_substore_mapping, save_substore_mapping
from cache import substore_cache, substore_pincode_map, pincode_cache
from utils import is_product_in_stock, mask
from notifier import send_telegram_notification_for_user
import asyncio
import sys
import os
from config import DATABASE_FILE, TELEGRAM_BOT_TOKEN, SEMAPHORE_LIMIT, USE_SUBSTORE_CACHE, FALLBACK_TO_PINCODE_CACHE
from database import Database
import logging
from datetime import datetime, timedelta
from collections import Counter
from telegram.ext import Application
from common import PRODUCT_NAME_MAP, PRODUCT_ALIAS_MAP
import cloudscraper
import aiohttp

# Ensure the current directory is in the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)

async def get_products_availability_api_only_async(pincode, max_concurrent_products=SEMAPHORE_LIMIT):
    logger = logging.getLogger(__name__)
    
    try:
        sync_session = cloudscraper.create_scraper()
        tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
        
        async with aiohttp.ClientSession(cookies=cookies) as session:
            semaphore = asyncio.Semaphore(max_concurrent_products)
            tasks = []
            
            for product_name in PRODUCT_ALIAS_MAP.keys():
                alias = PRODUCT_ALIAS_MAP[product_name]
                task = fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, cookies=cookies)
                tasks.append((product_name, alias, task))

            product_status = []
            results = await asyncio.gather(*[task for _, _, task in tasks], return_exceptions=True)
            
            result_idx = 0
            for product_name, alias, task in tasks:
                try:
                    data = results[result_idx] if result_idx < len(results) else None
                    result_idx += 1
                    
                    if isinstance(data, Exception):
                        logger.error(f"Error fetching data for product '{product_name}' (alias: {alias}, pincode: {pincode}): {str(data)}")
                        continue

                    if data is None:
                        logger.warning(f"Session expired for product '{product_name}' (alias: {alias}, pincode: {pincode}). Refreshing session...")
                        sync_session = cloudscraper.create_scraper()
                        try:
                            tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
                            session.cookie_jar.update_cookies(cookies)
                            data = await fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, cookies=cookies)
                        except Exception as e:
                            logger.error(f"Retry failed for product '{product_name}' (alias: {alias}, pincode: {pincode}): {str(e)}")
                            continue

                    if not data:
                        logger.warning(f"No data returned for product '{product_name}' (alias: {alias}, pincode: {pincode})")
                        continue

                    item = data[0]  # Raw item dict from API
                    
                    try:
                        in_stock, inventory_quantity = is_product_in_stock(item, substore_id)
                        status = "In Stock" if in_stock else "Sold Out"
                        product_status.append((product_name, status, inventory_quantity))
                        logger.debug(f"Processed product '{product_name}' (alias: {alias}, pincode: {pincode}): {status}, quantity: {inventory_quantity}")
                    except Exception as e:
                        logger.error(f"Error processing product '{product_name}' (alias: {alias}, pincode: {pincode}): {str(e)}")
                        continue
                        
                except Exception as e:
                    logger.error(f"Unexpected error for product '{product_name}' (alias: {alias}, pincode: {pincode}): {str(e)}")
                    continue

        if not product_status:
            logger.error(f"No valid products processed for pincode {pincode}")
        else:
            logger.info(f"Successfully processed {len(product_status)} products for pincode {pincode}")

        return product_status, substore_id, substore

    except Exception as e:
        logger.error(f"API-only error for pincode {pincode}: {str(e)}")
        return [], None, None

async def check_product_availability_for_state(state_alias, sample_pincode, db):
    """Check product availability and return restock information."""
    logger.info(f"Checking state {state_alias} with sample pincode: {sample_pincode}")
    
    try:
        # Check cache first
        if USE_SUBSTORE_CACHE:
            cached_status = substore_cache.get(state_alias)
            if cached_status:
                logger.info(f"Cache hit for state {state_alias}")
                # For cached data, we can't detect restocks, so assume no restocks
                return cached_status, {}
        elif FALLBACK_TO_PINCODE_CACHE:
            cached_status = pincode_cache.get(sample_pincode)
            if cached_status:
                logger.info(f"Pincode cache hit for {sample_pincode}")
                # For cached data, we can't detect restocks, so assume no restocks
                return cached_status, {}

        product_status, substore_id, substore = await get_products_availability_api_only_async(sample_pincode)
        
        if not product_status:
            logger.error(f"No product status returned for state {state_alias} (pincode: {sample_pincode})")
            return [], {}

        logger.info(f"Processed {len(product_status)} products for state {state_alias}")

        # Record state changes and detect restocks
        restock_info = {}
        for product_name, status, inventory_quantity in product_status:
            try:
                previous_state = await db.record_state_change(state_alias, product_name, status, inventory_quantity)
                is_restock = await db.is_restock_event(state_alias, product_name, status, previous_state)
                restock_info[product_name] = is_restock
                
                if is_restock:
                    logger.info(f"RESTOCK DETECTED: {state_alias} - {product_name} - {status}")
                    
            except Exception as e:
                logger.error(f"Error recording state for {product_name}: {e}")
                restock_info[product_name] = False

        # Cache the results
        if USE_SUBSTORE_CACHE:
            substore_cache[state_alias] = product_status
        elif FALLBACK_TO_PINCODE_CACHE:
            pincode_cache[sample_pincode] = product_status

        return product_status, restock_info

    except Exception as e:
        logger.error(f"Error checking products for state {state_alias}: {str(e)}")
        return [], {}

async def should_notify_user(user, product_name, current_status, state_alias, db, is_restock=False):
    """Simplified and more reliable notification logic."""
    logger = logging.getLogger(__name__)
    
    chat_id = user.get("chat_id")
    notification_preference = user.get("notification_preference", "until_stop")
    last_notified = user.get("last_notified", {})

    # Only notify for In Stock products
    if current_status != "In Stock":
        logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): status is {current_status}")
        return False

    if notification_preference == "until_stop":
        logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): until_stop preference")
        return True
        
    elif notification_preference == "once_and_stop":
        if product_name in last_notified:
            logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): already notified (once_and_stop)")
            return False
        logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): once_and_stop, first time")
        return True
        
    elif notification_preference == "once_per_restock":
        # If no prior notification for the product, send notification for in-stock products (initial notify)
        if product_name not in last_notified:
            if current_status == "In Stock":
                return True
         # Otherwise, notify only on genuine restock detected
        if not is_restock:
            logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): not a restock event")
            return False
        logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): restock detected")
        return True

    logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): unknown preference {notification_preference}")
    return False

async def update_user_notification_tracking(user, product_name, db):
    """Update user's last notification tracking."""
    chat_id = int(user.get("chat_id"))
    
    # Update last_notified timestamp
    if "last_notified" not in user:
        user["last_notified"] = {}
    
    user["last_notified"][product_name] = datetime.now().isoformat()
    
    # Update user in database
    await db.update_user(chat_id, user)

async def check_products_for_users():
    """Check products for all users and send notifications."""
    db = Database(DATABASE_FILE)
    logger.info(f"Database object type: {type(db)}, methods: {dir(db)}")
    await db._init_db()

    try:
        users_data = await db.get_all_users()
        active_users = [u for u in users_data if u.get("active", False)]
        
        if not active_users:
            logger.info("No active users to check")
            return

        logger.info(f"Found {len(active_users)} active users")

        # Load substore mapping
        substore_info = load_substore_mapping()
        state_groups = {}
        pincode_to_state = {}
        
        # Build pincode to state mapping
        for state_data in substore_info:
            state_alias = state_data.get("alias", "")
            if state_data.get("pincodes"):
                pincodes = state_data["pincodes"].split(",")
                for pincode in pincodes:
                    pincode = pincode.strip()
                    if pincode:
                        pincode_to_state[pincode] = state_alias

        # Group users by state
        unmapped_users = []
        for user in active_users:
            pincode = str(user.get('pincode', ''))
            state_alias = pincode_to_state.get(pincode)
            
            if state_alias:
                state_groups.setdefault(state_alias, []).append(user)
            else:
                logger.warning(f"No state found for pincode {pincode}. Fetching dynamically...")
                try:
                    sync_session = cloudscraper.create_scraper()
                    tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
                    fetched_alias = substore.get("alias", f"unknown-{pincode}")
                    
                    # Check if state already exists
                    existing_entry = next((entry for entry in substore_info if entry.get("alias") == fetched_alias), None)
                    
                    if existing_entry:
                        # Append to existing state
                        if existing_entry["pincodes"]:
                            existing_entry["pincodes"] += f",{pincode}"
                        else:
                            existing_entry["pincodes"] = pincode
                            
                        if existing_entry["_id"]:
                            existing_entry["_id"] += f",{substore_id}"
                        else:
                            existing_entry["_id"] = substore_id
                            
                        state_alias = fetched_alias
                        logger.info(f"Appended pincode {pincode} and substore_id {substore_id} to existing state {state_alias}")
                    else:
                        # Create new state entry
                        new_entry = {
                            "_id": substore_id,
                            "name": substore.get("name", f"Unknown-{pincode}"),
                            "alias": fetched_alias,
                            "pincodes": pincode
                        }
                        substore_info.append(new_entry)
                        state_alias = new_entry["alias"]
                        logger.info(f"Created new entry for state {state_alias} with pincode {pincode}")
                    
                    # Save updated mapping
                    save_substore_mapping(substore_info)
                    pincode_to_state[pincode] = state_alias
                    substore_pincode_map[pincode] = substore_id
                    state_groups.setdefault(state_alias, []).append(user)
                    
                except Exception as e:
                    logger.error(f"Failed to dynamically map pincode {pincode}: {str(e)}. Skipping user.")
                    unmapped_users.append(user)
                    continue

        states_to_check = [alias for alias, users in state_groups.items() if users]
        logger.info(f"Checking {len(states_to_check)} states with users")

        # Initialize Telegram app
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await app.initialize()

        try:
            # Process all states concurrently
            state_tasks = []
            for state_alias in states_to_check:
                users_in_state = state_groups[state_alias]
                sample_pincode = users_in_state[0].get('pincode')
                task = asyncio.create_task(check_product_availability_for_state(state_alias, sample_pincode, db))
                state_tasks.append((state_alias, task))

            # Wait for all state checks to complete
            results = await asyncio.gather(*[task for _, task in state_tasks], return_exceptions=True)

            # Log any errors
            for state_alias, result in zip([state_alias for state_alias, _ in state_tasks], results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing state {state_alias}: {str(result)}")

            # Process results and send notifications
            for idx, (state_alias, _) in enumerate(state_tasks):
                result = results[idx] if not isinstance(results[idx], Exception) else ([], {})
                product_status, restock_info = result if isinstance(result, tuple) and len(result) == 2 else (result, {})
                
                if not product_status:
                    logger.warning(f"No product status for state {state_alias}")
                    continue

                notification_tasks = []
                
                for user in state_groups[state_alias]:
                    chat_id = user.get("chat_id")
                    products_to_check = user.get("products", [])
                    
                    if not chat_id or not products_to_check:
                        continue

                    check_all_products = len(products_to_check) == 1 and products_to_check[0].strip().lower() == "any"
                    preference = user.get("notification_preference", "until_stop")
                    
                    notify_products = []
                    products_notified = []
                    
                    for product_name, status, inventory_quantity in product_status:
                        if check_all_products or product_name in products_to_check:
                            is_restock = restock_info.get(product_name, False)
                            
                            if await should_notify_user(user, product_name, status, state_alias, db, is_restock):
                                notify_products.append((product_name, status, inventory_quantity))
                                products_notified.append(product_name)
                    
                    if notify_products:
                        # Send notification first
                        try:
                            await send_telegram_notification_for_user(
                                app, chat_id, user.get('pincode'), products_to_check, notify_products
                            )
                            
                            # Only update notification tracking after successful notification
                            # Skip last_notified updates for 'until_stop' to avoid unnecessary DB writes
                            if user.get("notification_preference") != "until_stop":
                                for product_name in products_notified:
                                    await update_user_notification_tracking(user, product_name, db)
                            
                            logger.info(f"Successfully notified user {chat_id} for products: {products_notified}")
                            
                        except Exception as e:
                            logger.error(f"Failed to notify user {chat_id}: {e}")
                            continue
                        
                        # Handle deactivation logic for once_and_stop
                        if preference == "once_and_stop":
                            if check_all_products:
                                user["active"] = False
                                await db.update_user(chat_id, user)
                                await app.bot.send_message(
                                    chat_id=chat_id,
                                    text="You have been notified about available products. Notifications are now stopped.\n\nUse /start to reactivate.",
                                    parse_mode="Markdown"
                                )
                            else:
                                # Check if all tracked products have been notified
                                remaining_products = [p for p in products_to_check if p not in user.get("last_notified", {})]
                                if not remaining_products:
                                    user["active"] = False
                                    await db.update_user(chat_id, user)
                                    await app.bot.send_message(
                                        chat_id=chat_id,
                                        text="You have been notified for all tracked products. Notifications are now stopped. Use /start to reactivate.",
                                        parse_mode="Markdown"
                                    )

                logger.info(f"Completed notifications for state {state_alias}")

        finally:
            await app.shutdown()
            logger.info("Telegram application shutdown completed")

    finally:
        await db.cleanup_state_history(days=2)
        await db.close()
        logger.info("Database connection closed")

    logger.info("Product check completed")
