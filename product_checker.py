from api_client import get_tid_and_substore, fetch_product_data_for_alias, fetch_product_data_for_alias_async, product_api_rate_limiter
from substore_mapping import load_substore_mapping, save_substore_mapping  # Restored usage
from cache import substore_cache, substore_pincode_map, pincode_cache  # Restored caching
from utils import is_product_in_stock, mask
from notifier import send_telegram_notification_for_user
import asyncio
import sys
import os
from config import DATABASE_FILE, TELEGRAM_BOT_TOKEN, SEMAPHORE_LIMIT, USE_SUBSTORE_CACHE, FALLBACK_TO_PINCODE_CACHE, EXECUTION_MODE  # Restored config usage
from database import Database
import logging
from datetime import datetime, timedelta
from collections import Counter  # For state counting if needed
from telegram.ext import Application
from common import PRODUCT_NAME_MAP, PRODUCT_ALIAS_MAP
import cloudscraper  # Ensure imported for dynamic mapping

# Ensure the current directory is in the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)

async def get_products_availability_api_only_async(pincode, max_concurrent_products=SEMAPHORE_LIMIT):  # Restored semaphore limit from config
    try:
        import cloudscraper
        import aiohttp
        # Initialize cloudscraper session
        sync_session = cloudscraper.create_scraper()
        # Get initial tid, substore, substore_id, and cookies
        tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
        async with aiohttp.ClientSession(cookies=cookies) as session:
            semaphore = asyncio.Semaphore(max_concurrent_products)
            tasks = []
            for product_name in PRODUCT_ALIAS_MAP.keys():
                alias = PRODUCT_ALIAS_MAP[product_name]
                task = fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, cookies=cookies)  # Restored full async fetch with retries
                tasks.append((product_name, alias, task))
            # Gather results concurrently (restored concurrency)
            product_status = []
            results = await asyncio.gather(*[task for _, _, task in tasks], return_exceptions=True)
            result_idx = 0
            for product_name, alias, task in tasks:
                data = results[result_idx] if result_idx < len(results) else None
                result_idx += 1
                if isinstance(data, Exception):
                    logger.error(f"Error for alias '{alias}' (pincode: {pincode}): {str(data)}")
                    continue
                if data is None:
                    logger.warning(f"Session expired for alias '{alias}' (pincode: {pincode}). Refreshing session...")
                    sync_session = cloudscraper.create_scraper()
                    try:
                        tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
                        session.cookie_jar.update_cookies(cookies)
                        data = await fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, cookies=cookies)
                    except Exception as e:
                        logger.error(f"Retry failed for alias '{alias}' (pincode: {pincode}): {str(e)}")
                        continue
                if data:
                    item = data[0]
                    in_stock = is_product_in_stock(item, substore_id)
                    availability = "In Stock" if in_stock else "Sold Out"
                    product_status.append((product_name, availability))
                else:
                    logger.warning(f"No data returned for alias '{alias}' (pincode: {pincode})")
                    continue
            return product_status, substore_id, substore
    except Exception as e:
        logger.error(f"API-only error for pincode {pincode}: {str(e)}")
        return [], None, None

async def check_product_availability_for_state(state_alias, sample_pincode, db):
    logger.info(f"Checking state {state_alias} with sample pincode: {sample_pincode}")
    try:
        # Restored: Check cache first if enabled
        if USE_SUBSTORE_CACHE:
            cached_status = substore_cache.get(state_alias)  # Assuming state_alias as key; adapt if needed
            if cached_status:
                logger.info(f"Cache hit for state {state_alias}")
                return cached_status
        # Fallback to pincode cache if enabled and no substore cache hit
        elif FALLBACK_TO_PINCODE_CACHE:
            cached_status = pincode_cache.get(sample_pincode)
            if cached_status:
                logger.info(f"Pincode cache hit for {sample_pincode}")
                return cached_status
        product_status, substore_id, substore = await get_products_availability_api_only_async(sample_pincode)
        if not product_status:
            logger.error(f"No product status returned for state {state_alias} (pincode: {sample_pincode})")
            return []
        logger.info(f"Processed {len(product_status)} products for state {state_alias}")
        # Record state changes in database (preserved)
        for product_name, status in product_status:
            last_state = await db.get_last_state_change(state_alias, product_name)
            if not last_state or last_state["status"] != status:
                await db.record_state_change(state_alias, product_name, status)
                logger.info(f"State change recorded: {state_alias} - {product_name} - {status}")
        # Restored: Update cache
        if USE_SUBSTORE_CACHE:
            substore_cache[state_alias] = product_status
        elif FALLBACK_TO_PINCODE_CACHE:
            pincode_cache[sample_pincode] = product_status
        return product_status
    except Exception as e:
        logger.error(f"Error checking products for state {state_alias}: {str(e)}")
        return []

async def should_notify_user(user, product_name, current_status, state_alias, db):
    """Determine if user should be notified based on notification preference."""
    logger = logging.getLogger(__name__)
    chat_id = user.get("chat_id")
    notification_preference = user.get("notification_preference", "until_stop")
    last_notified = user.get("last_notified", {})
    # Only notify for in-stock products
    if current_status != "In Stock":
        return False
    if notification_preference == "until_stop":
        return True
    elif notification_preference == "once_and_stop":
        if product_name in last_notified:
            return False
        return True
    elif notification_preference == "once_per_restock":
        last_notification_time = last_notified.get(product_name)
        if not last_notification_time:
            return True
        last_state = await db.get_last_state_change(state_alias, product_name)
        if not last_state:
            return True
        try:
            last_notified_time = datetime.fromisoformat(last_notification_time)
            last_state_time = datetime.fromisoformat(last_state["timestamp"])
            # Check if the product was out-of-stock since last notification
            if last_state_time > last_notified_time:
                previous_states = await db.get_state_changes_since(state_alias, product_name, last_notified_time)
                last_sold_out = await db.get_last_sold_out_before(state_alias, product_name, datetime.now())
                if last_sold_out and last_sold_out["timestamp"] > last_notified_time:
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking restock status for chat_id {chat_id}: {e}")
            return False
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
    from telegram.ext import Application
    db = Database(DATABASE_FILE)
    await db._init_db()
    try:
        users_data = await db.get_all_users()
        active_users = [u for u in users_data if u.get("active", False)]
        if not active_users:
            logger.info("No active users to check")
            return
        logger.info(f"Found {len(active_users)} active users")
        # Restored: Load dynamic substore mapping
        substore_info = load_substore_mapping()
        # Group users by state_alias (preserved, but add dynamic handling)
        state_groups = {}
        pincode_to_state = {}
        for state_data in substore_info:
            state_alias = state_data.get("alias", "")
            if state_data.get("pincodes"):
                pincodes = state_data["pincodes"].split(",")
                for pincode in pincodes:
                    pincode = pincode.strip()
                    if pincode:
                        pincode_to_state[pincode] = state_alias
        # Restored: Handle users with unmapped pincodes dynamically
        unmapped_users = []
        for user in active_users:
            pincode = str(user.get('pincode', ''))
            state_alias = pincode_to_state.get(pincode)
            if state_alias:
                state_groups.setdefault(state_alias, []).append(user)
            else:
                # Dynamic fetch and update
                logger.warning(f"No state found for pincode {pincode}. Fetching dynamically...")
                try:
                    # Use sync fetch for simplicity (or make async if needed)
                    sync_session = cloudscraper.create_scraper()
                    tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
                    # Extract alias from fetched substore
                    fetched_alias = substore.get("alias", f"unknown-{pincode}")
                    # Check if this alias already exists in substore_info
                    existing_entry = next((entry for entry in substore_info if entry.get("alias") == fetched_alias), None)
                    if existing_entry:
                        # Append to existing entry
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
                        # Create new entry if alias doesn't exist
                        new_entry = {
                            "_id": substore_id,
                            "name": substore.get("name", f"Unknown-{pincode}"),
                            "alias": fetched_alias,
                            "pincodes": pincode
                        }
                        substore_info.append(new_entry)
                        state_alias = new_entry["alias"]
                        logger.info(f"Created new entry for state {state_alias} with pincode {pincode}")
                    # Persist changes
                    save_substore_mapping(substore_info)
                    # Update in-memory
                    pincode_to_state[pincode] = state_alias
                    substore_pincode_map[pincode] = substore_id  # Restore cache update
                    state_groups.setdefault(state_alias, []).append(user)
                except Exception as e:
                    logger.error(f"Failed to dynamically map pincode {pincode}: {str(e)}. Skipping user.")
                    unmapped_users.append(user)  # Or handle fallback
                    continue
        states_to_check = [alias for alias, users in state_groups.items() if users]
        logger.info(f"Checking {len(states_to_check)} states with users")
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await app.initialize()
        try:
            # Restored: Check states concurrently if EXECUTION_MODE == "Concurrent"
            if EXECUTION_MODE == "Concurrent":
                state_tasks = []
                for state_alias in states_to_check:
                    users_in_state = state_groups[state_alias]
                    sample_pincode = users_in_state[0].get('pincode')
                    task = asyncio.create_task(check_product_availability_for_state(state_alias, sample_pincode, db))
                    state_tasks.append((state_alias, task))
                # Gather results concurrently
                results = await asyncio.gather(*[task for _, task in state_tasks], return_exceptions=True)
                for idx, (state_alias, _) in enumerate(state_tasks):
                    product_status = results[idx] if not isinstance(results[idx], Exception) else []
                    if not product_status:
                        logger.warning(f"No product status for state {state_alias}")
                        continue
                    # Notify users in this state based on their preferences
                    notification_tasks = []
                    for user in state_groups[state_alias]:
                        chat_id = user.get("chat_id")
                        products_to_check = user.get("products", [])
                        if not chat_id or not products_to_check:
                            continue
                        # Filter products based on notification preferences
                        notify_products = []
                        check_all_products = len(products_to_check) == 1 and products_to_check[0].lower() == "any"
                        all_product_names = [name for name, _ in product_status] if check_all_products else products_to_check
                        for product_name, status in product_status:
                            if check_all_products or product_name in all_product_names:
                                if await should_notify_user(user, product_name, status, state_alias, db):
                                    notify_products.append((product_name, status))
                                    await update_user_notification_tracking(user, product_name, db)
                        if notify_products:
                            task = asyncio.create_task(
                                send_telegram_notification_for_user(
                                    app, chat_id, user.get('pincode'), products_to_check, notify_products
                                )
                            )
                            notification_tasks.append(task)
                    if notification_tasks:
                        await asyncio.gather(*notification_tasks, return_exceptions=True)
                    logger.info(f"Completed notifications for state {state_alias}")
            else:
                # Sequential fallback (current behavior)
                for state_alias in states_to_check:
                    try:
                        users_in_state = state_groups[state_alias]
                        # Get a sample pincode from this state
                        sample_pincode = users_in_state[0].get('pincode')
                        logger.info(f"Checking state {state_alias} with {len(users_in_state)} users (sample pincode: {sample_pincode})")
                        # Check product availability for this state
                        product_status = await check_product_availability_for_state(state_alias, sample_pincode, db)
                        if not product_status:
                            logger.warning(f"No product status for state {state_alias}")
                            continue
                        # Notify users in this state based on their preferences
                        notification_tasks = []
                        for user in users_in_state:
                            chat_id = user.get("chat_id")
                            products_to_check = user.get("products", [])
                            if not chat_id or not products_to_check:
                                continue
                            # Filter products based on notification preferences
                            notify_products = []
                            check_all_products = len(products_to_check) == 1 and products_to_check[0].lower() == "any"
                            all_product_names = [name for name, _ in product_status] if check_all_products else products_to_check
                            for product_name, status in product_status:
                                if check_all_products or product_name in all_product_names:
                                    if await should_notify_user(user, product_name, status, state_alias, db):
                                        notify_products.append((product_name, status))
                                        await update_user_notification_tracking(user, product_name, db)
                            if notify_products:
                                task = asyncio.create_task(
                                    send_telegram_notification_for_user(
                                        app, chat_id, user.get('pincode'), products_to_check, notify_products
                                    )
                                )
                                notification_tasks.append(task)
                        if notification_tasks:
                            await asyncio.gather(*notification_tasks, return_exceptions=True)
                        logger.info(f"Completed notifications for state {state_alias}")
                    except Exception as e:
                        logger.error(f"Error processing state {state_alias}: {str(e)}")
                        continue
        finally:
            await app.shutdown()
            logger.info("Telegram application shutdown completed")
    finally:
        await db.close()
        logger.info("Database connection closed")
