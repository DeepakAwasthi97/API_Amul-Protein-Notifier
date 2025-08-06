from api_client import get_tid_and_substore, fetch_product_data_for_alias, fetch_product_data_for_alias_async, product_api_rate_limiter
from substore_mapping import load_substore_mapping, save_substore_mapping  # Restored usage
from cache import substore_cache, substore_pincode_map, pincode_cache  # Restored caching
from utils import is_product_in_stock, mask
from notifier import send_telegram_notification_for_user
import asyncio
import sys
import os
from config import DATABASE_FILE, TELEGRAM_BOT_TOKEN, SEMAPHORE_LIMIT, USE_SUBSTORE_CACHE, FALLBACK_TO_PINCODE_CACHE  # Restored config usage
from database import Database
import logging
from datetime import datetime, timedelta
from collections import Counter  # For state counting if needed
from telegram.ext import Application
from common import PRODUCT_NAME_MAP, PRODUCT_ALIAS_MAP
import cloudscraper  # Ensure imported for dynamic mapping
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
    logger.info(f"Checking state {state_alias} with sample pincode: {sample_pincode}")
    try:
        if USE_SUBSTORE_CACHE:
            cached_status = substore_cache.get(state_alias)
            if cached_status:
                logger.info(f"Cache hit for state {state_alias}")
                return cached_status
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
        for product_name, status, inventory_quantity in product_status:
            await db.record_state_change(state_alias, product_name, status, inventory_quantity)
        if USE_SUBSTORE_CACHE:
            substore_cache[state_alias] = product_status
        elif FALLBACK_TO_PINCODE_CACHE:
            pincode_cache[sample_pincode] = product_status
        return product_status
    except Exception as e:
        logger.error(f"Error checking products for state {state_alias}: {str(e)}")
        return []
    
async def should_notify_user(user, product_name, current_status, state_alias, db):
    logger = logging.getLogger(__name__)
    chat_id = user.get("chat_id")
    notification_preference = user.get("notification_preference", "until_stop")
    last_notified = user.get("last_notified", {})
    if current_status != "In Stock":
        logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): not In Stock")
        return False
    if notification_preference == "until_stop":
        logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): until_stop preference")
        return True
    elif notification_preference == "once_and_stop":
        if product_name in last_notified:
            logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): already notified")
            return False
        logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): once_and_stop, first time")
        return True
    elif notification_preference == "once_per_restock":
        last_notification_time = last_notified.get(product_name)
        if not last_notification_time:
            logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): no prior notification")
            return True
        last_state = await db.get_last_state_change(state_alias, product_name)
        if not last_state:
            logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): no state history, assuming first restock")
            return True
        try:
            last_notified_time = datetime.fromisoformat(last_notification_time)
            last_state_time = datetime.fromisoformat(last_state["timestamp"])
            previous_states = await db.get_state_changes_since(state_alias, product_name, last_notified_time)
            if any(state['status'] == 'Sold Out' for state in previous_states):
                logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): restock detected")
                return True
            if last_state["status"] == "Sold Out" and last_state_time > last_notified_time:
                logger.info(f"Notifying for {product_name} (chat_id: {chat_id}): fallback restock detected")
                return True
            logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): no restock, still In Stock")
            return False
        except Exception as e:
            logger.error(f"Error checking restock status for chat_id {chat_id}: {e}")
            return False
    logger.debug(f"Not notifying for {product_name} (chat_id: {chat_id}): unknown preference")
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

        substore_info = load_substore_mapping()
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
                    existing_entry = next((entry for entry in substore_info if entry.get("alias") == fetched_alias), None)
                    if existing_entry:
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
                        new_entry = {
                            "_id": substore_id,
                            "name": substore.get("name", f"Unknown-{pincode}"),
                            "alias": fetched_alias,
                            "pincodes": pincode
                        }
                        substore_info.append(new_entry)
                        state_alias = new_entry["alias"]
                        logger.info(f"Created new entry for state {state_alias} with pincode {pincode}")
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

        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await app.initialize()

        try:
            state_tasks = []
            for state_alias in states_to_check:
                users_in_state = state_groups[state_alias]
                sample_pincode = users_in_state[0].get('pincode')
                task = asyncio.create_task(check_product_availability_for_state(state_alias, sample_pincode, db))
                state_tasks.append((state_alias, task))

            results = await asyncio.gather(*[task for _, task in state_tasks], return_exceptions=True)

            for state_alias, result in zip([state_alias for state_alias, _ in state_tasks], results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing state {state_alias}: {str(result)}")

            for idx, (state_alias, _) in enumerate(state_tasks):
                product_status = results[idx] if not isinstance(results[idx], Exception) else []
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
                    all_product_names = [name for name, _, _ in product_status] if check_all_products else products_to_check

                    if preference == "once_per_restock":
                        restocked_products = [
                            product_name
                            for product_name, status, inventory_quantity in product_status
                            if (check_all_products or product_name in all_product_names)
                                and await should_notify_user(user, product_name, status, state_alias, db)
                        ]
                        if restocked_products:
                            notify_products = [
                                (product_name, status, inventory_quantity)
                                for product_name, status, inventory_quantity in product_status
                                if status == "In Stock" and (check_all_products or product_name in all_product_names)
                            ]
                            for product_name, _, _ in notify_products:
                                await update_user_notification_tracking(user, product_name, db)
                        else:
                            notify_products = []
                    else:
                        for product_name, status, inventory_quantity in product_status:
                            if check_all_products or product_name in all_product_names:
                                if await should_notify_user(user, product_name, status, state_alias, db):
                                    notify_products.append((product_name, status, inventory_quantity))
                                    await update_user_notification_tracking(user, product_name, db)
                                    if preference == "once_and_stop" and not check_all_products:
                                        # Check if all tracked products have been notified
                                        remaining_products = [
                                            p for p in products_to_check
                                            if p not in user.get("last_notified", {})
                                        ]
                                        if not remaining_products:
                                            user["active"] = False
                                            await db.update_user(chat_id, user)
                                            await app.bot.send_message(
                                                chat_id=chat_id,
                                                text="You have been notified for all tracked products. Notifications are now stopped. Use /start to reactivate.",
                                                parse_mode="Markdown"
                                            )
                                            logger.info(f"Deactivated user {chat_id} after once_and_stop notifications for all products")

                    if notify_products:
                        task = asyncio.create_task(
                            send_telegram_notification_for_user(
                                app, chat_id, user.get('pincode'), products_to_check, notify_products
                            )
                        )
                        notification_tasks.append(task)
                        if preference == "once_and_stop" and check_all_products:
                            user["active"] = False
                            await db.update_user(chat_id, user)
                            await app.bot.send_message(
                                chat_id=chat_id,
                                text="You have been notified about available products. Notifications are now stopped.\n\nUse /start to reactivate or use /setproducts to track specific products once again.",
                                parse_mode="Markdown"
                            )
                            logger.info(f"Deactivated user {chat_id} after once_and_stop notification for 'any' products")

                if notification_tasks:
                    await asyncio.gather(*notification_tasks, return_exceptions=True)
                logger.info(f"Completed notifications for state {state_alias}")

        finally:
            await app.shutdown()
            logger.info("Telegram application shutdown completed")

    finally:
        await db.cleanup_state_history(days=2)
        await db.close()
        logger.info("Database connection closed")
        logger.info("Product check completed")