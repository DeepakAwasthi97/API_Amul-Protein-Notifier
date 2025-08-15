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
    
    try:
        if not isinstance(user, dict):
            logger.error(f"Invalid user data type for chat_id {chat_id}: {type(user)}")
            return False

    # Basic validation checks
        if status != "In Stock":
            logger.debug(f"Product {product_name} not in stock for user {chat_id}")
            return False

        if not user.get("active", False):
            logger.debug(f"User {chat_id} is not active")
            return False

        # Get and validate notification preference
        preference = user.get("notification_preference", "until_stop")
        last_notified = user.get("last_notified", {})
        
        # Handle JSON string format of last_notified
        if isinstance(last_notified, str):
            try:
                last_notified = json.loads(last_notified)
            except json.JSONDecodeError:
                logger.warning(f"Invalid last_notified JSON for user {chat_id}, resetting to empty")
                last_notified = {}
        # DEBUG: Log current decision inputs
        logger.debug(
            f"Decision inputs for user={chat_id} product={product_name} preference={preference} "
            f"status={status} is_restock={is_restock} last_notified_keys={list(last_notified.keys())} active={user.get('active')} products={user.get('products')}"
        )
    except Exception as e:
        logger.error(f"Error in initial notification check for user {chat_id}: {str(e)}")
        return False

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
        logger.debug(f"Already notified for {product_name}, skipping (once_and_stop). last_notified contains: {list(last_notified.keys())}")
        return False

    elif preference == "once_per_restock":
        try:
            # Handle first-time tracking
            if not last_notified:
                logger.info(f"First-time check for user {chat_id}, product {product_name}")
                return True

            # Only notify on restock events
            if is_restock:
                logger.info(f"Restock detected for {product_name} user {chat_id}")
                if product_name in last_notified:
                    try:
                        last_time = datetime.fromisoformat(last_notified[product_name])
                        time_since_last = datetime.now() - last_time

                        # Since we know this is a restock event:
                        # 1. Product is currently In Stock
                        # 2. Product was Out of Stock at some point since last In Stock (verified by is_restock)
                        # Just have a minimal cooldown to prevent double notifications
                        if time_since_last.total_seconds() < 60:  # 1-minute cooldown
                            logger.debug(f"Skipping notification for {product_name} - too soon since last notification (time_since_last={time_since_last.total_seconds():.1f}s)")
                            return False
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid timestamp for {product_name}, user {chat_id}: {e}")
                        # Continue with notification if timestamp is invalid

                logger.info(f"Notifying for restock of {product_name} for user {chat_id}")
                return True

            # Product is in stock but not a restock event
            logger.debug(f"Product {product_name} is in stock but not a restock event for user {chat_id}")
            return False

        except Exception as e:
            logger.error(f"Error in once_per_restock handler for user {chat_id}: {str(e)}")
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
        return False

    try:
        chat_id = int(user["chat_id"])
    except (ValueError, KeyError, TypeError):
        logger.error(f"Invalid chat_id in user data: {user.get('chat_id', 'unknown')}")
        return False

    try:
        preference = user.get("notification_preference", "until_stop")
        now_iso = datetime.now().isoformat()

        if preference == "until_stop":
            logger.debug(f"No tracking needed for until_stop preference - user {chat_id}")
            return True

        # Get current last_notified data
        last_notified = user.get("last_notified", {})
        if isinstance(last_notified, str):
            try:
                last_notified = json.loads(last_notified)
            except json.JSONDecodeError:
                logger.warning(f"Invalid last_notified JSON for user {chat_id}, resetting")
                last_notified = {}

        # Update the tracking based on preference
        if preference in ["once_and_stop", "once_per_restock"]:
            # Update in-memory structure first so callers see immediate change
            last_notified[product_name] = now_iso
            user['last_notified'] = last_notified
            path = ['last_notified']
            await db.update_user_partial(chat_id, path, json.dumps(last_notified))
            logger.debug(f"Updated {preference} notification tracking for user {chat_id} - {product_name}. last_notified now: {list(last_notified.keys())}")
            return True

    except Exception as e:
        logger.error(f"Error updating notification tracking for user {chat_id}, product {product_name}: {str(e)}")
        return False

    return True

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

async def validate_user_state(user, db):
    """
    Validate that a user is still active and has a valid configuration.
    """
    try:
        chat_id = user.get("chat_id")
        if not chat_id:
            logger.warning("User has no chat_id")
            return False

        # Check if user is marked as active
        if not user.get("active", False):
            logger.debug(f"User {chat_id} is not active")
            return False

        # Check for valid pincode
        pincode = user.get("pincode")
        if not pincode:
            logger.warning(f"User {chat_id} has no pincode")
            return False

        # Check for valid product preferences
        products = user.get("products", [])
        if not products:
            logger.warning(f"User {chat_id} has no product preferences")
            return False

        # All checks passed
        return True

    except Exception as e:
        logger.error(f"Error validating user state: {str(e)}")
        return False

async def should_deactivate_user(chat_id, app):
    """
    Determine if a user should be deactivated based on their chat state.
    """
    try:
        async with asyncio.timeout(5):
            # Try to get chat member info
            chat = await app.bot.get_chat(chat_id)
            if not chat:
                logger.info(f"Chat {chat_id} not found")
                return True
            return False
    except Exception as e:
        error_msg = str(e).lower()
        # Deactivate if bot was blocked or chat not found
        if "blocked" in error_msg or "not found" in error_msg or "forbidden" in error_msg:
            return True
        # For other errors, don't deactivate
        logger.error(f"Error checking chat state for {chat_id}: {str(e)}")
        return False

async def check_products_for_users(db):
    logger.info("Starting product check for all users")
    try:
        await db.cleanup_state_history()
        users = await db.get_all_users()
        total_users = len(users)
        if not users:
            logger.warning("No users found in database")
            return
            
        # Log user statistics
        active_users = sum(1 for user in users if user.get("active", False))
        configured_users = sum(1 for user in users 
                             if user.get("pincode") and user.get("products"))
        preference_stats = {
            "until_stop": 0,
            "once_and_stop": 0,
            "once_per_restock": 0
        }
        for user in users:
            pref = user.get("notification_preference", "until_stop")
            preference_stats[pref] = preference_stats.get(pref, 0) + 1
            
        logger.info(f"User Statistics:")
        logger.info(f"Total Users: {total_users}")
        logger.info(f"Active Users: {active_users}")
        logger.info(f"Configured Users: {configured_users}")
        logger.info(f"Notification Preferences: {preference_stats}")
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
            user_notifications = {}  # Track notifications per user: chat_id -> (products, state)
            
            # First pass: collect all notifications per user across all states
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
                    if not chat_id:
                        logger.warning(f"User in state {state_alias} has no chat_id")
                        continue

                    try:
                        chat_id = int(chat_id)  # Convert to int early to catch invalid format
                    except ValueError:
                        logger.error(f"Invalid chat_id format in state {state_alias}: {chat_id}")
                        continue
                        
                    products_to_check = user.get("products", [])
                    if not chat_id or not products_to_check:
                        continue
                    check_all_products = len(products_to_check) == 1 and products_to_check[0].strip().lower() == "any"
                    notify_products = [
                        (name, status, qty) for name, status, qty in product_status
                        if (check_all_products or name in products_to_check) and
                           await should_notify_user(user, name, status, state_alias, db, restock_info.get(name, False))
                    ]
                    if notify_products:
                        products_notified = [name for name, _, _ in notify_products]
                        if chat_id in user_notifications:
                            # Merge with existing notifications
                            existing_products = user_notifications[chat_id][0]
                            existing_notify = user_notifications[chat_id][1]
                            merged_products = list(set(existing_products + products_to_check))
                            merged_notify = [(n, s, q) for n, s, q in notify_products + existing_notify
                                          if (n, s, q) not in existing_notify]
                            user_notifications[chat_id] = (merged_products, merged_notify)
                            logger.debug(f"Merged notifications for user {chat_id}")
                        else:
                            user_notifications[chat_id] = (products_to_check, notify_products)
                            if chat_id not in user_locks:
                                user_locks[chat_id] = asyncio.Lock()
                        logger.info(f"Prepared notifications for user {chat_id}: {products_notified}")
                        
            # Define the notification sending function outside the loop
            async def locked_send(chat_id, user, products_to_check, notify_products, products_notified):
                try:
                    async with user_locks[chat_id]:
                        if not await validate_user_state(user, db):
                            logger.info(f"User {chat_id} is no longer active or has invalid configuration")
                            return None  # Don't retry for invalid users
                        async with notification_semaphore:
                            logger.info(f"Starting notification process for user {chat_id}")
                            result = await send_telegram_notification_for_user(
                                app, 
                                chat_id, 
                                user.get('pincode'), 
                                products_to_check, 
                                notify_products
                            )
                            if result is True:  # Success
                                try:
                                    for product_name in products_notified:
                                        await update_user_notification_tracking(user, product_name, db)
                                    logger.info(f"Successfully notified user {chat_id} for {len(products_notified)} products")
                                    return True
                                except Exception as e:
                                    logger.error(f"Error updating notification tracking for user {chat_id}: {str(e)}")
                                    return True  # Still return True as notification succeeded
                            elif result is None:  # Permanent error
                                logger.warning(f"Permanent error for user {chat_id}, deactivating...")
                                await db.update_user_partial(chat_id, ["active"], False)
                                return None  # Don't retry
                            else:  # Temporary error (False)
                                logger.warning(f"Temporary error for user {chat_id}, may retry")
                except asyncio.CancelledError:
                    logger.warning(f"Notification task cancelled for user {chat_id}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error in notification task for user {chat_id}: {str(e)}")
                    return False
                return True

            # After collecting all notifications, create tasks
            for chat_id, (products_to_check, notify_products) in user_notifications.items():
                products_notified = [name for name, _, _ in notify_products]
                logger.info(f"Creating notification task for user {chat_id} with {len(products_notified)} products")
                # Find the user object for this chat_id
                user = next((u for users in state_groups.values() for u in users if str(u.get("chat_id")) == str(chat_id)), None)
                # Create and add the task with name for better tracking
                task = asyncio.create_task(
                    locked_send(chat_id, user, products_to_check, notify_products, products_notified),
                    name=f"notify_{chat_id}"
                )
                notification_tasks.append(task)

            # Wait for all notification tasks to complete and handle any errors
            if notification_tasks:
                logger.info(f"Waiting for {len(notification_tasks)} notification tasks to complete...")
                try:
                    results = await asyncio.gather(*notification_tasks, return_exceptions=True)
                    success_count = 0
                    permanent_error_count = 0
                    temp_error_count = 0
                    
                    for i, result in enumerate(results):
                        task_name = notification_tasks[i].get_name()
                        if isinstance(result, Exception):
                            logger.error(f"Notification task {task_name} failed with error: {str(result)}")
                            temp_error_count += 1
                        elif result is True:
                            success_count += 1
                        elif result is None:
                            permanent_error_count += 1
                        else:
                            temp_error_count += 1
                            
                    logger.info(
                        f"Completed notifications: {success_count} successful, "
                        f"{permanent_error_count} permanent failures, "
                        f"{temp_error_count} temporary failures out of {len(notification_tasks)} total"
                    )
                except Exception as e:
                    logger.error(f"Error while gathering notification tasks: {str(e)}")
            else:
                logger.info("No notifications to send")
            logger.info("All notification tasks completed")

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
                            # For "Any", deactivate as soon as we've notified about ANY product
                            if last_notified:  # If we've notified about at least one product
                                if user.get("active", True):  # Only send message if user is still active
                                    user["active"] = False
                                    await db.update_user(chat_id, user)
                                    await app.bot.send_message(
                                        chat_id=chat_id, 
                                        text="Notifications stopped after first available product notification. Use /start to reactivate and get notifications for more products.", 
                                        parse_mode="Markdown"
                                    )
                        else:
                            # For specific products, deactivate only when we've notified about all requested products
                            notified_all = all(p in last_notified for p in products_to_check)
                            if notified_all and user.get("active", True):  # Only send message if user is still active
                                user["active"] = False
                                await db.update_user(chat_id, user)
                                await app.bot.send_message(
                                    chat_id=chat_id, 
                                    text="Notified for all tracked products. Notifications stopped. Use /start to reactivate.", 
                                    parse_mode="Markdown"
                                )

        finally:
            await app.shutdown()
            logger.info("Telegram application shutdown completed")
    finally:
        await db.close()
        logger.info("Database connection closed")
    logger.info("Product check completed")