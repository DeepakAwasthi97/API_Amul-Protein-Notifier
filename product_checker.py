from api_client import get_tid_and_substore, fetch_product_data_for_alias, fetch_product_data_for_alias_async, product_api_rate_limiter
from substore_mapping import load_substore_mapping, save_substore_mapping
from cache import substore_cache, substore_pincode_map, pincode_cache
from utils import is_product_in_stock, mask
from notifier import send_telegram_notification_for_user
import asyncio
import sys
import os
from config import USE_DATABASE, DATABASE_FILE, TELEGRAM_BOT_TOKEN, SEMAPHORE_LIMIT, USE_SUBSTORE_CACHE, EXECUTION_MODE
from database import Database  # Import Database class for USE_DATABASE = True

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def get_products_availability_api_only_async(pincode, max_concurrent_products=3):
    import logging
    logger = logging.getLogger(__name__)
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
            from common import PRODUCT_ALIAS_MAP
            for product_name, alias in PRODUCT_ALIAS_MAP.items():
                # Pass cookies to ensure session consistency
                task = fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, cookies=cookies)
                tasks.append((product_name, alias, task))
            # Gather results
            product_status = []
            results = await asyncio.gather(*[task for _, _, task in tasks], return_exceptions=True)
            result_idx = 0
            for product_name, alias, task in tasks:
                data = results[result_idx] if result_idx < len(results) else None
                result_idx += 1
                if isinstance(data, Exception):
                    logger.error(f"Error for alias '{alias}' (pincode: {pincode}): {str(data)}")
                    continue  # Skip failed products
                if data is None:
                    # Session likely expired, refresh session and retry
                    logger.warning(f"Session expired for alias '{alias}' (pincode: {pincode}). Refreshing session...")
                    sync_session = cloudscraper.create_scraper()
                    try:
                        tid, substore, substore_id, cookies = get_tid_and_substore(sync_session, pincode)
                        session.cookie_jar.update_cookies(cookies)
                        data = await fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, cookies=cookies)
                    except Exception as e:
                        logger.error(f"Retry failed for alias '{alias}' (pincode: {pincode}): {str(e)}")
                        continue  # Skip failed products
                if data:
                    item = data[0]
                    in_stock = is_product_in_stock(item, substore_id)
                    availability = "In Stock" if in_stock else "Sold Out"
                    product_status.append((product_name, availability))
                else:
                    logger.warning(f"No data returned for alias '{alias}' (pincode: {pincode})")
                    continue  # Skip products with no data
            return product_status, substore_id, substore
    except Exception as e:
        logger.error(f"API-only error for pincode {pincode}: {str(e)}")
        return [], None, None

async def check_product_availability_async(pincode):
    global pincode_cache, substore_cache, substore_pincode_map
    import logging
    logger = logging.getLogger(__name__)
    try:
        if USE_SUBSTORE_CACHE:
            if not substore_pincode_map:
                substore_info = load_substore_mapping()
                for sub in substore_info:
                    for pc in sub.get('pincodes', '').split(','):
                        pc = pc.strip()
                        if pc:
                            substore_pincode_map[pc] = sub['_id']
            substore_id = substore_pincode_map.get(str(pincode))
            # If substore_id is comma-separated, use the first ID for cache lookup
            if substore_id and ',' in substore_id:
                substore_id = substore_id.split(',')[0].strip()
            if substore_id and substore_id in substore_cache:
                logger.info(f"[CACHE] Using substore cache for substore_id: {substore_id} (pincode: {pincode})")
                return substore_cache[substore_id]
            elif pincode in pincode_cache:
                logger.info(f"[CACHE] Using pincode cache for pincode: {pincode} (fallback)")
                return pincode_cache[pincode]
        else:
            if pincode in pincode_cache:
                logger.info(f"[CACHE] Using pincode cache for pincode: {pincode}")
                return pincode_cache[pincode]
        
        product_status, substore_id, substore = await get_products_availability_api_only_async(pincode)
        if not product_status or not substore_id:
            logger.error(f"Skipping product processing for pincode {pincode} due to session failure.")
            return []
        
        logger.info(f"Processed {len(product_status)} products for substore {substore_id} (pincode {pincode})")
        
        if USE_SUBSTORE_CACHE and substore_id:
            substore_cache[substore_id] = product_status
            substore_pincode_map[str(pincode)] = substore_id
            substore_info = load_substore_mapping()
            found = False
            for sub in substore_info:
                sub_alias = sub.get('alias', '')
                substore_alias = substore.get('alias', '')
                if sub['_id'] == substore_id or substore_id in sub['_id'].split(','):
                    pincodes = set([pc.strip() for pc in sub.get('pincodes', '').split(',') if pc.strip()])
                    if str(pincode) not in pincodes:
                        pincodes.add(str(pincode))
                        sub['pincodes'] = ','.join(sorted(pincodes))
                        logger.info(f"[MAPPING] Added pincode {pincode} to substore_id {substore_id} (alias: {sub_alias}) in mapping.")
                        save_substore_mapping(substore_info)
                    found = True
                    break
                elif sub_alias == substore_alias and substore_alias:
                    # Update existing substore with matching alias
                    pincodes = set([pc.strip() for pc in sub.get('pincodes', '').split(',') if pc.strip()])
                    ids = set([id.strip() for id in sub.get('_id', '').split(',') if id.strip()])
                    if str(pincode) not in pincodes:
                        pincodes.add(str(pincode))
                        sub['pincodes'] = ','.join(sorted(pincodes))
                    if substore_id not in ids:
                        ids.add(substore_id)
                        sub['_id'] = ','.join(sorted(ids))
                        logger.info(f"[MAPPING] Appended substore_id {substore_id} to alias {substore_alias} with pincode {pincode}.")
                        save_substore_mapping(substore_info)
                    found = True
                    break
            if not found:
                # Create new substore entry using API response data
                new_entry = {
                    "_id": substore_id,
                    "name": substore.get('name', f"Unknown-{substore_id}"),
                    "alias": substore.get('alias', f"substore-{substore_id[:6]}"),
                    "pincodes": str(pincode)
                }
                substore_info.append(new_entry)
                logger.info(f"[MAPPING] Created new substore entry for substore_id {substore_id} with pincode {pincode} and alias {new_entry['alias']}.")
                save_substore_mapping(substore_info)
        
        if USE_SUBSTORE_CACHE and substore_id:
            return substore_cache[substore_id]
        else:
            pincode_cache[pincode] = product_status
            return product_status
    except Exception as e:
        logger.error(f"Error checking products for pincode {pincode}: {str(e)}")
        return []

async def check_products_for_users():
    import logging
    logger = logging.getLogger(__name__)
    from common import read_users_file, PRODUCT_NAME_MAP
    from telegram.ext import Application
    # Initialize database if USE_DATABASE is True
    db = None
    if USE_DATABASE:
        db = Database(DATABASE_FILE)
        await db._init_db()
    
    try:
        # Read user data based on USE_DATABASE
        if USE_DATABASE:
            users_data = await db.get_all_users()
        else:
            users_data = read_users_file()["users"]
        
        debug_pincode = ""
        if debug_pincode:
            active_users = [u for u in users_data if u.get("active", False) and u.get("pincode") == debug_pincode]
            logger.info(f"[DEBUG] Restricting to single pincode: {debug_pincode} ({len(active_users)} users)")
        else:
            active_users = [u for u in users_data if u.get("active", False)]
        if not active_users:
            logger.info("No active users to check")
            return
        
        logger.info(f"Found {len(active_users)} active users")
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await app.initialize()
        
        try:
            user_groups = {}
            if USE_SUBSTORE_CACHE:
                substore_info = load_substore_mapping()
                for sub in substore_info:
                    for pc in sub.get('pincodes', '').split(','):
                        pc = pc.strip()
                        if pc:
                            # Use the first substore_id for grouping
                            substore_id = sub['_id'].split(',')[0].strip() if ',' in sub['_id'] else sub['_id']
                            substore_pincode_map[pc] = substore_id
                for user in active_users:
                    pincode = str(user.get('pincode'))
                    substore_id = substore_pincode_map.get(pincode)
                    if substore_id:
                        user_groups.setdefault(substore_id, []).append(user)
                    else:
                        user_groups.setdefault(pincode, []).append(user)
                logger.info(f"Pre-grouped users into {len(user_groups)} groups (substore or pincode)")
            else:
                for user in active_users:
                    pincode = user.get('pincode')
                    if not pincode:
                        logger.error(f"Skipping user with missing pincode: {user}")
                        continue
                    user_groups.setdefault(pincode, []).append(user)
                logger.info(f"Grouped users into {len(user_groups)} unique pincodes")
            
            if EXECUTION_MODE == 'Concurrent':
                semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
            
            import importlib
            MAX_ATTEMPTS = getattr(importlib.import_module('config'), 'MAX_RETRY', 3)
            all_keys = list(user_groups.keys())
            successfully_checked = set()
            failed_checks = set(all_keys)
            attempt = 1
            attempts = {k: 0 for k in all_keys}
            passed_on_attempt = {k: None for k in all_keys}
            
            while attempt <= MAX_ATTEMPTS and failed_checks:
                if attempt == 1:
                    logger.info(f"--- Attempt {attempt} for {len(failed_checks)} total groups ---")
                else:
                    logger.info(f"--- Attempt {attempt} for {len(failed_checks)} failed groups ---")

                if EXECUTION_MODE == 'Concurrent':
                    tasks = []
                    for key in list(failed_checks):
                        async def check_and_notify(key=key):
                            try:
                                attempts[key] += 1
                                if USE_SUBSTORE_CACHE and key in substore_cache:
                                    product_status = substore_cache[key]
                                    logger.info(f"[CACHE] Used substore cache for group {key}")
                                else:
                                    group_users = user_groups[key]
                                    pincode = group_users[0].get('pincode')
                                    product_status = await check_product_availability_async(pincode)

                                if product_status is not None:
                                    successfully_checked.add(key)
                                    passed_on_attempt[key] = attempt
                                    users = user_groups[key]
                                    notification_tasks = []
                                    for user in users:
                                        chat_id = user.get("chat_id")
                                        products_to_check = user.get("products", [])
                                        if chat_id and products_to_check:
                                            task = asyncio.create_task(
                                                send_telegram_notification_for_user(
                                                    app, chat_id, user.get('pincode'), products_to_check, product_status
                                                )
                                            )
                                            notification_tasks.append(task)
                                    if notification_tasks:
                                        await asyncio.gather(*notification_tasks, return_exceptions=True)
                                else:
                                    logger.warning(f"No product status returned for group {key}")
                            except Exception as e:
                                logger.error(f"Error checking or notifying for group {key}: {str(e)}")

                        tasks.append(asyncio.create_task(check_and_notify()))
                    
                    await asyncio.gather(*tasks, return_exceptions=True)
                    failed_checks.difference_update(successfully_checked)

                else:  # Sequential execution
                    for key in list(failed_checks):
                        try:
                            attempts[key] += 1
                            if USE_SUBSTORE_CACHE and key in substore_cache:
                                product_status = substore_cache[key]
                                logger.info(f"[CACHE] Used substore cache for group {key}")
                            else:
                                group_users = user_groups[key]
                                pincode = group_users[0].get('pincode')
                                product_status = await check_product_availability_async(pincode)

                            if product_status is not None:
                                successfully_checked.add(key)
                                failed_checks.remove(key)
                                passed_on_attempt[key] = attempt
                                users = user_groups[key]
                                notification_tasks = []
                                for user in users:
                                    chat_id = user.get("chat_id")
                                    products_to_check = user.get("products", [])
                                    if chat_id and products_to_check:
                                        task = asyncio.create_task(
                                            send_telegram_notification_for_user(
                                                app, chat_id, user.get('pincode'), products_to_check, product_status
                                            )
                                        )
                                        notification_tasks.append(task)
                                    if notification_tasks:
                                        await asyncio.gather(*notification_tasks, return_exceptions=True)
                            else:
                                logger.warning(f"No product status returned for group {key}")
                        except Exception as e:
                            logger.error(f"Error checking or notifying for group {key}: {str(e)}")

                if failed_checks:
                    logger.warning(f"Groups failed in attempt {attempt}: {[str(k) for k in sorted(failed_checks)]}")
                attempt += 1
            
            for k, att in passed_on_attempt.items():
                if att is not None and att > 1:
                    logger.info(f"Group {str(k)} failed in earlier attempts but passed in attempt {att}")
            if failed_checks:
                logger.error(f"The following groups failed completely after {MAX_ATTEMPTS} retries: {[str(k) for k in sorted(failed_checks)]}")
            logger.info(f"Final cache size: {len(pincode_cache)} pincodes, {len(substore_cache)} substores cached")
        
        except Exception as e:
            logger.error(f"Error in main processing: {str(e)}")
            raise
        finally:
            await app.shutdown()
            logger.info("Telegram application shutdown completed")
    
    finally:
        if USE_DATABASE and db:
            await db.close()
            logger.info("Database connection closed")