import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests
import aiohttp
import time
import random
import hashlib
import json
import re
import asyncio
from urllib.parse import urlencode
from config import API_HEADERS, BASE_URL, PINCODE_URL, SETTINGS_URL, INFO_URL, API_URL, PRODUCT_API_DELAY_RANGE, GLOBAL_PRODUCT_API_RPS
from utils import setup_logging

logger = setup_logging()

# --- Global Rate Limiter ---
class AsyncRateLimiter:
    def __init__(self, rate_per_sec):
        self._interval = 1.0 / rate_per_sec
        self._lock = None
        self._last = 0.0
    async def wait(self):
        import asyncio
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            now = time.monotonic()
            wait_time = max(0, self._last + self._interval - now)
            if wait_time > 0:
                logger.info(f"[RATE LIMIT] Waiting {wait_time:.2f}s to respect global rate limit")
                await asyncio.sleep(wait_time)
            self._last = time.monotonic()

product_api_rate_limiter = AsyncRateLimiter(GLOBAL_PRODUCT_API_RPS)

def get_tid_and_substore(session, pincode):
    logger.info(f"[SESSION] Creating session and substore for pincode: {pincode}")
    headers = {
        "user-agent": API_HEADERS["user-agent"],
        "accept": "application/json, text/plain, */*",
        "referer": BASE_URL + "/en/browse/protein",
        "origin": BASE_URL,
        "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6",
        "x-amul-b2c-access-key": "shop.amul.com",
        "Connection": "keep-alive",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "base_url": "https://shop.amul.com/en/browse/protein",
        "frontend": "1",
        "priority": "u=1, i",
        "if-modified-since": "Tue, 01 Jul 2025 16:30:10 GMT"
    }
    browse_url = f"{BASE_URL}/en/browse/protein"
    logger.info(f"[SESSION] Visiting browse URL: {browse_url}")
    browse_resp = session.get(browse_url, headers=headers, timeout=10)
    logger.info(f"[SESSION] /en/browse/protein status: {browse_resp.status_code}")
    logger.info(f"[SESSION] /en/browse/protein response (first 300 chars): {browse_resp.text[:300]}")
    pincode_params = {
        "limit": 50,
        "filters[0][field]": "pincode",
        "filters[0][value]": str(pincode),
        "filters[0][operator]": "regex",
        "cf_cache": "1h"
    }
    dummy_tid = "dummy"
    tid_header = calculate_tid_header(dummy_tid)
    pincode_headers = headers.copy()
    pincode_headers["referer"] = BASE_URL + "/"
    pincode_headers["tid"] = tid_header
    pincode_url = PINCODE_URL + "?" + urlencode(pincode_params)
    logger.info(f"[SESSION] Looking up substore for pincode: {pincode_url}")
    pincode_resp = session.get(PINCODE_URL, headers=pincode_headers, params=pincode_params, timeout=10)
    logger.info(f"[SESSION] /entity/pincode status: {pincode_resp.status_code}")
    logger.info(f"[SESSION] /entity/pincode response (first 300 chars): {pincode_resp.text[:300]}")
    pincode_data = pincode_resp.json()
    records = pincode_data.get('records', [])
    if not records:
        logger.error(f"[SESSION] No substore found for pincode {pincode}")
        raise Exception(f"No substore found for pincode {pincode}")
    substore = records[0]['substore']
    substore_id = records[0]['_id']
    # Store raw substore for preferences
    raw_substore = substore
    # Normalize substore to a dictionary for return
    if isinstance(substore, str):
        logger.warning(f"[SESSION] Substore is a string for pincode {pincode}: {substore}. Converting to dict for return.")
        substore = {"alias": substore, "name": substore.title() or f"Unknown-{substore_id}"}
    elif not isinstance(substore, dict):
        logger.error(f"[SESSION] Unexpected substore type for pincode {pincode}: {type(substore)}. Converting to dict.")
        substore = {"alias": str(substore), "name": str(substore).title() or f"Unknown-{substore_id}"}
    pref_headers = headers.copy()
    pref_headers["content-type"] = "application/json"
    pref_headers["x-requested-with"] = "XMLHttpRequest"
    pref_headers["sec-fetch-mode"] = "cors"
    pref_headers["sec-fetch-site"] = "same-origin"
    pref_headers["tid"] = tid_header
    cookie_str = "; ".join([f"{k}={v}" for k, v in session.cookies.get_dict().items()])
    if cookie_str:
        pref_headers["cookie"] = cookie_str
    pref_payload = {"data": {"store": raw_substore}}
    pref_url = SETTINGS_URL
    logger.info(f"[SESSION] Setting preferences for substore: {raw_substore}")
    pref_resp = session.put(pref_url, headers=pref_headers, data=json.dumps(pref_payload), timeout=10)
    logger.info(f"[SESSION] setPreferences status: {pref_resp.status_code}")
    logger.info(f"[SESSION] setPreferences response (first 300 chars): {pref_resp.text[:300]}")
    if pref_resp.status_code == 406:
        logger.error(f"[SESSION] 406 Not Acceptable for setPreferences with payload: {json.dumps(pref_payload)}")
        raise Exception(f"setPreferences failed with 406 for pincode {pincode}")
    info_url = f"{INFO_URL}?_v={int(time.time()*1000)}"
    logger.info(f"[SESSION] Fetching info.js for session data: {info_url}")
    info_js = session.get(info_url, headers=headers, timeout=10)
    logger.info(f"[SESSION] /user/info.js status: {info_js.status_code}")
    logger.info(f"[SESSION] /user/info.js response (first 300 chars): {info_js.text[:300]}")
    tid_match = re.search(r'session\s*=\s*(\{.*\})', info_js.text, re.DOTALL)
    if not tid_match:
        logger.error(f"[SESSION] Could not extract session JSON from info.js for pincode {pincode}")
        raise Exception("Could not extract session JSON from info.js")
    session_data = json.loads(tid_match.group(1))
    tid = session_data.get("tid")
    js_substore_id = session_data.get("substore_id")
    js_substore_obj = session_data.get("substore", {})
    if js_substore_id:
        substore_id = js_substore_id
    elif js_substore_obj:
        substore_id = js_substore_obj.get("_id", substore_id)
    if not tid or not substore_id:
        logger.error(f"[SESSION] tid or substore_id not found in info.js JSON for pincode {pincode}")
        raise Exception("tid or substore_id not found in info.js JSON")
    logger.info(f"[SESSION] Session created: tid={tid}, substore_id={substore_id}")
    return tid, substore, substore_id

def fetch_product_data_for_alias(session, tid, substore_id, alias):
    calc_tid = calculate_tid_header(tid)
    headers = {
        "user-agent": API_HEADERS["user-agent"],
        "accept": "application/json, text/plain, */*",
        "referer": BASE_URL + "/en/browse/protein",
        "origin": BASE_URL,
        "accept-language": "en-US,en;q=0.9",
        "tid": calc_tid,
        "base_url": f"{BASE_URL}/en/browse/protein",
        "frontend": "1",
        "priority": "u=1, i",
        "x-amul-b2c-access-key": "shop.amul.com"
    }
    query = {
        "fields[name]": 1,
        "fields[alias]": 1,
        "fields[available]": 1,
        "filters[0][field]": "alias",
        "filters[0][value]": alias,
        "filters[0][operator]": "eq",
        "filters[0][original]": 1,
        "limit": 1,
        "substore": substore_id
    }
    product_url = API_URL + "?" + urlencode(query, doseq=True)
    logger.info(f"[SESSION] Fetching product data for alias '{alias}': {product_url}")
    resp = session.get(API_URL, headers=headers, params=query, timeout=10)
    logger.info(f"[SESSION] Product API status for alias '{alias}': {resp.status_code}")
    logger.info(f"[SESSION] Product API response for alias '{alias}' (first 300 chars): {resp.text[:300]}")
    try:
        return resp.json().get("data", [])
    except Exception as e:
        logger.error(f"[SESSION] Error parsing product API response for alias '{alias}': {str(e)}")
        return []

async def fetch_product_data_for_alias_async(session, tid, substore_id, alias, semaphore, max_retries=3):
    calc_tid = calculate_tid_header(tid)
    headers = {
        "user-agent": API_HEADERS["user-agent"],
        "accept": "application/json, text/plain, */*",
        "referer": BASE_URL + "/en/browse/protein",
        "origin": BASE_URL,
        "accept-language": "en-US,en;q=0.9",
        "x-amul-b2c-access-key": "shop.amul.com",
        "tid": calc_tid
    }
    query = {
        "q": json.dumps({"alias": alias}),
        "limit": 1
    }
    product_url = API_URL + "?" + urlencode(query)
    for attempt in range(1, max_retries + 1):
        async with semaphore:
            await product_api_rate_limiter.wait()
            await asyncio.sleep(random.uniform(*PRODUCT_API_DELAY_RANGE))
            try:
                async with session.get(API_URL, headers=headers, params=query, timeout=10) as resp:
                    text = await resp.text()
                    logger.info(f"[SESSION] Product API status for alias '{alias}': {resp.status}")
                    logger.info(f"[SESSION] Product API response for alias '{alias}' (first 300 chars): {text[:300]}")
                    if resp.status == 406:
                        logger.warning(f"406 Not Acceptable for alias '{alias}', attempt {attempt}")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if resp.status >= 500:
                        logger.warning(f"Server error {resp.status} for alias '{alias}', attempt {attempt}")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    try:
                        data = await resp.json()
                        return data.get("data", [])
                    except Exception as e:
                        logger.error(f"[SESSION] Error parsing product API response for alias '{alias}': {str(e)}")
                        return []
            except Exception as e:
                logger.error(f"[SESSION] Network error for alias '{alias}', attempt {attempt}: {str(e)}")
                await asyncio.sleep(2 ** attempt)
    logger.error(f"[SESSION] Failed to fetch product data for alias '{alias}' after {max_retries} attempts.")
    return []

def calculate_tid_header(session_tid):
    store_id = '62fa94df8c13af2e242eba16'
    timestamp = str(int(time.time() * 1000))
    rand = str(random.randint(0, 1000))
    base = f"{store_id}:{timestamp}:{rand}:{session_tid}"
    hash_bytes = hashlib.sha256(base.encode('utf-8')).hexdigest()
    return f"{timestamp}:{rand}:{hash_bytes}"