"""
Amul Protein Product Availability Checker (API-only, robust, fast)
- No Selenium or HTML parsing
- Uses robust session and substore logic
- Checks product stock using: available == 1 and substore_id in seller_substore_ids
"""
import time
import json
import logging
import requests
import re
import hashlib
import random
import cloudscraper
from urllib.parse import urlencode

# --- Configuration ---
BASE_URL = "https://shop.amul.com"
PRODUCTS_API_URL = f"{BASE_URL}/api/1/entity/ms.products"
USER_INFO_JS = f"{BASE_URL}/user/info.js"
PINCODE_API_URL = f"{BASE_URL}/entity/pincode"
PINCODE_TO_TEST = "201014"   # Single pincode as string
PRODUCT_ALIAS_TO_TEST = [
    "amul-kool-protein-milkshake-or-chocolate-180-ml-or-pack-of-30",
    "amul-kool-protein-milkshake-or-arabica-coffee-180-ml-or-pack-of-8",
    "amul-kool-protein-milkshake-or-arabica-coffee-180-ml-or-pack-of-30",
    "amul-kool-protein-milkshake-or-kesar-180-ml-or-pack-of-8",
    "amul-kool-protein-milkshake-or-kesar-180-ml-or-pack-of-30",
    "amul-kool-protein-milkshake-or-vanilla-180-ml-or-pack-of-8",
    "amul-kool-protein-milkshake-or-vanilla-180-ml-or-pack-of-30",
    "amul-high-protein-blueberry-shake-200-ml-or-pack-of-30",
    "amul-high-protein-plain-lassi-200-ml-or-pack-of-30",
    "amul-high-protein-rose-lassi-200-ml-or-pack-of-30",
    "amul-high-protein-buttermilk-200-ml-or-pack-of-30",
    "amul-high-protein-milk-250-ml-or-pack-of-8",
    "amul-high-protein-milk-250-ml-or-pack-of-32",
    "amul-high-protein-paneer-400-g-or-pack-of-24",
    "amul-high-protein-paneer-400-g-or-pack-of-2",
    "amul-whey-protein-gift-pack-32-g-or-pack-of-10-sachets",
    "amul-whey-protein-32-g-or-pack-of-30-sachets",
    "amul-whey-protein-32-g-or-pack-of-60-sachets",
    "amul-chocolate-whey-protein-gift-pack-34-g-or-pack-of-10-sachets",
    "amul-chocolate-whey-protein-34-g-or-pack-of-30-sachets",
    "amul-chocolate-whey-protein-34-g-or-pack-of-60-sachets"
]

logger = None

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    ENDC = '\033[0m'

def setup_logging():
    global logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

def get_tid_and_substore(session, pincode):
    """
    Unified function to get tid, substore, and substore_id for a single pincode.
    Handles all session/cookie logic. Use this everywhere.
    """
    # Use a real Chrome version and all headers from the working curl
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
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
    # 1. Visit /en/browse/protein to get initial cookies
    browse_url = f"{BASE_URL}/en/browse/protein"
    print(f"[DEBUG] GET: {browse_url}")
    browse_resp = session.get(browse_url, headers=headers)
    print(f"[DEBUG] /en/browse/protein status: {browse_resp.status_code}")
    # print(f"[DEBUG] /en/browse/protein headers: {dict(browse_resp.headers)}")
    # print(f"[DEBUG] /en/browse/protein cookies: {session.cookies.get_dict()}")
    # print(f"[DEBUG] /en/browse/protein body (first 500 chars): {browse_resp.text[:500]}")
    # 2. Lookup substore for pincode using the pincode API
    pincode_params = {
        "limit": 50,
        "filters[0][field]": "pincode",
        "filters[0][value]": str(pincode),
        "filters[0][operator]": "regex",
        "cf_cache": "1h"
    }
    # Calculate TID for this request as well
    # We'll use a dummy session_tid for now, as info.js is not yet fetched
    dummy_tid = "dummy"
    tid_header = calculate_tid_header(dummy_tid)
    pincode_headers = headers.copy()
    pincode_headers["referer"] = BASE_URL + "/"  # as in curl
    pincode_headers["tid"] = tid_header
    pincode_url = PINCODE_API_URL + "?" + urlencode(pincode_params)
    print(f"[DEBUG] GET: {pincode_url}")
    pincode_resp = session.get(PINCODE_API_URL, headers=pincode_headers, params=pincode_params)
    print(f"[DEBUG] /entity/pincode status: {pincode_resp.status_code}")
    # print(f"[DEBUG] /entity/pincode headers: {dict(pincode_resp.headers)}")
    # print(f"[DEBUG] /entity/pincode cookies: {session.cookies.get_dict()}")
    # logger.info(f"/entity/pincode body: {pincode_resp.text}")
    try:
        pincode_data = pincode_resp.json()
    except Exception as e:
        logger.error(f"Pincode API returned invalid JSON for pincode {pincode}: {pincode_resp.text}")
        raise Exception(f"Pincode API returned invalid JSON for pincode {pincode}: {str(e)}")
    records = pincode_data.get('records', [])
    if not records:
        raise Exception(f"No substore found for pincode {pincode}")
    substore = records[0]['substore']
    substore_id = records[0]['_id']
    # 3. Set substore via preferences API (match amul-notify repo)
    pref_headers = headers.copy()
    pref_headers["content-type"] = "application/json"
    pref_headers["x-requested-with"] = "XMLHttpRequest"
    # Add sec- headers if missing
    pref_headers["sec-fetch-mode"] = "cors"
    pref_headers["sec-fetch-site"] = "same-origin"
    # Calculate a tid for this call (repo does this)
    pref_headers["tid"] = tid_header
    # Explicitly set the cookie header (repo does this)
    cookie_str = "; ".join([f"{k}={v}" for k, v in session.cookies.get_dict().items()])
    if cookie_str:
        pref_headers["cookie"] = cookie_str
    # Use only {data: {store: substore}} as payload
    pref_payload = {"data": {"store": substore}}
    pref_url = f"{BASE_URL}/entity/ms.settings/_/setPreferences"
    print(f"[DEBUG] PUT: {pref_url}")
    # print(f"[DEBUG] setPreferences headers: {pref_headers}")
    # print(f"[DEBUG] setPreferences payload: {json.dumps(pref_payload)}")
    pref_resp = session.put(pref_url, headers=pref_headers, data=json.dumps(pref_payload))
    print(f"[DEBUG] setPreferences status: {pref_resp.status_code}")
    # print(f"[DEBUG] setPreferences headers: {dict(pref_resp.headers)}")
    # print(f"[DEBUG] setPreferences cookies: {session.cookies.get_dict()}")
    print(f"[DEBUG] setPreferences body: {pref_resp.text}")
    info_url = f"{USER_INFO_JS}?_v={int(time.time()*1000)}"
    print(f"[DEBUG] GET: {info_url}")
    info_js = session.get(info_url, headers=headers)
    if not info_js.text.strip():
        logger.error(f"/user/info.js returned empty response for pincode {pincode}")
        raise Exception("/user/info.js returned empty response")
    tid_match = re.search(r'session\s*=\s*(\{.*\})', info_js.text, re.DOTALL)
    if not tid_match:
        logger.error(f"/user/info.js full response (first 1000 chars): {info_js.text[:1000]}")
        raise Exception("Could not extract session JSON from info.js")
    try:
        session_data = json.loads(tid_match.group(1))
    except Exception as e:
        logger.error(f"Failed to parse session JSON from info.js for pincode {pincode}: {tid_match.group(1)}")
        raise Exception(f"Failed to parse session JSON from info.js for pincode {pincode}: {str(e)}")
    tid = session_data.get("tid")
    js_substore_id = session_data.get("substore_id")
    js_substore_obj = session_data.get("substore", {})
    if js_substore_id:
        substore_id = js_substore_id
    elif js_substore_obj:
        substore_id = js_substore_obj.get("_id", substore_id)
    if not tid or not substore_id:
        raise Exception("tid or substore_id not found in info.js JSON")
    return tid, substore, substore_id

def calculate_tid_header(session_tid):
    store_id = '62fa94df8c13af2e242eba16'
    timestamp = str(int(time.time() * 1000))
    rand = str(random.randint(0, 1000))
    base = f"{store_id}:{timestamp}:{rand}:{session_tid}"
    hash_bytes = hashlib.sha256(base.encode('utf-8')).hexdigest()
    return f"{timestamp}:{rand}:{hash_bytes}"

def fetch_product_data(session, tid, substore_id):
    logger.info("Fetching product data via API-only session...")
    calc_tid = calculate_tid_header(tid)
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
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
        "filters[0][field]": "categories",
        "filters[0][value][0]": "protein",
        "filters[0][operator]": "in",
        "filters[0][original]": 1,
        "facets": "true",
        "facetgroup": "default_category_facet",
        "limit": 50,
        "start": 0,
        "cdc": "1m",
        "substore": substore_id
    }
    product_url = PRODUCTS_API_URL + "?" + urlencode(query, doseq=True)
    print(f"[DEBUG] GET: {product_url}")
    resp = session.get(PRODUCTS_API_URL, headers=headers, params=query)
    # logger.info(f"Product API Response: {json.dumps(resp.json(), indent=2)}")
    return resp.json().get("data", [])

def check_specific_product_availability(session, product_alias, substore_id, pincode, log=False):
    """
    Check availability of a specific product alias for a given substore_id and pincode.
    Returns a tuple: (product_name, status_str)
    """
    # We need the session tid for this request, so fetch from info.js again
    headers_info = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "referer": BASE_URL + "/en/browse/protein",
        "origin": BASE_URL,
        "accept-language": "en-US,en;q=0.9",
        "x-amul-b2c-access-key": "shop.amul.com"
    }
    info_url = f"{USER_INFO_JS}?_v={int(time.time()*1000)}"
    print(f"[DEBUG] GET: {info_url}")
    info_js = session.get(info_url, headers=headers_info)
    tid_match = re.search(r'session\s*=\s*(\{.*\})', info_js.text, re.DOTALL)
    if not tid_match:
        return (product_alias, f"{Colors.RED}TID NOT FOUND{Colors.ENDC}")
    session_data = json.loads(tid_match.group(1))
    session_tid = session_data.get("tid")
    calc_tid = calculate_tid_header(session_tid)
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "referer": BASE_URL + "/en/browse/protein",
        "origin": BASE_URL,
        "accept-language": "en-US,en;q=0.9",
        "x-amul-b2c-access-key": "shop.amul.com",
        "tid": calc_tid
    }
    query = {
        "q": json.dumps({"alias": product_alias}),
        "limit": 1
    }
    product_url = PRODUCTS_API_URL + "?" + urlencode(query)
    print(f"[DEBUG] GET: {product_url}")
    resp = session.get(PRODUCTS_API_URL, headers=headers, params=query)
    if log:
        logger.info(f"Specific Product API Response: {json.dumps(resp.json(), indent=2)}")
    data = resp.json().get("data", [])
    if not data:
        return (product_alias, f"{Colors.RED}NOT FOUND for pincode {pincode}{Colors.ENDC}")
    product = data[0]
    available = int(product.get("available", 0))
    seller_substore_ids = product.get("seller_substore_ids", [])
    if substore_id in seller_substore_ids and available == 1:
        return (product.get('name', product_alias), f"{Colors.GREEN}IN STOCK{Colors.ENDC}")
    else:
        return (product.get('name', product_alias), f"{Colors.RED}OUT OF STOCK{Colors.ENDC}")

def main():
    setup_logging()
    print(f"{Colors.BLUE}--- Amul Product Checker [API Emulation] ---{Colors.ENDC}")
    start_time = time.time()  # Start timing
    try:
        # Use PINCODE_TO_TEST directly as a string
        PINCODE = PINCODE_TO_TEST
        print(f"\nChecking availability for pincode: {PINCODE}")
        session = cloudscraper.create_scraper()  # Use cloudscraper instead of requests.Session()
        tid, substore, substore_id = get_tid_and_substore(session, PINCODE)
        products = fetch_product_data(session, tid, substore_id)
        if not products:
            print(f"{Colors.RED}No products returned in API response for pincode {PINCODE}{Colors.ENDC}")
            return
        # Print first product status for demo
        product = products[0]
        name = product.get("name", "N/A")
        alias = product.get("alias", "N/A")
        status = check_specific_product_availability(session, alias, substore_id, PINCODE)[1]
        print(f"-> {name} ({alias}): {status}")
        # Check all product aliases and collect results
        results = []
        for alias in PRODUCT_ALIAS_TO_TEST:
            result = check_specific_product_availability(session, alias, substore_id, PINCODE, log=False)
            results.append(result)
        print(f"\n--- Availability Status for All Products (Pincode: {PINCODE}) ---")
        for product_name, status_str in results:
            print(f"{product_name}: {status_str}")
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        print(f"\n{Colors.RED}ERROR: {str(e)}{Colors.ENDC}")
    finally:
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"\n{Colors.YELLOW}Total execution time: {elapsed:.2f} seconds{Colors.ENDC}")

if __name__ == "__main__":
    main()
