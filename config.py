import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Database Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")  # PostgreSQL connection string (required)
DATABASE_FILE = os.getenv(
    "DATABASE_FILE", "users.db"
)  # SQLite file path (optional, for migration)
# --- Secrets and Environment-Specific ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

# --- Sentry Configuration ---
SENTRY_DSN = os.getenv("SENTRY_DSN")
SENTRY_ENVIRONMENT = os.getenv("SENTRY_ENVIRONMENT", "production")

# --- Concurrency Settings ---
SEMAPHORE_LIMIT = 1
NOTIFICATION_CONCURRENCY_LIMIT = int(
    os.getenv("NOTIFICATION_CONCURRENCY_LIMIT", 30)
)  # Default to 30 for Telegram limit
MAX_RETRY = 1

# --- File Paths ---
LOG_FILE = "product_check.log"

# --- API Configuration ---
BASE_URL = "https://shop.amul.com"
PROTEIN_URL = f"{BASE_URL}/en/browse/protein"
API_URL = f"{BASE_URL}/api/1/entity/ms.products"
PINCODE_URL = f"{BASE_URL}/entity/pincode"
SETTINGS_URL = f"{BASE_URL}/entity/ms.settings/_/setPreferences"
INFO_URL = f"{BASE_URL}/user/info.js"

# Session management
COOKIE_REFRESH_INTERVAL = 1200

# API Headers
API_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6",
    "frontend": "1",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "referer": BASE_URL,
    "x-requested-with": "XMLHttpRequest",
    "sec-gpc": "1",
    "priority": "u=1, i",
    "content-type": "application/json",
}

# --- Substore Mapping ---
USE_SUBSTORE_CACHE = True
FALLBACK_TO_PINCODE_CACHE = True
SUBSTORE_LIST_FILE = "substore_list.py"

# --- Rate Limiting Settings ---
PRODUCT_API_DELAY_RANGE = (1.0, 2.0)
GLOBAL_PRODUCT_API_RPS = 5

# --- Logging and Monitoring ---
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
MAX_OF_DAYS = 1

# --- Execution Mode ---
EXECUTION_MODE = "Concurrent"
