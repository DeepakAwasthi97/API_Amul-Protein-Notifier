import logging
import os
import psutil
from config import LOG_FILE

# Constants
PRODUCTS = [
    "Any",
    "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30",
    "Amul High Protein Blueberry Shake, 200 mL | Pack of 30",
    "Amul High Protein Plain Lassi, 200 mL | Pack of 30",
    "Amul High Protein Rose Lassi, 200 mL | Pack of 30",
    "Amul High Protein Buttermilk, 200 mL | Pack of 30",
    "Amul High Protein Milk, 250 mL | Pack of 8",
    "Amul High Protein Milk, 250 mL | Pack of 32",
    "Amul High Protein Paneer, 400 g | Pack of 24",
    "Amul High Protein Paneer, 400 g | Pack of 2",
    "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets",
    "Amul Whey Protein, 32 g | Pack of 30 Sachets",
    "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets",
    "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets",
    "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets",
    "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets",
]

PRODUCT_NAME_MAP = {
    "Any": "❗ Any of the products from the list",
    "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30": "🍫🍫 Chocolate Milkshake 180mL | Pack of 30",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8": "☕ Coffee Milkshake 180mL | Pack of 8",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30": "☕☕ Coffee Milkshake 180mL | Pack of 30",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8": "🌸 Kesar Milkshake 180mL | Pack of 8",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30": "🌸🌸 Kesar Milkshake 180mL | Pack of 30",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8": "🍨 Vanilla Milkshake 180mL | Pack of 8",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30": "🍨🍨 Vanilla Milkshake 180mL | Pack of 30",
    "Amul High Protein Blueberry Shake, 200 mL | Pack of 30": "🫐🫐 Blueberry Shake 200mL | Pack of 30",
    "Amul High Protein Plain Lassi, 200 mL | Pack of 30": "🥛🥛 Plain Lassi 200mL | Pack of 30",
    "Amul High Protein Rose Lassi, 200 mL | Pack of 30": "🌹🌹 Rose Lassi 200mL | Pack of 30",
    "Amul High Protein Buttermilk, 200 mL | Pack of 30": "🥛🥛 Buttermilk 200mL | Pack of 30",
    "Amul High Protein Milk, 250 mL | Pack of 8": "🥛 Milk 250mL | Pack of 8",
    "Amul High Protein Milk, 250 mL | Pack of 32": "🥛🥛 Milk 250mL | Pack of 32",
    "Amul High Protein Paneer, 400 g | Pack of 24": "🧀🧀 Paneer 400g | Pack of 24",
    "Amul High Protein Paneer, 400 g | Pack of 2": "🧀 Paneer 400g | Pack of 2",
    "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets": "💪 Whey Protein 32g | Pack of 10 sachets",
    "Amul Whey Protein, 32 g | Pack of 30 Sachets": "💪💪 Whey Protein 32g | Pack of 30 Sachets",
    "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets": "💪💪💪 Whey Protein 32g | Pack of 60 Sachets",
    "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets": "🍫 Chocolate Whey 34g | Pack of 10 sachets",
    "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets": "🍫🍫 Chocolate Whey 34g | Pack of 30 sachets",
    "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets": "🍫🍫🍫 Chocolate Whey 34g | Pack of 60 sachets",
}

CATEGORIZED_PRODUCTS = {
    "Milkshakes & Shakes": [
        "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30",
        "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8",
        "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30",
        "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8",
        "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30",
        "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8",
        "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30",
        "Amul High Protein Blueberry Shake, 200 mL | Pack of 30",
    ],
    "Lassi & Buttermilk": [
        "Amul High Protein Plain Lassi, 200 mL | Pack of 30",
        "Amul High Protein Rose Lassi, 200 mL | Pack of 30",
        "Amul High Protein Buttermilk, 200 mL | Pack of 30",
    ],
    "Milk": [
        "Amul High Protein Milk, 250 mL | Pack of 8",
        "Amul High Protein Milk, 250 mL | Pack of 32",
    ],
    "Paneer": [
        "Amul High Protein Paneer, 400 g | Pack of 24",
        "Amul High Protein Paneer, 400 g | Pack of 2",
    ],
    "Whey Protein (Sachets)": [
        "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets",
        "Amul Whey Protein, 32 g | Pack of 30 Sachets",
        "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets",
        "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets",
        "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets",
        "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets",
    ],
}

CATEGORIES = list(CATEGORIZED_PRODUCTS.keys())

PRODUCT_ALIAS_MAP = {
    "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30": "amul-kool-protein-milkshake-or-chocolate-180-ml-or-pack-of-30",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8": "amul-kool-protein-milkshake-or-arabica-coffee-180-ml-or-pack-of-8",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30": "amul-kool-protein-milkshake-or-arabica-coffee-180-ml-or-pack-of-30",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8": "amul-kool-protein-milkshake-or-kesar-180-ml-or-pack-of-8",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30": "amul-kool-protein-milkshake-or-kesar-180-ml-or-pack-of-30",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8": "amul-kool-protein-milkshake-or-vanilla-180-ml-or-pack-of-8",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30": "amul-kool-protein-milkshake-or-vanilla-180-ml-or-pack-of-30",
    "Amul High Protein Blueberry Shake, 200 mL | Pack of 30": "amul-high-protein-blueberry-shake-200-ml-or-pack-of-30",
    "Amul High Protein Plain Lassi, 200 mL | Pack of 30": "amul-high-protein-plain-lassi-200-ml-or-pack-of-30",
    "Amul High Protein Rose Lassi, 200 mL | Pack of 30": "amul-high-protein-rose-lassi-200-ml-or-pack-of-30",
    "Amul High Protein Buttermilk, 200 mL | Pack of 30": "amul-high-protein-buttermilk-200-ml-or-pack-of-30",
    "Amul High Protein Milk, 250 mL | Pack of 8": "amul-high-protein-milk-250-ml-or-pack-of-8",
    "Amul High Protein Milk, 250 mL | Pack of 32": "amul-high-protein-milk-250-ml-or-pack-of-32",
    "Amul High Protein Paneer, 400 g | Pack of 24": "amul-high-protein-paneer-400-g-or-pack-of-24",
    "Amul High Protein Paneer, 400 g | Pack of 2": "amul-high-protein-paneer-400-g-or-pack-of-2",
    "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets": "amul-whey-protein-gift-pack-32-g-or-pack-of-10-sachets",
    "Amul Whey Protein, 32 g | Pack of 30 Sachets": "amul-whey-protein-32-g-or-pack-of-30-sachets",
    "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets": "amul-whey-protein-32-g-or-pack-of-60-sachets",
    "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets": "amul-chocolate-whey-protein-gift-pack-34-g-or-pack-of-10-sachets",
    "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets": "amul-chocolate-whey-protein-34-g-or-pack-of-30-sachets",
    "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets": "amul-chocolate-whey-protein-34-g-or-pack-of-60-sachets",
}

TEMP_PRDCT_TO_ID_MAP = {
    "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30": "akpmc30",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8": "akpmac8",
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30": "akpmac30",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8": "akpmk8",
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30": "akpmk30",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8": "akpmv8",
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30": "akpmv30",
    "Amul High Protein Blueberry Shake, 200 mL | Pack of 30": "ahpbbs30",
    "Amul High Protein Plain Lassi, 200 mL | Pack of 30": "ahppl30",
    "Amul High Protein Rose Lassi, 200 mL | Pack of 30": "ahprl30",
    "Amul High Protein Buttermilk, 200 mL | Pack of 30": "ahpbm20030",
    "Amul High Protein Milk, 250 mL | Pack of 8": "ahpm2508",
    "Amul High Protein Milk, 250 mL | Pack of 32": "ahpm32",
    "Amul High Protein Paneer, 400 g | Pack of 24": "ahppr40024",
    "Amul High Protein Paneer, 400 g | Pack of 2": "ahppr4002",
    "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets": "awpgp10",
    "Amul Whey Protein, 32 g | Pack of 30 Sachets": "awp30",
    "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets": "awp60",
    "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets": "acwpgp10",
    "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets": "acwp30",
    "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets": "acwp60"
}

TEMP_ID_TO_PRDCT_MAP = {
    "akpmc30": "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30",
    "akpmac8": "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8",
    "akpmac30": "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30",
    "akpmk8": "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8",
    "akpmk30": "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30",
    "akpmv8": "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8",
    "akpmv30": "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30",
    "ahpbbs30": "Amul High Protein Blueberry Shake, 200 mL | Pack of 30",
    "ahppl30": "Amul High Protein Plain Lassi, 200 mL | Pack of 30",
    "ahprl30": "Amul High Protein Rose Lassi, 200 mL | Pack of 30",
    "ahpbm20030": "Amul High Protein Buttermilk, 200 mL | Pack of 30",
    "ahpm2508": "Amul High Protein Milk, 250 mL | Pack of 8",
    "ahpm32": "Amul High Protein Milk, 250 mL | Pack of 32",
    "ahppr40024": "Amul High Protein Paneer, 400 g | Pack of 24",
    "ahppr4002": "Amul High Protein Paneer, 400 g | Pack of 2",
    "awpgp10": "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets",
    "awp30": "Amul Whey Protein, 32 g | Pack of 30 Sachets",
    "awp60": "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets",
    "acwpgp10": "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets",
    "acwp30": "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets",
    "acwp60": "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets",
}

SHORT_TO_FULL = {v: k for k, v in PRODUCT_NAME_MAP.items()}

# Logging setup
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE),
        ],
    )
    return logging.getLogger(__name__)

# Helper functions
def mask(value, visible=2):
    value = str(value)
    if len(value) <= visible * 2:
        return "*" * len(value)
    return value[:visible] + "*" * (len(value) - 2 * visible) + value[-visible:]

def is_already_running(script_name):
    logger = logging.getLogger(__name__)
    logger.info("Checking for running instances of %s", script_name)
    current_pid = os.getpid()
    
    try:
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if (
                    proc.info["name"].lower() == "python"
                    and proc.info["cmdline"]
                    and script_name in " ".join(proc.info["cmdline"]).lower()
                    and proc.info["pid"] != current_pid
                ):
                    logger.info("Found another running instance with PID %d", proc.info["pid"])
                    return True
            except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
                logger.warning("Could not access process %d: %s", proc.info["pid"], str(e))
                continue
    except Exception as e:
        logger.error("Error checking running processes: %s", str(e))
        return False
    
    logger.info("No other running instances found")
    return False
