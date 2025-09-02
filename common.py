import logging
import os
import psutil
from config import LOG_FILE, BASE_URL

PRODUCT_DATA = {
    "Any": {
        "display_name": "❗ Any of the products from the list",
        "slug": None,
        "temp_id": None,
        "category": None,
    },
    "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 8": {
        "display_name": "🍫 Chocolate Milkshake 180mL | Pack of 8",
        "slug": "amul-kool-protein-milkshake-or-chocolate-180-ml-or-pack-of-8",
        "temp_id": "akpmc8",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Chocolate, 180 mL | Pack of 30": {
        "display_name": "🍫🍫 Chocolate Milkshake 180mL | Pack of 30",
        "slug": "amul-kool-protein-milkshake-or-chocolate-180-ml-or-pack-of-30",
        "temp_id": "akpmc30",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 8": {
        "display_name": "☕ Coffee Milkshake 180mL | Pack of 8",
        "slug": "amul-kool-protein-milkshake-or-arabica-coffee-180-ml-or-pack-of-8",
        "temp_id": "akpmac8",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Arabica Coffee, 180 mL | Pack of 30": {
        "display_name": "☕☕ Coffee Milkshake 180mL | Pack of 30",
        "slug": "amul-kool-protein-milkshake-or-arabica-coffee-180-ml-or-pack-of-30",
        "temp_id": "akpmac30",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 8": {
        "display_name": "🌸 Kesar Milkshake 180mL | Pack of 8",
        "slug": "amul-kool-protein-milkshake-or-kesar-180-ml-or-pack-of-8",
        "temp_id": "akpmk8",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Kesar, 180 mL | Pack of 30": {
        "display_name": "🌸🌸 Kesar Milkshake 180mL | Pack of 30",
        "slug": "amul-kool-protein-milkshake-or-kesar-180-ml-or-pack-of-30",
        "temp_id": "akpmk30",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 8": {
        "display_name": "🍨 Vanilla Milkshake 180mL | Pack of 8",
        "slug": "amul-kool-protein-milkshake-or-vanilla-180-ml-or-pack-of-8",
        "temp_id": "akpmv8",
        "category": "Milkshakes & Shakes",
    },
    "Amul Kool Protein Milkshake | Vanilla, 180 mL | Pack of 30": {
        "display_name": "🍨🍨 Vanilla Milkshake 180mL | Pack of 30",
        "slug": "amul-kool-protein-milkshake-or-vanilla-180-ml-or-pack-of-30",
        "temp_id": "akpmv30",
        "category": "Milkshakes & Shakes",
    },
    "Amul High Protein Blueberry Shake, 200 mL | Pack of 30": {
        "display_name": "🫐🫐 Blueberry Shake 200mL | Pack of 30",
        "slug": "amul-high-protein-blueberry-shake-200-ml-or-pack-of-30",
        "temp_id": "ahpbbs30",
        "category": "Milkshakes & Shakes",
    },
    "Amul High Protein Plain Lassi, 200 mL | Pack of 30": {
        "display_name": "🥛🥛 Plain Lassi 200mL | Pack of 30",
        "slug": "amul-high-protein-plain-lassi-200-ml-or-pack-of-30",
        "temp_id": "ahppl30",
        "category": "Lassi & Buttermilk",
    },
    "Amul High Protein Rose Lassi, 200 mL | Pack of 30": {
        "display_name": "🌹🌹 Rose Lassi 200mL | Pack of 30",
        "slug": "amul-high-protein-rose-lassi-200-ml-or-pack-of-30",
        "temp_id": "ahprl30",
        "category": "Lassi & Buttermilk",
    },
    "Amul High Protein Buttermilk, 200 mL | Pack of 30": {
        "display_name": "🥛🥛 Buttermilk 200mL | Pack of 30",
        "slug": "amul-high-protein-buttermilk-200-ml-or-pack-of-30",
        "temp_id": "ahpbm20030",
        "category": "Lassi & Buttermilk",
    },
    "Amul High Protein Milk, 250 mL | Pack of 8": {
        "display_name": "🥛 Milk 250mL | Pack of 8",
        "slug": "amul-high-protein-milk-250-ml-or-pack-of-8",
        "temp_id": "ahpm2508",
        "category": "Milk",
    },
    "Amul High Protein Milk, 250 mL | Pack of 32": {
        "display_name": "🥛🥛 Milk 250mL | Pack of 32",
        "slug": "amul-high-protein-milk-250-ml-or-pack-of-32",
        "temp_id": "ahpm32",
        "category": "Milk",
    },
    "Amul High Protein Paneer, 400 g | Pack of 24": {
        "display_name": "🧀🧀 Paneer 400g | Pack of 24",
        "slug": "amul-high-protein-paneer-400-g-or-pack-of-24",
        "temp_id": "ahppr40024",
        "category": "Paneer",
    },
    "Amul High Protein Paneer, 400 g | Pack of 2": {
        "display_name": "🧀 Paneer 400g | Pack of 2",
        "slug": "amul-high-protein-paneer-400-g-or-pack-of-2",
        "temp_id": "ahppr4002",
        "category": "Paneer",
    },
    "Amul Whey Protein Gift Pack, 32 g | Pack of 10 sachets": {
        "display_name": "💪 Whey Protein 32g | Pack of 10 sachets",
        "slug": "amul-whey-protein-gift-pack-32-g-or-pack-of-10-sachets",
        "temp_id": "awpgp10",
        "category": "Whey Protein (Sachets)",
    },
    "Amul Whey Protein, 32 g | Pack of 30 Sachets": {
        "display_name": "💪💪 Whey Protein 32g | Pack of 30 Sachets",
        "slug": "amul-whey-protein-32-g-or-pack-of-30-sachets",
        "temp_id": "awp30",
        "category": "Whey Protein (Sachets)",
    },
    "Amul Whey Protein Pack, 32 g | Pack of 60 Sachets": {
        "display_name": "💪💪💪 Whey Protein 32g | Pack of 60 Sachets",
        "slug": "amul-whey-protein-32-g-or-pack-of-60-sachets",
        "temp_id": "awp60",
        "category": "Whey Protein (Sachets)",
    },
    "Amul Chocolate Whey Protein Gift Pack, 34 g | Pack of 10 sachets": {
        "display_name": "🍫 Chocolate Whey 34g | Pack of 10 sachets",
        "slug": "amul-chocolate-whey-protein-gift-pack-34-g-or-pack-of-10-sachets",
        "temp_id": "acwpgp10",
        "category": "Whey Protein (Sachets)",
    },
    "Amul Chocolate Whey Protein, 34 g | Pack of 30 sachets": {
        "display_name": "🍫🍫 Chocolate Whey 34g | Pack of 30 sachets",
        "slug": "amul-chocolate-whey-protein-34-g-or-pack-of-30-sachets",
        "temp_id": "acwp30",
        "category": "Whey Protein (Sachets)",
    },
    "Amul Chocolate Whey Protein, 34 g | Pack of 60 sachets": {
        "display_name": "🍫🍫🍫 Chocolate Whey 34g | Pack of 60 sachets",
        "slug": "amul-chocolate-whey-protein-34-g-or-pack-of-60-sachets",
        "temp_id": "acwp60",
        "category": "Whey Protein (Sachets)",
    },
}


# Generate derived data structures from the single source of truth
def generate_derived_structures():
    """Generate all the derived data structures from PRODUCT_DATA"""

    # Products list (excluding "Any")
    products = [name for name in PRODUCT_DATA.keys() if name != "Any"]

    # Product name mapping
    product_name_map = {
        name: data["display_name"] for name, data in PRODUCT_DATA.items()
    }

    # Categorized products
    categorized_products = {}
    for name, data in PRODUCT_DATA.items():
        if name == "Any" or data["category"] is None:
            continue
        category = data["category"]
        if category not in categorized_products:
            categorized_products[category] = []
        categorized_products[category].append(name)

    # Product slug mapping
    product_alias_map = {
        name: data["slug"]
        for name, data in PRODUCT_DATA.items()
        if data["slug"] is not None
    }

    return {
        "products": products,
        "product_name_map": product_name_map,
        "categorized_products": categorized_products,
        "product_alias_map": product_alias_map,
    }


# Generate all the derived structures
derived = generate_derived_structures()

# Expose the derived structures as module-level constants
PRODUCTS = derived["products"]
PRODUCT_NAME_MAP = derived["product_name_map"]
CATEGORIZED_PRODUCTS = derived["categorized_products"]
PRODUCT_ALIAS_MAP = derived["product_alias_map"]

# Categories list
CATEGORIES = list(CATEGORIZED_PRODUCTS.keys())

# Short to full mapping (reverse of product_name_map)
SHORT_TO_FULL = {v: k for k, v in PRODUCT_NAME_MAP.items()}


def get_product_info(identifier, return_field="display_name", search_by="name"):
    """
    Get product information by various identifier types.

    Args:
        identifier: The identifier to search for
        return_field: Field to return - "name", "display_name", "slug", "temp_id", "category", or "all"
        search_by: Field to search by - "name", "slug", "temp_id", or "display_name"

    Returns:
        str/dict: Product information if found, None otherwise
    """
    if search_by == "name":
        # Direct lookup by full product name
        if identifier in PRODUCT_DATA:
            data = PRODUCT_DATA[identifier]
            if return_field == "all":
                return {"name": identifier, **data}
            else:
                return data.get(return_field)

    else:
        # Use list comprehension to find the product by the specified identifier type
        matches = [
            {"name": name, **data}
            for name, data in PRODUCT_DATA.items()
            if data.get(search_by) == identifier
        ]

        if matches:
            data = matches[0]
            if return_field == "all":
                return data
            else:
                return data.get(return_field)

    return None


def create_product_markdown_link(product_name, base_url=None):
    """
    Create a markdown link for a single product.

    Args:
        product_name: The full product name to create a link for
        base_url: Base URL for the product link (defaults to config.BASE_URL)

    Returns:
        str: Markdown formatted link string, or just the display name if slug is not available
    """
    display_name = get_product_info(product_name, "display_name")

    if not display_name:
        # If we can't find the product, return the original name
        return product_name

    product_url = create_product_url(product_name, base_url)

    if product_url:
        return f"[{display_name}]({product_url})"
    else:
        return display_name


def create_product_list_markdown_links(product_names, base_url=None, separator="\n"):
    """
    Create markdown links for a list of products.

    Args:
        product_names: List of product names to create links for
        base_url: Base URL for the product links (defaults to config.BASE_URL)
        separator: String to separate multiple product links (defaults to newline)

    Returns:
        str: Markdown formatted links string with each product on a new line
    """
    if not product_names:
        return ""

    links = []
    for product_name in product_names:
        link = create_product_markdown_link(product_name, base_url)
        if link:
            links.append(f"- {link}")

    return separator.join(links)


def create_product_url(product_name, base_url=None):
    """
    Create a product URL for a single product.

    Args:
        product_name: The full product name to create a URL for
        base_url: Base URL for the product link (defaults to config.BASE_URL)

    Returns:
        str: Full product URL if slug is available, None otherwise
    """
    if base_url is None:
        base_url = BASE_URL

    slug = get_product_info(product_name, "slug")

    if slug:
        return f"{base_url}/en/product/{slug}"
    else:
        return None


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
                    logger.info(
                        "Found another running instance with PID %d", proc.info["pid"]
                    )
                    return True
            except (psutil.AccessDenied, psutil.NoSuchProcess) as e:
                logger.warning(
                    "Could not access process %d: %s", proc.info["pid"], str(e)
                )
                continue
    except Exception as e:
        logger.error("Error checking running processes: %s", str(e))
        return False

    logger.info("No other running instances found")
    return False
