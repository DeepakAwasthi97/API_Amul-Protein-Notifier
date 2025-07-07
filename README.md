# Amul Protein Notifier

A modular, production-ready Telegram bot that checks Amul protein product availability and notifies users. 

## Features
- Checks product availability via Amul's API (no Selenium required)
- Notifies users on Telegram when products are in stock
- Caching and retry logic for reliability
- Configurable log rotation and error handling
- Modular codebase for easy maintenance

## Main Files
- `check_products.py` — Entrypoint script
- `product_checker.py` — Main orchestration logic
- `api_client.py` — API/session logic
- `notifier.py` — Telegram notification logic
- `substore_mapping.py` — Persistent substore mapping
- `cache.py` — In-memory cache dicts
- `utils.py` — Utility functions (logging, masking, etc.)
- `config.py` — All configuration (API, logging, cache, etc.)

## Requirements
- Python 3.8+
- Telegram Bot Token (set in `.env`)

## Setup
1. Clone the repo
2. Create a `.env` file with your Telegram bot token and any secrets
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the main script:
   ```bash
   python check_products.py
   ```

## Excluded from Public Repo
- `users.json`, `users.db`, `substore_list.py`, `.env`, logs, and backup/debug files are excluded for privacy and security.

## License
Amul Paglu
