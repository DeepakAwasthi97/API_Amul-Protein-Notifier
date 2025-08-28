# Amul Protein Notifier

A modular, production-ready Telegram bot that checks Amul protein product availability and notifies users.

## Features

- Checks product availability via Amul's API (no Selenium required)
- Notifies users on Telegram when products are in stock based on their pincode
- Caching and retry logic for reliability
- Configurable log rotation and error handling
- Modular codebase for easy maintenance
- Customizable product selection to only receive certain notifications

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
- Generate a Telegram Bot TOKEN for a dummy bot of your own using the @BotFather official bot on telegram and store it in .env

## Setup

1. Clone the repo
2. Create a `.env` file with your Telegram bot token and other secrets, 
   primarily the keys required will be TELEGRAM_BOT_TOKEN and ADMIN_CHAT_ID
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   ```bash
   pip install aiosqlite
   ```
4. Consider downloading and installing DB Browser
5. Run the main script:
   ```bash
   python check_products.py
   ```

## You may need to run the below command, subjective to any warnings you get in the console

```bash
pip install "python-telegram-bot[job-queue]"
```

## Excluded from Public Repo

- `users.json`, `users.db`, `substore_list.py`, `.env`, logs, and backup/debug files are excluded for privacy and security.

## License

Amul Paglu
