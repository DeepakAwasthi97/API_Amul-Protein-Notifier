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

- Python 3.12+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) - Fast Python package installer and resolver
- Generate a Telegram Bot TOKEN for a dummy bot of your own using the @BotFather official bot on telegram and store it in .env

## Setup

1. Clone the repository

2. Create a `.env` file with your configuration. You can copy from `.env.example` as a template:
   ```bash
   cp .env.example .env
   ```

   Required environment variables:
   - `TELEGRAM_BOT_TOKEN` - Your Telegram bot token from @BotFather
   - `ADMIN_CHAT_ID` - Your Telegram chat ID for admin notifications
   - `DATABASE_URL` - PostgreSQL connection string


3. Install uv (if not already installed):

   **macOS and Linux:**
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   **Windows:**
   ```powershell
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

   **Alternative methods:**
   - PyPI: `pipx install uv` or `pip install uv`
   - Homebrew: `brew install uv`
   - WinGet: `winget install --id=astral-sh.uv -e`

   For more installation options, see the [official uv installation guide](https://docs.astral.sh/uv/getting-started/installation/).

4. Install dependencies:

   ```bash
   uv sync
   ```

4. Consider downloading and installing DB Browser

5. Run the application:

   **Start the Telegram bot:**
   ```bash
   uv run main.py
   ```
   This starts the Telegram bot that users can interact with to subscribe/unsubscribe to notifications.

   **Fetch product details and notify users:**
   ```bash
   uv run check_products.py
   ```
   This script fetches product availability from the Amul website and sends notifications to subscribed users.

## Excluded from Public Repo

- `users.json`, `users.db`, `substore_list.py`, `.env`, logs, and backup/debug files are excluded for privacy and security.

## License

Amul Paglu
