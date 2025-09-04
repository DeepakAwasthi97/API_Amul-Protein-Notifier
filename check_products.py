import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from utils import setup_logging
from common import is_already_running
import time
import signal
from product_checker import check_products_for_users
import asyncio
import config
from database import Database


async def main_async():
    logger = setup_logging()
    logger.info("Starting API-based product check script")

    def handle_shutdown(signum, frame):
        logger.info("Received shutdown signal, exiting...")
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        if is_already_running("check_products.py"):
            logger.error(
                "Another instance of check_products.py is already running. Exiting..."
            )
            raise SystemExit(1)

        # Initialize database
        db = Database(config.DATABASE_URL)
        logger.info("Initializing database...")
        await db._init_db()  # Explicitly call _init_db
        logger.info("Database initialized successfully")

        await check_products_for_users(db)

    except KeyboardInterrupt:
        logger.info("Main process interrupted, exiting cleanly...")
        await db.close()
        raise SystemExit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        await db.close()
        raise SystemExit(1)


def main():
    start_time = time.time()
    if sys.platform == "win32":
        try:
            os.system("chcp 65001 >nul 2>&1")
            if hasattr(sys.stdout, "reconfigure"):
                sys.stdout.reconfigure(encoding="utf-8")
            if hasattr(sys.stderr, "reconfigure"):
                sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    logger = setup_logging()
    try:
        asyncio.run(main_async())
        total_time = time.time() - start_time
        minutes, seconds = divmod(total_time, 60)
        logger.info(
            f"Total execution time: {int(minutes)} minutes {seconds:.2f} seconds"
        )
        print(f"Total execution time: {int(minutes)} minutes {seconds:.2f} seconds")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
