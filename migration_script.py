import asyncio
import aiosqlite
import json
import logging
import os
import requests
from dotenv import load_dotenv
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

async def init_db(db_file):
    """Initialize the database and create users table."""
    try:
        async with aiosqlite.connect(db_file) as conn:
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            await conn.commit()
            logger.info("Database initialized with users table")
    except aiosqlite.Error as e:
        logger.error(f"Error initializing database: {e}")
        raise

async def import_users_to_db(db_file, users):
    """Import users into the database."""
    try:
        async with aiosqlite.connect(db_file) as conn:
            for user in users:
                # Validate and normalize user data
                if not isinstance(user, dict) or "chat_id" not in user:
                    logger.warning(f"Skipping invalid user data: {user}")
                    continue

                # Ensure required fields
                if "notification_preference" not in user:
                    user["notification_preference"] = "until_stop"
                if "last_notified" not in user:
                    user["last_notified"] = {}
                if "products" not in user:
                    user["products"] = ["Any"]
                if "active" not in user:
                    user["active"] = True

                # Ensure chat_id in data matches the key
                user["chat_id"] = str(user["chat_id"])

                try:
                    await conn.execute(
                        "INSERT OR REPLACE INTO users (chat_id, data) VALUES (?, ?)",
                        (int(user["chat_id"]), json.dumps(user))
                    )
                except (ValueError, json.JSONDecodeError) as e:
                    logger.warning(f"Skipping user {user.get('chat_id', 'unknown')}: {e}")
                    continue

            await conn.commit()
            logger.info(f"Imported {len(users)} users to database")
    except aiosqlite.Error as e:
        logger.error(f"Error importing users to database: {e}")
        raise

def fetch_users_from_github(github_token, repo_name, file_path):
    url = f"https://raw.githubusercontent.com/{repo_name}/main/{file_path}"
    headers = {"Authorization": f"token {github_token}"}
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            raw_content = response.text
            if len(raw_content) > 1000:
                logger.debug(f"Raw response (truncated): {raw_content[:1000]}...")
            else:
                logger.debug(f"Raw response: {raw_content}")
            users = response.json()
            # Handle case where JSON is an object with a 'users' key
            if isinstance(users, dict) and "users" in users and isinstance(users["users"], list):
                users = users["users"]
                logger.info("Extracted users list from 'users' key in JSON")
            if not isinstance(users, list):
                logger.error(f"Fetched data is not a list: {type(users)}")
                raise ValueError("Invalid JSON format: Expected a list")
            logger.info(f"Fetched {len(users)} users from GitHub")
            return users
        except requests.RequestException as e:
            logger.error(f"Attempt {attempt + 1} failed fetching users.json: HTTP error - {e}")
            if attempt < 2:
                time.sleep(2)
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Attempt {attempt + 1} failed fetching users.json: JSON error - {e}")
            if attempt < 2:
                time.sleep(2)
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed fetching users.json: Unexpected error - {e}")
            if attempt < 2:
                time.sleep(2)
        if attempt == 2:
            logger.error("All attempts failed to fetch valid users.json")
            raise ValueError("Failed to fetch valid JSON after 3 attempts")

async def main():
    """Main function to migrate users.json from GitHub to users.db."""
    load_dotenv()
    db_file = os.getenv("DATABASE_FILE", "users.db")
    github_token = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPO")
    file_path = os.getenv("GITHUB_JSON_PATH", "users.json")

    if not all([db_file, github_token, repo_name, file_path]):
        logger.error("Missing environment variables: DATABASE_FILE, GITHUB_TOKEN, GITHUB_REPO, or GITHUB_JSON_PATH")
        return

    try:
        # Initialize database
        await init_db(db_file)

        # Fetch users from GitHub
        users = fetch_users_from_github(github_token, repo_name, file_path)

        # Import users to database
        await import_users_to_db(db_file, users)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise

if __name__ == "__main__":
    import time
    asyncio.run(main())