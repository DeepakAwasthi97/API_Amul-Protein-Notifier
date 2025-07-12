import asyncio
import json
import logging
from database import Database
import common
import config

async def migrate_users_from_github():
    """Fetches users.json from GitHub and migrates it to the SQLite database."""
    logging.info("Starting user migration from GitHub...")

    # Fetch user data from GitHub
    users_data = common.read_users_file()
    if not users_data or "users" not in users_data:
        logging.error("Could not fetch or parse user data from GitHub. Aborting migration.")
        return

    users_list = users_data["users"]
    logging.info(f"Successfully fetched {len(users_list)} user records from GitHub.")

    # Initialize the database
    db = Database(config.DATABASE_FILE)
    try:
        await db._init_db()
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        return

    # Migrate each user
    for user_record in users_list:
        try:
            chat_id = int(user_record.get("chat_id"))
            if not chat_id:
                logging.warning(f"Skipping record with missing chat_id: {user_record}")
                continue
            await db.add_user(chat_id, user_record)
        except (ValueError, TypeError) as e:
            logging.error(f"Invalid chat_id in record: {user_record}. Error: {e}")
        except Exception as e:
            logging.error(f"Error migrating user {user_record.get('chat_id', 'unknown')}: {e}")

    # Ensure all changes are committed
    try:
        await db.commit()
    except Exception as e:
        logging.error(f"Failed to commit changes: {e}")

    logging.info("User migration from GitHub to SQLite completed.")
    await db.close()

if __name__ == '__main__':
    common.setup_logging()
    asyncio.run(migrate_users_from_github())