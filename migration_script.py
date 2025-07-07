
import json
import logging
from database import Database
import common
import config

def migrate_users_from_github():
    """Fetches users.json from GitHub and migrates it to the SQLite database."""
    logging.info("Starting user migration from GitHub...")

    # 1. Fetch user data from GitHub
    users_data = common.read_users_file()
    if not users_data or "users" not in users_data:
        logging.error("Could not fetch or parse user data from GitHub. Aborting migration.")
        return

    # The data is nested under a 'users' key
    users_list = users_data["users"]
    logging.info(f"Successfully fetched {len(users_list)} user records from GitHub.")

    # 2. Initialize the database
    db = Database(config.DATABASE_FILE)

    # 3. Migrate each user
    for user_record in users_list:
        try:
            # The chat_id is a string in the JSON, so it needs to be cast to int
            chat_id = int(user_record.get("chat_id"))
            if not chat_id:
                logging.warning("Skipping record with missing chat_id: %s", user_record)
                continue
            
            # The entire record is the user_data
            db.add_user(chat_id, user_record)

        except (ValueError, TypeError) as e:
            logging.error("Could not process record, invalid chat_id: %s. Error: %s", user_record, e)
        except Exception as e:
            logging.error("An unexpected error occurred while migrating user %s: %s", user_record.get("chat_id"), e)

    logging.info("User migration from GitHub to SQLite has been completed.")
    db.close()

if __name__ == '__main__':
    # Setup logging to see the output of the migration
    common.setup_logging()
    migrate_users_from_github()
