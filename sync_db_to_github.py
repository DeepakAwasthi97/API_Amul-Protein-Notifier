
import logging
from database import Database
import common
import config

def sync_database_to_github():
    """
    Fetches all user data from the SQLite database and updates the users.json
    file in the GitHub repository.
    """
    logging.info("Starting database to GitHub sync...")

    if not config.USE_DATABASE:
        logging.warning("USE_DATABASE is set to False. Sync script will not run.")
        return

    # 1. Initialize the database and fetch all users
    db = Database(config.DATABASE_FILE)
    all_users = db.get_all_users()
    db.close()

    if not all_users:
        logging.info("No users found in the database. Nothing to sync.")
        return

    logging.info(f"Found {len(all_users)} users in the database to sync.")

    # 2. Format the data for the JSON file
    # The users.json file expects a dictionary with a "users" key
    users_data_for_json = {"users": all_users}

    # 3. Update the file in the GitHub repository
    success = common.update_users_file(users_data_for_json)

    if success:
        logging.info("Successfully synced database to users.json on GitHub.")
    else:
        logging.error("Failed to sync database to users.json on GitHub.")

if __name__ == '__main__':
    common.setup_logging()
    sync_database_to_github()
