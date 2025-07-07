
import sqlite3
import threading
import json
import logging

class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.create_table()

    def create_table(self):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        chat_id INTEGER PRIMARY KEY,
                        data TEXT NOT NULL
                    )
                """)
                self.conn.commit()
                logging.info("Database table 'users' created or already exists.")
            except sqlite3.Error as e:
                logging.error(f"Error creating table: {e}")

    def add_user(self, chat_id, user_data):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                # Use INSERT OR REPLACE to perform an "upsert"
                cursor.execute("INSERT OR REPLACE INTO users (chat_id, data) VALUES (?, ?)", (chat_id, json.dumps(user_data)))
                self.conn.commit()
                logging.info(f"User {chat_id} added or updated in the database.")
            except sqlite3.Error as e:
                logging.error(f"Error adding or updating user {chat_id}: {e}")

    def get_user(self, chat_id):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT data FROM users WHERE chat_id = ?", (chat_id,))
                row = cursor.fetchone()
                if row:
                    return json.loads(row[0])
                return None
            except sqlite3.Error as e:
                logging.error(f"Error getting user {chat_id}: {e}")
                return None

    def update_user(self, chat_id, user_data):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("UPDATE users SET data = ? WHERE chat_id = ?", (json.dumps(user_data), chat_id))
                self.conn.commit()
                logging.info(f"User {chat_id} updated in the database.")
            except sqlite3.Error as e:
                logging.error(f"Error updating user {chat_id}: {e}")

    def get_all_users(self):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT data FROM users")
                rows = cursor.fetchall()
                return [json.loads(row[0]) for row in rows]
            except sqlite3.Error as e:
                logging.error(f"Error getting all users: {e}")
                return []

    def close(self):
        self.conn.close()
        logging.info("Database connection closed.")

# Example usage (optional, for testing)
# if __name__ == '__main__':
#     db = Database('users.db')
#     # Example: Add a user
#     user_1_data = {"name": "John Doe", "preferences": {"notifications": "on"}}
#     db.add_user(12345, user_1_data)

#     # Example: Get a user
#     user = db.get_user(12345)
#     print("Retrieved user:", user)

#     # Example: Update a user
#     user_1_data["preferences"]["notifications"] = "off"
#     db.update_user(12345, user_1_data)
#     user = db.get_user(12345)
#     print("Updated user:", user)

#     # Example: Get all users
#     all_users = db.get_all_users()
#     print("All users:", all_users)

#     db.close()
