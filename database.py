import aiosqlite
import json
import logging
import asyncio

class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self._connection = None

    async def _init_db(self):
        """Initialize the database connection and create the users table."""
        try:
            self._connection = await aiosqlite.connect(self.db_file)
            await self._connection.execute("PRAGMA journal_mode=WAL")
            await self.create_table()
            logging.info("Database initialized with WAL mode.")
        except aiosqlite.Error as e:
            logging.error(f"Error initializing database: {e}")
            raise

    async def create_table(self):
        """Create the users table if it doesn't exist."""
        try:
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            await self._connection.commit()
            logging.info("Database table 'users' created or already exists.")
        except aiosqlite.Error as e:
            logging.error(f"Error creating table: {e}")
            raise

    async def add_user(self, chat_id, user_data):
        """Add or update a user in the database."""
        for attempt in range(3):
            try:
                await self._connection.execute(
                    "INSERT OR REPLACE INTO users (chat_id, data) VALUES (?, ?)",
                    (chat_id, json.dumps(user_data))
                )
                logging.debug(f"User {chat_id} queued for insertion/update.")
                return
            except aiosqlite.Error as e:
                logging.error(f"Error adding/updating user {chat_id} (attempt {attempt + 1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(1)
            logging.error(f"Failed to add/update user {chat_id} after 3 attempts.")

    async def commit(self):
        """Commit pending transactions."""
        try:
            await self._connection.commit()
            logging.debug("Database transaction committed.")
        except aiosqlite.Error as e:
            logging.error(f"Error committing transaction: {e}")
            raise

    async def get_user(self, chat_id):
        """Retrieve a user from the database."""
        try:
            async with self._connection.execute("SELECT data FROM users WHERE chat_id = ?", (chat_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return json.loads(row[0])
                return None
        except aiosqlite.Error as e:
            logging.error(f"Error getting user {chat_id}: {e}")
            return None

    async def update_user(self, chat_id, user_data):
        """Update a user in the database."""
        for attempt in range(3):
            try:
                await self._connection.execute(
                    "UPDATE users SET data = ? WHERE chat_id = ?",
                    (json.dumps(user_data), chat_id)
                )
                logging.debug(f"User {chat_id} queued for update.")
                return
            except aiosqlite.Error as e:
                logging.error(f"Error updating user {chat_id} (attempt {attempt + 1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(1)
            logging.error(f"Failed to update user {chat_id} after 3 attempts.")

    async def get_all_users(self):
        """Retrieve all users from the database."""
        try:
            async with self._connection.execute("SELECT data FROM users") as cursor:
                rows = await cursor.fetchall()
                return [json.loads(row[0]) for row in rows]
        except aiosqlite.Error as e:
            logging.error(f"Error getting all users: {e}")
            return []

    async def close(self):
        """Close the database connection."""
        try:
            if self._connection:
                await self._connection.commit()
                await self._connection.close()
                logging.info("Database connection closed.")
                self._connection = None
        except aiosqlite.Error as e:
            logging.error(f"Error closing database: {e}")
            raise