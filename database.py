import aiosqlite
import json
import logging
import asyncio
from datetime import datetime

class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self._connection = None

    async def _init_db(self):
        """Initialize the database connection and create tables."""
        try:
            self._connection = await aiosqlite.connect(self.db_file)
            await self._connection.execute("PRAGMA journal_mode=WAL")
            await self.create_tables()
            logging.info("Database initialized with WAL mode.")
        except aiosqlite.Error as e:
            logging.error(f"Error initializing database: {e}")
            raise

    async def create_tables(self):
        """Create both users and state_product_history tables."""
        try:
            # Create users table
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            
            # Create state_product_history table
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS state_product_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    state_alias TEXT NOT NULL,
                    product_name TEXT NOT NULL,
                    status TEXT NOT NULL,  -- 'In Stock' or 'Sold Out'
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(state_alias, product_name)
                )
            """)
            
            await self._connection.commit()
            logging.info("Database tables created or already exist.")
        except aiosqlite.Error as e:
            logging.error(f"Error creating tables: {e}")
            raise

    async def add_user(self, chat_id, user_data):
        """Add or update a user in the database."""
        # Ensure default notification preferences are set
        if "notification_preference" not in user_data:
            user_data["notification_preference"] = "until_stop"  # Default
        if "last_notified" not in user_data:
            user_data["last_notified"] = {}
        
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

    async def get_user(self, chat_id):
        """Retrieve a user from the database."""
        try:
            async with self._connection.execute("SELECT data FROM users WHERE chat_id = ?", (chat_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    user_data = json.loads(row[0])
                    # Ensure notification preferences exist
                    if "notification_preference" not in user_data:
                        user_data["notification_preference"] = "until_stop"
                    if "last_notified" not in user_data:
                        user_data["last_notified"] = {}
                    return user_data
                return None
        except aiosqlite.Error as e:
            logging.error(f"Error getting user {chat_id}: {e}")
            return None

    async def get_all_users(self):
        """Retrieve all users from the database."""
        try:
            async with self._connection.execute("SELECT data FROM users") as cursor:
                rows = await cursor.fetchall()
                users = []
                for row in rows:
                    user_data = json.loads(row[0])
                    # Ensure notification preferences exist
                    if "notification_preference" not in user_data:
                        user_data["notification_preference"] = "until_stop"
                    if "last_notified" not in user_data:
                        user_data["last_notified"] = {}
                    users.append(user_data)
                return users
        except aiosqlite.Error as e:
            logging.error(f"Error getting all users: {e}")
            return []

    async def record_state_change(self, state_alias, product_name, status):
        """Record a state change in the state_product_history table."""
        try:
            await self._connection.execute("""
                INSERT OR REPLACE INTO state_product_history 
                (state_alias, product_name, status, timestamp) 
                VALUES (?, ?, ?, ?)
            """, (state_alias, product_name, status, datetime.now()))
            await self._connection.commit()
            logging.debug(f"State change recorded: {state_alias} - {product_name} - {status}")
        except aiosqlite.Error as e:
            logging.error(f"Error recording state change: {e}")

    async def get_last_state_change(self, state_alias, product_name):
        """Get the last recorded state for a product in a state."""
        try:
            async with self._connection.execute("""
                SELECT status, timestamp FROM state_product_history 
                WHERE state_alias = ? AND product_name = ? 
                ORDER BY timestamp DESC LIMIT 1
            """, (state_alias, product_name)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {"status": row[0], "timestamp": row[1]}
                return None
        except aiosqlite.Error as e:
            logging.error(f"Error getting last state change: {e}")
            return None

    async def commit(self):
        """Commit pending transactions."""
        try:
            await self._connection.commit()
            logging.debug("Database transaction committed.")
        except aiosqlite.Error as e:
            logging.error(f"Error committing transaction: {e}")
            raise

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

async def get_state_changes_since(self, state_alias, product_name, since_time):
    """Get all state changes for a product since a specific time."""
    try:
        async with self._connection.execute("""
            SELECT status, timestamp FROM state_product_history 
            WHERE state_alias = ? AND product_name = ? AND timestamp > ?
            ORDER BY timestamp ASC
        """, (state_alias, product_name, since_time)) as cursor:
            rows = await cursor.fetchall()
            return [{"status": row[0], "timestamp": row[1]} for row in rows]
    except aiosqlite.Error as e:
        logging.error(f"Error getting state changes: {e}")
        return []

async def get_last_sold_out_before(self, state_alias, product_name, before_time):
    """Get the last 'Sold Out' state before a specific time."""
    try:
        async with self._connection.execute("""
            SELECT status, timestamp FROM state_product_history 
            WHERE state_alias = ? AND product_name = ? AND status = 'Sold Out' AND timestamp < ?
            ORDER BY timestamp DESC LIMIT 1
        """, (state_alias, product_name, before_time)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {"status": row[0], "timestamp": row[1]}
            return None
    except aiosqlite.Error as e:
        logging.error(f"Error getting last sold out state: {e}")
        return None