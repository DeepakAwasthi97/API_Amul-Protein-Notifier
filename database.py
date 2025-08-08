import aiosqlite
import json
import logging
import asyncio
from datetime import datetime, timedelta

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
        try:
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)

            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS state_product_status (
                    state_alias TEXT,
                    product_name TEXT,
                    status TEXT,
                    inventory_quantity INTEGER,
                    timestamp TEXT,
                    PRIMARY KEY (state_alias, product_name)
                )
            """)

            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS state_product_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    state_alias TEXT,
                    product_name TEXT,
                    status TEXT,
                    inventory_quantity INTEGER,
                    timestamp TEXT
                )
            """)

            await self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_product_history
                ON state_product_history (state_alias, product_name, timestamp)
            """)

            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS cleanup_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_cleanup_timestamp TEXT
                )
            """)

            await self.commit()
        except aiosqlite.Error as e:
            logging.error(f"Error creating tables: {e}")

    async def get_last_cleanup_time(self):
        """Retrieve the timestamp of the last cleanup."""
        try:
            async with self._connection.execute("""
                SELECT last_cleanup_timestamp FROM cleanup_history
                ORDER BY last_cleanup_timestamp DESC LIMIT 1
            """) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    return datetime.fromisoformat(row[0])
                return None
        except aiosqlite.Error as e:
            logging.error(f"Error getting last cleanup time: {e}")
            return None

    async def record_cleanup_time(self):
        """Record the current timestamp as the last cleanup time."""
        try:
            await self._connection.execute("""
                INSERT INTO cleanup_history (last_cleanup_timestamp)
                VALUES (?)
            """, (datetime.now().isoformat(),))
            await self.commit()
            logging.debug("Recorded cleanup timestamp")
        except aiosqlite.Error as e:
            logging.error(f"Error recording cleanup time: {e}")

    async def cleanup_state_history(self, days=2):
        """Clean up state_product_history older than specified days, if 2 days have passed since last cleanup."""
        try:
            last_cleanup = await self.get_last_cleanup_time()
            now = datetime.now()
            
            if last_cleanup and (now - last_cleanup) < timedelta(days=days):
                logging.debug("Skipping cleanup: less than 2 days since last cleanup")
                return False

            cutoff = (now - timedelta(days=days)).isoformat()
            await self._connection.execute(
                "DELETE FROM state_product_history WHERE timestamp < ?",
                (cutoff,)
            )
            
            await self.record_cleanup_time()
            await self.commit()
            logging.info(f"Cleaned up state_product_history older than {days} days")
            return True
        except aiosqlite.Error as e:
            logging.error(f"Error cleaning up state history: {e}")
            return False

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

    async def record_state_change(self, state_alias, product_name, status, inventory_quantity):
        """Record state change with proper transaction handling."""
        try:
            # Use a transaction to ensure consistency
            await self._connection.execute("BEGIN")
            
            # Get current state within transaction
            async with self._connection.execute("""
                SELECT status, inventory_quantity, timestamp
                FROM state_product_status
                WHERE state_alias = ? AND product_name = ?
            """, (state_alias, product_name)) as cursor:
                current_row = await cursor.fetchone()
                
            previous_state = None
            if current_row:
                previous_state = {
                    "status": current_row[0], 
                    "inventory_quantity": current_row[1], 
                    "timestamp": current_row[2]
                }

            # Always update current status
            await self._connection.execute("""
                INSERT OR REPLACE INTO state_product_status
                (state_alias, product_name, status, inventory_quantity, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (state_alias, product_name, status, inventory_quantity, datetime.now().isoformat()))

            # Log to history if status changed or no previous state
            if not previous_state or previous_state["status"] != status:
                await self._connection.execute("""
                    INSERT INTO state_product_history
                    (state_alias, product_name, status, inventory_quantity, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (state_alias, product_name, status, inventory_quantity, datetime.now().isoformat()))
                
                logging.info(f"State transition recorded: {state_alias} - {product_name} - {status} (previous: {previous_state['status'] if previous_state else 'None'})")
            else:
                logging.debug(f"No state transition for {state_alias} - {product_name}: status unchanged ({status})")

            await self._connection.commit()
            return previous_state  # Return previous state for restock detection
            
        except Exception as e:
            await self._connection.rollback()
            logging.error(f"Error recording state change for {state_alias} - {product_name}: {e}")
            raise

    async def is_restock_event(self, state_alias, product_name, current_status, previous_state):
        """Check if current state change represents a restock event."""
        if current_status != "In Stock":
            return False
            
        if not previous_state:
            # First time seeing this product - consider it a restock
            return True
            
        if previous_state["status"] == "Sold Out":
            # Direct transition from Sold Out to In Stock
            return True
            
        return False

    async def get_last_state_change(self, state_alias, product_name):
        try:
            async with self._connection.execute("""
                SELECT status, inventory_quantity, timestamp
                FROM state_product_status
                WHERE state_alias = ? AND product_name = ?
            """, (state_alias, product_name)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {"status": row[0], "inventory_quantity": row[1], "timestamp": row[2]}
                return None
        except aiosqlite.Error as e:
            logging.error(f"Error getting last state change for {state_alias} - {product_name}: {e}")
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
        try:
            async with self._connection.execute("""
                SELECT status, timestamp FROM state_product_history
                WHERE state_alias = ? AND product_name = ? AND timestamp > ?
                ORDER BY timestamp ASC
            """, (state_alias, product_name, since_time)) as cursor:
                rows = await cursor.fetchall()
                return [{"status": row[0], "timestamp": row[1]} for row in rows]
        except aiosqlite.Error as e:
            logging.error(f"Error getting state changes for {state_alias} - {product_name}: {e}")
            return []

    async def get_last_sold_out_before(self, state_alias, product_name, before_time):
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