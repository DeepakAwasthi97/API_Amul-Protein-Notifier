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
            # await self.migrate_tables()
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
            await self.commit()
        except aiosqlite.Error as e:
            logging.error(f"Error creating tables: {e}")

    # async def migrate_tables(self):
    #     try:
    #         async with self._connection.execute("PRAGMA table_info(state_product_history)") as cursor:
    #             columns = [row[1] for row in await cursor.fetchall()]
    #             if "inventory_quantity" in columns or "unique" in [row[5].lower() for row in await cursor.fetchall()]:
    #                 # Migrate old state_product_history to state_product_status
    #                 await self._connection.execute("""
    #                     INSERT OR REPLACE INTO state_product_status (state_alias, product_name, status, inventory_quantity, timestamp)
    #                     SELECT state_alias, product_name, status, inventory_quantity, timestamp
    #                     FROM state_product_history
    #                 """)
    #                 await self._connection.execute("DROP TABLE IF EXISTS state_product_history")
    #                 await self.create_tables()
    #                 logging.info("Migrated state_product_history to new schema")
    #     except aiosqlite.Error as e:
    #         logging.error(f"Error migrating tables: {e}")

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
        try:
            await self._connection.execute("""
                INSERT OR REPLACE INTO state_product_status (state_alias, product_name, status, inventory_quantity, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (state_alias, product_name, status, inventory_quantity, datetime.now().isoformat()))
            last_state = await self.get_last_state_change(state_alias, product_name)
            if not last_state or last_state["status"] != status:
                await self._connection.execute("""
                    INSERT INTO state_product_status (state_alias, product_name, status, timestamp)
                    VALUES (?, ?, ?, ?)
                """, (state_alias, product_name, status, datetime.now().isoformat()))
                logging.info(f"State transition recorded: {state_alias} - {product_name} - {status}")
            await self.commit()
        except aiosqlite.Error as e:
            logging.error(f"Error recording state change: {e}")

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
        try:
            async with self._connection.execute("""
                SELECT status, timestamp FROM state_product_status
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
                SELECT status, timestamp FROM state_product_status 
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
    
    async def cleanup_state_history(self, days=30):
        try:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            await self._connection.execute("DELETE FROM state_product_status WHERE timestamp < ?", (cutoff,))
            await self.commit()
            logging.info(f"Cleaned up state_product_status older than {days} days")
        except aiosqlite.Error as e:
            logging.error(f"Error cleaning up state history: {e}")