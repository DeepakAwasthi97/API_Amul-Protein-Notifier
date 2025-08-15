import asyncpg
import logging
import asyncio
from datetime import datetime, timedelta
from config import DATABASE_URL
import json  # Added for potential loads

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_url):
        self.db_url = db_url
        self._pool = None

    async def _init_db(self):
        """Initialize the PostgreSQL connection pool and create tables."""
        logging.info(f"Initializing PostgreSQL database with URL: {self.db_url}")
        try:
            self._pool = await asyncpg.create_pool(
                self.db_url,
                min_size=10,  # Scaled for 5k users
                max_size=50,  # High concurrency
                max_inactive_connection_lifetime=300,
                timeout=30
            )
            logging.info("Connection pool created successfully")
            async with self._pool.acquire() as conn:
                await self.create_tables(conn)
                logging.info("Database tables created successfully")
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"PostgreSQL error during initialization: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during initialization: {type(e).__name__}: {e}")
            raise

    async def create_tables(self, conn):
        """Create necessary tables with proper constraints."""
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id BIGINT PRIMARY KEY,
                    data JSONB NOT NULL,
                    CONSTRAINT valid_user_data CHECK (jsonb_typeof(data) = 'object' AND data ? 'chat_id')
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS state_product_status (
                    state_alias TEXT,
                    product_name TEXT,
                    status TEXT NOT NULL CHECK (status IN ('In Stock', 'Sold Out')),
                    inventory_quantity INTEGER NOT NULL CHECK (inventory_quantity >= 0),
                    timestamp TEXT NOT NULL,
                    PRIMARY KEY (state_alias, product_name)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS state_product_history (
                    id BIGSERIAL PRIMARY KEY,
                    state_alias TEXT NOT NULL,
                    product_name TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('In Stock', 'Sold Out')),
                    inventory_quantity INTEGER NOT NULL CHECK (inventory_quantity >= 0),
                    timestamp TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_product_history
                ON state_product_history (state_alias, product_name, timestamp DESC)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_product_history_status
                ON state_product_history (status)
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cleanup_history (
                    id BIGSERIAL PRIMARY KEY,
                    last_cleanup_timestamp TEXT NOT NULL
                )
            """)
            # Add GIN index for JSONB queries on users.data
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_data_gin
                ON users USING GIN (data)
            """)
            logging.info("Database tables and indexes created successfully")
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error creating tables: {e}")
            raise

    async def get_last_cleanup_time(self):
        """Retrieve the timestamp of the last cleanup."""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT last_cleanup_timestamp FROM cleanup_history
                    ORDER BY last_cleanup_timestamp DESC LIMIT 1
                """)
                if row:
                    return datetime.fromisoformat(row['last_cleanup_timestamp'])
                return None
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error getting last cleanup time: {e}")
            return None
        except ValueError as e:
            logging.error(f"Timestamp parse error in get_last_cleanup_time: {e}")
            return None

    async def record_cleanup_time(self):
        """Record the current timestamp as the last cleanup time."""
        try:
            now_iso = datetime.now().isoformat()
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO cleanup_history (last_cleanup_timestamp)
                        VALUES ($1)
                    """, now_iso)
                    logging.debug("Recorded cleanup timestamp")
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error recording cleanup time: {e}")

    async def cleanup_state_history(self, days=2):
        """Clean up state_product_history older than specified days."""
        try:
            last_cleanup = await self.get_last_cleanup_time()
            now = datetime.now()
            if last_cleanup and (now - last_cleanup) < timedelta(days=2):
                logging.debug("Skipping cleanup: less than 2 days since last cleanup")
                return False
            cutoff_iso = (now - timedelta(days=days)).isoformat()
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        DELETE FROM state_product_history
                        WHERE timestamp < $1
                    """, cutoff_iso)
                    logging.info(f"Cleaned up state history older than {cutoff_iso}")
                    await self.record_cleanup_time()
                    return True
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error during cleanup: {e}")
            return False

    def _decode_jsonb(self, data):
        """Helper function to decode JSONB data from PostgreSQL."""
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error: {e}")
                return None
        
        if isinstance(data, dict):
            # Parse known JSONB fields
            if 'products' in data and isinstance(data['products'], str):
                try:
                    data['products'] = json.loads(data['products'])
                except json.JSONDecodeError:
                    pass  # Keep as string if can't parse
            
            if 'notification_preference' in data and isinstance(data['notification_preference'], str):
                try:
                    data['notification_preference'] = json.loads(data['notification_preference'])
                except json.JSONDecodeError:
                    pass  # Keep as string if can't parse
            
            if 'last_notified' in data and isinstance(data['last_notified'], str):
                try:
                    data['last_notified'] = json.loads(data['last_notified'])
                except json.JSONDecodeError:
                    data['last_notified'] = {}  # Default to empty dict
        
        return data

    async def get_user(self, chat_id):
        """Retrieve user data by chat_id."""
        try:
            chat_id = int(chat_id)  # Ensure int for BIGINT
            logging.info(f"Fetching user for chat_id {chat_id} of type {type(chat_id)}")
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT data FROM users WHERE chat_id = $1
                """, chat_id)
                if not row:
                    logging.warning(f"No row found for chat_id {chat_id}")
                    return None
                
                data = self._decode_jsonb(row['data'])
                if not isinstance(data, dict):
                    logging.error(f"Invalid data type for chat_id {chat_id}: {type(data)}")
                    return None
                return data
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error getting user {chat_id}: {e}")
            return None

    # Updated database.py with ::jsonb casts
    async def update_user(self, chat_id, user_data):
        """Update user data with full JSONB overwrite."""
        try:
            chat_id = int(chat_id)  # Ensure int for BIGINT
            user_json = json.dumps(user_data)  # Serialize to str
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO users (chat_id, data)
                        VALUES ($1, $2::jsonb)
                        ON CONFLICT (chat_id)
                        DO UPDATE SET data = EXCLUDED.data
                    """, chat_id, user_json)
                    logging.debug(f"Updated user {chat_id}")
                    # Transaction is automatically committed here
            return True
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error updating user {chat_id}: {e}")
            return False

    async def update_user_partial(self, chat_id, path, value):
        """Perform partial JSONB update using jsonb_set."""
        try:
            chat_id = int(chat_id)  # Ensure int for BIGINT
            value_json = json.dumps(value)  # Serialize to JSON str
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        UPDATE users
                        SET data = jsonb_set(data, $2, $3::jsonb)
                        WHERE chat_id = $1
                    """, chat_id, path, value_json)
                    logging.debug(f"Partial update for user {chat_id} at path {path}")
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error partial updating user {chat_id}: {e}")
            raise

    async def delete_user(self, chat_id):
        """Delete user by chat_id."""
        try:
            chat_id = int(chat_id)  # Ensure int for BIGINT
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        DELETE FROM users WHERE chat_id = $1
                    """, chat_id)
                    logging.info(f"Deleted user {chat_id}")
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error deleting user {chat_id}: {e}")
            raise

    async def get_all_users(self):
        """Retrieve all users for broadcasts or stats."""
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("SELECT data FROM users")
                users = []
                for row in rows:
                    data = row['data']
                    if isinstance(data, str):
                        logging.warning("Data is str in get_all_users, attempting json.loads")
                        try:
                            data = json.loads(data)
                        except json.JSONDecodeError as e:
                            logging.error(f"JSON decode error in get_all_users: {e}")
                            continue
                    if isinstance(data, dict):
                        users.append(data)
                    else:
                        logging.error(f"Invalid data type in get_all_users: {type(data)}")
                if len(users) != len(rows):
                    logging.warning(f"Filtered {len(rows) - len(users)} invalid user records")
                return users
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error getting all users: {e}")
            return []

    async def record_state_change(self, state_alias, product_name, status, inventory_quantity):
        """Record state change and return previous state."""
        now_iso = datetime.now().isoformat()  # Str for TEXT
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow("""
                        SELECT status, inventory_quantity, timestamp
                        FROM state_product_status
                        WHERE state_alias = $1 AND product_name = $2
                    """, state_alias, product_name)
                    previous_state = dict(row) if row else None
                    await conn.execute("""
                        INSERT INTO state_product_status
                        (state_alias, product_name, status, inventory_quantity, timestamp)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (state_alias, product_name)
                        DO UPDATE SET
                            status = EXCLUDED.status,
                            inventory_quantity = EXCLUDED.inventory_quantity,
                            timestamp = EXCLUDED.timestamp
                    """, state_alias, product_name, status, inventory_quantity, now_iso)
                    state_changed = (
                        not previous_state or 
                        previous_state["status"] != status or 
                        (previous_state["status"] == "In Stock" and previous_state["inventory_quantity"] == 0 and inventory_quantity > 0)
                    )
                    
                    if state_changed:
                        await conn.execute("""
                            INSERT INTO state_product_history
                            (state_alias, product_name, status, inventory_quantity, timestamp)
                            VALUES ($1, $2, $3, $4, $5)
                        """, state_alias, product_name, status, inventory_quantity, now_iso)
                        logging.info(f"State transition: {state_alias} - {product_name} - {status} (quantity: {inventory_quantity}) [previous: {previous_state['status'] if previous_state else 'None'}]")
                    else:
                        logging.debug(f"No significant state change for {state_alias} - {product_name}: status unchanged")
                    return previous_state
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error recording state change for {state_alias} - {product_name}: {e}")
            raise

    async def is_restock_event(self, state_alias, product_name, current_status, previous_state):
        """Check if current state change is a restock."""
        try:
            # Only consider In Stock status for restock events
            if current_status != "In Stock":
                logger.debug(f"is_restock_event: current_status for {product_name} is not In Stock ({current_status})")
                return False

            # Use previous_state (from state_product_status before update) to decide
            # previous_state is the row before we updated the current status
            logger.debug(f"is_restock_event: previous_state for {product_name} in {state_alias}: {previous_state}")

            # If we never saw this product before, consider it a restock (new product)
            if not previous_state:
                logger.info(f"is_restock_event: no previous state for {product_name} in {state_alias} - treating as restock")
                return True

            prev_status = previous_state.get('status') if isinstance(previous_state, dict) else None
            prev_qty = previous_state.get('inventory_quantity') if isinstance(previous_state, dict) else None

            # If previously not in stock, and now in stock => restock
            if prev_status != 'In Stock':
                logger.info(f"is_restock_event: {product_name} in {state_alias} changed from '{prev_status}' to 'In Stock' - restock")
                return True

            # Edge case: previous status was In Stock but quantity was 0 and now >0
            try:
                if prev_status == 'In Stock' and isinstance(prev_qty, int) and prev_qty == 0:
                    logger.info(f"is_restock_event: {product_name} had In Stock with qty=0 previously, treating as restock when qty increases")
                    return True
            except Exception:
                pass

            # Otherwise not a restock
            logger.debug(f"is_restock_event: {product_name} in {state_alias} is In Stock and was already In Stock previously - not a restock")
            return False

        except Exception as e:
            logger.error(f"Error checking restock event for {product_name} in {state_alias}: {e}")
            return False

    async def get_last_state_change(self, state_alias, product_name):
        """Get the last recorded state for a product."""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT status, inventory_quantity, timestamp
                    FROM state_product_status
                    WHERE state_alias = $1 AND product_name = $2
                """, state_alias, product_name)
                if row:
                    row_dict = dict(row)
                    row_dict['timestamp'] = datetime.fromisoformat(row_dict['timestamp'])
                    return row_dict
                return None
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error getting last state change for {state_alias} - {product_name}: {e}")
            return None
        except ValueError as e:
            logging.error(f"Timestamp parse error in get_last_state_change: {e}")
            return None

    async def get_state_changes_since(self, state_alias, product_name, since_time):
        """Get state changes since a given time."""
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT status, timestamp FROM state_product_history
                    WHERE state_alias = $1 AND product_name = $2 AND timestamp > $3
                    ORDER BY timestamp ASC
                """, state_alias, product_name, since_time)
                changes = []
                for row in rows:
                    row_dict = dict(row)
                    row_dict['timestamp'] = datetime.fromisoformat(row_dict['timestamp'])
                    changes.append(row_dict)
                return changes
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error getting state changes for {state_alias} - {product_name}: {e}")
            return []
        except ValueError as e:
            logging.error(f"Timestamp parse error in get_state_changes_since: {e}")
            return []

    async def get_last_sold_out_before(self, state_alias, product_name, before_time):
        """Get the last 'Sold Out' state before a given time."""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT status, timestamp FROM state_product_history
                    WHERE state_alias = $1 AND product_name = $2 AND status = 'Sold Out' AND timestamp < $3
                    ORDER BY timestamp DESC LIMIT 1
                """, state_alias, product_name, before_time)
                if row:
                    row_dict = dict(row)
                    row_dict['timestamp'] = datetime.fromisoformat(row_dict['timestamp'])
                    return row_dict
                return None
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error getting last sold out state: {e}")
            return None
        except ValueError as e:
            logging.error(f"Timestamp parse error in get_last_sold_out_before: {e}")
            return None

    async def close(self):
        """Close the database connection pool."""
        try:
            if self._pool:
                await asyncio.wait_for(self._pool.close(), timeout=30)
                logging.info("Database connection pool closed")
                self._pool = None
        except asyncio.TimeoutError:
            logging.warning("Timeout closing pool; connections may linger")
        except asyncpg.exceptions.PostgresError as e:
            logging.error(f"Error closing database: {e}")
            raise