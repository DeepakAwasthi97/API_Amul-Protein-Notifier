import aiosqlite
import asyncpg
import json
import asyncio
import os
from config import DATABASE_FILE, DATABASE_URL
import logging
from datetime import datetime

async def migrate_data():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Validate SQLite database file
        if not os.path.isfile(DATABASE_FILE):
            logger.error(f"SQLite database file does not exist: {DATABASE_FILE}")
            raise FileNotFoundError(f"SQLite database file not found: {DATABASE_FILE}")
        if not os.access(DATABASE_FILE, os.R_OK | os.W_OK):
            logger.error(f"SQLite database file is not accessible (check permissions): {DATABASE_FILE}")
            raise PermissionError(f"Insufficient permissions for SQLite database file: {DATABASE_FILE}")

        # Connect to SQLite
        logger.info(f"Connecting to SQLite database: {DATABASE_FILE}")
        sqlite_conn = await aiosqlite.connect(DATABASE_FILE)
        await sqlite_conn.execute("PRAGMA journal_mode=WAL")

        # Connect to PostgreSQL
        logger.info(f"Connecting to PostgreSQL: {DATABASE_URL}")
        pg_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=5,
            max_size=20,
            server_settings={"search_path":"public"}
        )

        async with pg_pool.acquire() as pg_conn:
            async with pg_conn.transaction():  # Use transaction for atomicity
                # Verify users table schema
                schema_check = await pg_conn.fetchrow("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = 'data'
                """)
                if schema_check and schema_check['data_type'] != 'jsonb':
                    logger.warning(f"Users.data column type is {schema_check['data_type']}, expected jsonb. Dropping and recreating table...")
                    await pg_conn.execute("DROP TABLE IF EXISTS users")
                elif not schema_check:
                    logger.info("Users table does not exist. Creating...")

                # Create users table
                role_info = await pg_conn.fetchrow("SELECT current_user, session_user;")
                logger.info(f"PostgreSQL current_user: {role_info['current_user']}, session_user: {role_info['session_user']}")
                await pg_conn.execute("""
                    CREATE TABLE IF NOT EXISTS public.users (
                        chat_id BIGINT PRIMARY KEY,
                        data JSONB NOT NULL
                    )
                """)
                logger.info("Created PostgreSQL users table")

                # Get existing chat_ids from PostgreSQL
                logger.info("Fetching existing users from PostgreSQL...")
                existing_chat_ids = set()
                pg_users = await pg_conn.fetch("SELECT chat_id FROM users")
                for user in pg_users:
                    existing_chat_ids.add(str(user['chat_id']))
                logger.info(f"Found {len(existing_chat_ids)} existing users in PostgreSQL")

                # Migrate users table
                user_batch = []
                valid_users = 0
                invalid_users = 0
                skipped_users = 0
                async with sqlite_conn.execute("SELECT chat_id, data FROM users") as cursor:
                    async for row in cursor:
                        chat_id = row[0]
                        
                        # Skip if user already exists in PostgreSQL
                        if str(chat_id) in existing_chat_ids:
                            skipped_users += 1
                            continue
                        try:
                            # Handle SQLite data: string (JSON) or dict
                            if isinstance(row[1], str):
                                user_data = json.loads(row[1])
                            else:
                                user_data = row[1]  # Already a dict (rare in SQLite)
                            # Validate user_data
                            if not isinstance(user_data, dict):
                                logger.error(f"Invalid user data type for chat_id {chat_id}: {type(row[1])} - {row[1]}")
                                invalid_users += 1
                                continue
                            # Ensure required fields
                            if 'chat_id' not in user_data or 'pincode' not in user_data:
                                logger.error(f"Missing required fields in user data for chat_id {chat_id}: {user_data}")
                                invalid_users += 1
                                continue
                            # Ensure chat_id consistency
                            if str(user_data['chat_id']) != str(chat_id):
                                logger.warning(f"Chat_id mismatch for {chat_id}: data.chat_id={user_data['chat_id']}. Fixing...")
                                user_data['chat_id'] = str(chat_id)
                            # Serialize user_data to JSON string for JSONB
                            user_batch.append((chat_id, json.dumps(user_data)))
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse user data for chat_id {chat_id}: {e} - {row[1]}")
                            invalid_users += 1
                            continue
                    # Batch insert
                    if user_batch:
                        try:
                            # Insert users that don't exist yet
                            await pg_conn.executemany(
                                """
                                INSERT INTO users (chat_id, data) 
                                VALUES ($1, $2::jsonb)
                                ON CONFLICT (chat_id) DO NOTHING
                                """,
                                user_batch
                            )
                            valid_users = len(user_batch)
                            logger.info(f"Inserted {valid_users} users in batch")
                        except asyncpg.exceptions.DataError as e:
                            logger.error(f"Failed to insert batch: {e}")
                            invalid_users += len(user_batch)
                            valid_users = 0
                            raise  # Fail the migration to alert user
                    else:
                        logger.warning("No valid users found to migrate")

                # Validate migration success
                if valid_users == 0 and invalid_users > 0:
                    raise RuntimeError(f"Migration failed: No valid users migrated, {invalid_users} invalid records")

        await sqlite_conn.close()
        await pg_pool.close()
        logger.info(f"Migration complete:")
        logger.info(f"- {valid_users} valid users migrated")
        logger.info(f"- {skipped_users} existing users skipped")
        logger.info(f"- {invalid_users} invalid users skipped")
        logger.info(f"Migration ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Close connections

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if 'sqlite_conn' in locals():
            await sqlite_conn.close()
        if 'pg_pool' in locals():
            await pg_pool.close()
        raise

if __name__ == "__main__":
    asyncio.run(migrate_data())
