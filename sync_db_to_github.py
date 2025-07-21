import asyncio
import aiosqlite
import json
import logging
import os
from github import Github
from github.GithubException import GithubException
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

async def fetch_users_from_db(db_file):
    """Fetch all users from the database."""
    try:
        async with aiosqlite.connect(db_file) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute("SELECT data FROM users") as cursor:
                rows = await cursor.fetchall()
                users = [json.loads(row["data"]) for row in rows]
                logger.info(f"Fetched {len(users)} users from database")
                return users
    except aiosqlite.Error as e:
        logger.error(f"Error fetching users from database: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding user data: {e}")
        raise

def push_to_github(users, github_token, repo_name, file_path):
    """Push users.json to GitHub repository."""
    try:
        g = Github(github_token)
        repo = g.get_repo(repo_name)
        commit_message = f"Update users.json {datetime.now().isoformat()}"
        json_content = json.dumps(users, indent=2)

        # Check if file exists in repo
        try:
            contents = repo.get_contents(file_path)
            repo.update_file(file_path, commit_message, json_content, contents.sha)
            logger.info(f"Updated {file_path} in GitHub repository {repo_name}")
        except GithubException as e:
            if e.status == 404:
                # File doesn't exist, create it
                repo.create_file(file_path, commit_message, json_content)
                logger.info(f"Created {file_path} in GitHub repository {repo_name}")
            else:
                raise
    except GithubException as e:
        logger.error(f"GitHub API error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error pushing to GitHub: {e}")
        raise

async def main():
    """Main function to sync users.db to GitHub."""
    load_dotenv()
    db_file = os.getenv("DATABASE_FILE", "users.db")
    github_token = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPO")
    file_path = os.getenv("GITHUB_JSON_PATH", "users.json")

    if not all([db_file, github_token, repo_name, file_path]):
        logger.error("Missing environment variables: DATABASE_FILE, GITHUB_TOKEN, GITHUB_REPO, or GITHUB_JSON_PATH")
        return

    for attempt in range(3):
        try:
            # Fetch users from database
            users = await fetch_users_from_db(db_file)

            # Write to temporary local file
            temp_file = "users_temp.json"
            with open(temp_file, "w") as f:
                json.dump(users, f, indent=2)
            logger.info(f"Wrote {len(users)} users to temporary file {temp_file}")

            # Push to GitHub
            push_to_github(users, github_token, repo_name, file_path)

            # Clean up temporary file
            os.remove(temp_file)
            logger.info("Temporary file removed")
            break
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                logger.error("Sync failed after 3 attempts")
                raise

if __name__ == "__main__":
    asyncio.run(main())