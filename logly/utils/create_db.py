"""
Database initialization utility for Logly
Creates and initializes the SQLite database with schema if it doesn't exist
"""

import sqlite3
from pathlib import Path
from typing import Optional

from logly.utils.paths import get_db_path, get_db_dir
from logly.utils.logger import get_logger


logger = get_logger(__name__)


def db_exists() -> bool:
    """
    Check if the database file exists

    Returns:
        True if database file exists, False otherwise
    """
    db_path = get_db_path()
    return db_path.exists() and db_path.is_file()


def create_database(force: bool = False) -> Path:
    """
    Create and initialize the Logly database with schema

    This function will:
    1. Check if database already exists (unless force=True)
    2. Create the db directory if it doesn't exist
    3. Create the database file
    4. Execute the schema.sql to create all tables and indexes
    5. Insert initial metadata

    Args:
        force: If True, recreate database even if it exists (destructive!)

    Returns:
        Path to the created database file

    Raises:
        FileExistsError: If database exists and force=False
        FileNotFoundError: If schema.sql cannot be found
        sqlite3.Error: If database creation or schema execution fails
    """
    db_path = get_db_path()

    # Check if database already exists
    if db_exists() and not force:
        logger.info(f"Database already exists at {db_path}")
        return db_path

    # If force=True and db exists, warn about recreation
    if force and db_exists():
        logger.warning(f"Force flag set - recreating database at {db_path}")
        db_path.unlink()  # Delete existing database

    # Ensure db directory exists
    db_dir = get_db_dir()
    db_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Database directory ensured at {db_dir}")

    # Find schema.sql file
    # Schema is located at logly/storage/schema.sql
    schema_path = Path(__file__).parent.parent / "storage" / "schema.sql"

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found at {schema_path}")

    logger.info(f"Creating database at {db_path}")

    try:
        # Create database connection
        conn = sqlite3.connect(db_path)

        # Read and execute schema
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        logger.debug("Executing database schema")
        conn.executescript(schema_sql)
        conn.commit()

        # Verify tables were created
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]

        logger.info(f"Database created successfully with {len(tables)} tables")
        logger.debug(f"Tables created: {', '.join(tables)}")

        conn.close()

        return db_path

    except sqlite3.Error as e:
        logger.error(f"Failed to create database: {e}")
        # Clean up partial database file if it was created
        if db_path.exists():
            db_path.unlink()
        raise


def initialize_db_if_needed() -> Path:
    """
    Initialize database only if it doesn't already exist

    This is the main entry point for safe database initialization.
    It will create the database only if it doesn't exist yet.

    Returns:
        Path to the database file (existing or newly created)
    """
    if db_exists():
        db_path = get_db_path()
        logger.debug(f"Database already exists at {db_path}")
        return db_path

    logger.info("Database does not exist - initializing new database")
    return create_database(force=False)


def get_db_info() -> Optional[dict]:
    """
    Get information about the existing database

    Returns:
        Dictionary with database info or None if database doesn't exist
    """
    if not db_exists():
        return None

    db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)

        # Get table count
        cursor = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        )
        table_count = cursor.fetchone()[0]

        # Get schema version from metadata
        try:
            cursor = conn.execute(
                "SELECT value FROM metadata WHERE key='schema_version'"
            )
            schema_version = cursor.fetchone()
            schema_version = schema_version[0] if schema_version else "unknown"
        except sqlite3.Error:
            schema_version = "unknown"

        # Get database size
        size_bytes = db_path.stat().st_size
        size_mb = round(size_bytes / (1024 * 1024), 2)

        conn.close()

        return {
            "path": str(db_path),
            "exists": True,
            "size_bytes": size_bytes,
            "size_mb": size_mb,
            "table_count": table_count,
            "schema_version": schema_version
        }

    except sqlite3.Error as e:
        logger.error(f"Failed to get database info: {e}")
        return {
            "path": str(db_path),
            "exists": True,
            "error": str(e)
        }


if __name__ == "__main__":
    # Allow running this module directly to create database
    import sys

    force = "--force" in sys.argv

    if force:
        print("WARNING: --force flag detected - will recreate database")
        response = input("Are you sure? This will delete all existing data (y/N): ")
        if response.lower() != 'y':
            print("Aborted")
            sys.exit(1)

    try:
        db_path = create_database(force=force)
        print(f" Database created successfully at: {db_path}")

        # Show info
        info = get_db_info()
        if info:
            print("\nDatabase Info:")
            print(f"  Tables: {info.get('table_count')}")
            print(f"  Schema Version: {info.get('schema_version')}")
            print(f"  Size: {info.get('size_mb')} MB")
    except Exception as e:
        print(f" Failed to create database: {e}")
        sys.exit(1)
