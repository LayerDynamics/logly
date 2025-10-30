"""
Path utilities for Logly
Provides hardcoded paths for logs and database that cannot be changed
"""

import os
from pathlib import Path


def get_project_root() -> Path:
    """
    Get absolute path to project root (auto-detected using __file__)

    This function uses Python's __file__ to locate the logly package
    and navigates up to find the project root directory.

    File structure:
        /Users/ryanoboyle/logly/           <- project root (returned)
        └── logly/                          <- package directory
            └── utils/                      <- utils directory
                └── paths.py                <- this file (__file__)

    Returns:
        Absolute Path to project root
    """
    # This file is at: logly/utils/paths.py
    # Navigate up: paths.py -> utils -> logly -> project_root
    return Path(__file__).parent.parent.parent.resolve()


def get_logs_dir() -> Path:
    """
    Get logs directory path (HARDCODED to logly/logs)

    This path CANNOT be changed via configuration.
    The directory will be created automatically if it doesn't exist.

    Returns:
        Absolute Path to logs directory: {project_root}/logs/
    """
    logs_dir = get_project_root() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


def get_db_dir() -> Path:
    """
    Get database directory path (HARDCODED to logly/db)

    This path CANNOT be changed via configuration.
    The directory will be created automatically if it doesn't exist.

    Returns:
        Absolute Path to database directory: {project_root}/db/
    """
    db_dir = get_project_root() / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir


def get_db_path() -> Path:
    """
    Get database file path (HARDCODED to logly/db/logly.db)

    This path CANNOT be changed via configuration.
    The parent directory will be created automatically if it doesn't exist.

    Returns:
        Absolute Path to database file: {project_root}/db/logly.db
    """
    return get_db_dir() / "logly.db"


def validate_db_path(db_path: str) -> bool:
    """
    Validate that a database path matches the hardcoded expected path

    In test mode (LOGLY_TEST_MODE environment variable set), allows any path.
    Otherwise, enforces hardcoded path.

    Args:
        db_path: Path to validate

    Returns:
        True if path matches expected path or in test mode, False otherwise
    """
    # Allow any path in test mode
    if os.environ.get("LOGLY_TEST_MODE") == "1":
        return True

    expected = get_db_path()
    provided = Path(db_path).resolve()
    return provided == expected


def validate_log_dir(log_dir: str) -> bool:
    """
    Validate that a log directory matches the hardcoded expected path

    In test mode (LOGLY_TEST_MODE environment variable set), allows any path.
    Otherwise, enforces hardcoded path.

    Args:
        log_dir: Directory path to validate

    Returns:
        True if path matches expected path or in test mode, False otherwise
    """
    # Allow any path in test mode
    if os.environ.get("LOGLY_TEST_MODE") == "1":
        return True

    expected = get_logs_dir()
    provided = Path(log_dir).resolve()
    return provided == expected
