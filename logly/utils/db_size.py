"""
Database size utility
Calculate and report SQLite database file sizes
"""

from pathlib import Path
from typing import Dict, Union


def get_db_size(db_path: Union[str, Path]) -> Dict[str, float]:
    """
    Get database size in various units

    Args:
        db_path: Path to SQLite database file

    Returns:
        Dictionary containing:
        - size_bytes: Size in bytes (int)
        - size_kb: Size in kilobytes (float)
        - size_mb: Size in megabytes (float)
        - size_gb: Size in gigabytes (float)
        - exists: Whether the file exists (bool)
    """
    path = Path(db_path)

    if not path.exists():
        return {
            "size_bytes": 0,
            "size_kb": 0.0,
            "size_mb": 0.0,
            "size_gb": 0.0,
            "exists": False,
        }

    size_bytes = path.stat().st_size

    return {
        "size_bytes": size_bytes,
        "size_kb": round(size_bytes / 1024, 2),
        "size_mb": round(size_bytes / (1024 * 1024), 2),
        "size_gb": round(size_bytes / (1024 * 1024 * 1024), 3),
        "exists": True,
    }


def format_size(size_bytes: Union[int, float]) -> str:
    """
    Format bytes to human-readable string with appropriate unit

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string like "1.23 MB" or "456.78 KB"
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def get_db_info(db_path: Union[str, Path]) -> Dict[str, Union[str, int, float, bool]]:
    """
    Get comprehensive database information

    Args:
        db_path: Path to SQLite database file

    Returns:
        Dictionary with path, size info, and formatted size string
    """
    path = Path(db_path)
    size_info = get_db_size(path)

    return {
        "path": str(path.absolute()),
        "exists": size_info["exists"],
        "size_bytes": size_info["size_bytes"],
        "size_mb": size_info["size_mb"],
        "size_gb": size_info["size_gb"],
        "formatted_size": format_size(size_info["size_bytes"]),
    }
