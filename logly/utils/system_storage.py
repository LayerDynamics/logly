"""
System storage utility
Check filesystem storage space and usage
"""

import os
from typing import Dict, Union


def get_storage_info(path: str = "/") -> Dict[str, Union[str, int, float]]:
    """
    Get filesystem storage information

    Args:
        path: Mount point or directory to check (default: root filesystem)

    Returns:
        Dictionary containing:
        - total_bytes: Total storage capacity in bytes
        - free_bytes: Available storage in bytes
        - used_bytes: Used storage in bytes
        - percent_used: Percentage of storage used (0-100)
        - path: Path that was checked

    Raises:
        OSError: If the path doesn't exist or can't be accessed
    """
    try:
        stat = os.statvfs(path)
        total = stat.f_blocks * stat.f_frsize
        free = stat.f_bavail * stat.f_frsize
        used = total - free

        percent_used = 0.0
        if total > 0:
            percent_used = round(100.0 * used / total, 2)

        return {
            "path": path,
            "total_bytes": total,
            "free_bytes": free,
            "used_bytes": used,
            "percent_used": percent_used,
        }
    except Exception as e:
        raise OSError(f"Failed to get storage info for {path}: {e}")


def format_bytes(size_bytes: Union[int, float]) -> str:
    """
    Format bytes to human-readable string

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string like "1.23 GB" or "456.78 MB"
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    elif size_bytes < 1024 * 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024 * 1024):.2f} TB"


def get_storage_summary(path: str = "/") -> Dict[str, str]:
    """
    Get formatted storage summary

    Args:
        path: Mount point or directory to check

    Returns:
        Dictionary with human-readable formatted values
    """
    info = get_storage_info(path)

    total_bytes = int(info["total_bytes"])
    used_bytes = int(info["used_bytes"])
    free_bytes = int(info["free_bytes"])
    percent_used = float(info["percent_used"])

    return {
        "path": path,
        "total": format_bytes(total_bytes),
        "used": format_bytes(used_bytes),
        "free": format_bytes(free_bytes),
        "percent_used": f"{percent_used:.1f}%",
    }


def check_storage_warning(path: str = "/", threshold_percent: float = 90.0) -> bool:
    """
    Check if storage usage exceeds a warning threshold

    Args:
        path: Mount point or directory to check
        threshold_percent: Warning threshold percentage (default: 90.0)

    Returns:
        True if storage usage is above threshold, False otherwise
    """
    try:
        info = get_storage_info(path)
        return float(info["percent_used"]) >= threshold_percent
    except OSError:
        return False


def check_storage_critical(path: str = "/", threshold_percent: float = 95.0) -> bool:
    """
    Check if storage usage exceeds a critical threshold

    Args:
        path: Mount point or directory to check
        threshold_percent: Critical threshold percentage (default: 95.0)

    Returns:
        True if storage usage is above critical threshold, False otherwise
    """
    try:
        info = get_storage_info(path)
        return float(info["percent_used"]) >= threshold_percent
    except OSError:
        return False


def get_free_space_mb(path: str = "/") -> float:
    """
    Get available free space in megabytes

    Args:
        path: Mount point or directory to check

    Returns:
        Free space in MB
    """
    info = get_storage_info(path)
    free_bytes = int(info["free_bytes"])
    return round(free_bytes / (1024 * 1024), 2)


def get_free_space_gb(path: str = "/") -> float:
    """
    Get available free space in gigabytes

    Args:
        path: Mount point or directory to check

    Returns:
        Free space in GB
    """
    info = get_storage_info(path)
    free_bytes = int(info["free_bytes"])
    return round(free_bytes / (1024 * 1024 * 1024), 2)
