"""Utility modules for Logly"""

from logly.utils.logger import get_logger, initialize_logging, get_current_log_file
from logly.utils.db_size import get_db_size, format_size, get_db_info
from logly.utils.system_storage import (
    get_storage_info,
    get_storage_summary,
    format_bytes,
    check_storage_warning,
    check_storage_critical,
    get_free_space_mb,
    get_free_space_gb,
)
from logly.utils.paths import (
    get_project_root,
    get_logs_dir,
    get_db_dir,
    get_db_path,
    validate_db_path,
    validate_log_dir,
)

__all__ = [
    # Logger utilities
    "get_logger",
    "initialize_logging",
    "get_current_log_file",
    # Database size utilities
    "get_db_size",
    "format_size",
    "get_db_info",
    # System storage utilities
    "get_storage_info",
    "get_storage_summary",
    "format_bytes",
    "check_storage_warning",
    "check_storage_critical",
    "get_free_space_mb",
    "get_free_space_gb",
    # Path utilities (HARDCODED paths)
    "get_project_root",
    "get_logs_dir",
    "get_db_dir",
    "get_db_path",
    "validate_db_path",
    "validate_log_dir",
]
