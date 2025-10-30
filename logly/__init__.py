"""
Logly - Lightweight log aggregation and system monitoring for AWS EC2

A minimal-dependency Python application for collecting, storing, and exporting
system metrics, network activity, and log data from AWS EC2 instances.
"""

from logly.utils.create_db import initialize_db_if_needed
from logly.core.config import Config
from logly.storage.sqlite_store import SQLiteStore

__version__ = "0.1.0"
__author__ = "Ryan O'Boyle"

# Initialize database if it doesn't exist
initialize_db_if_needed()

__all__ = ["Config", "SQLiteStore"]
