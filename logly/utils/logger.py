"""
Centralized logging utility with daily file rotation
Creates new log files at midnight with date-based naming
Always logs verbosely (DEBUG level) without filtering
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from logly.utils.paths import get_logs_dir


class DailyRotatingLogger:
    """Logger that creates new files daily based on date"""

    def __init__(self, name: str = "logly"):
        """
        Initialize daily rotating logger

        Args:
            name: Base name for log files
        """
        # HARDCODED: Always use the project's logs directory
        self.log_dir = get_logs_dir()
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.name = name
        self.current_date = None
        self.file_handler = None
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # Always verbose
        self.logger.propagate = True  # Propagate to root logger for testing
        self._setup_handler()

    def _get_log_filename(self) -> Path:
        """Generate log filename based on current date"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        return self.log_dir / f"{self.name}-{date_str}.log"

    def _setup_handler(self):
        """Setup or rotate file handler based on date"""
        current_date = datetime.now().date()

        # Check if we need to rotate (new day)
        if self.current_date != current_date:
            # Remove old handler if exists
            if self.file_handler:
                self.logger.removeHandler(self.file_handler)
                self.file_handler.close()

            # Create new handler for today
            log_file = self._get_log_filename()
            self.file_handler = logging.FileHandler(log_file, mode='a')

            # Verbose format with all details
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            self.file_handler.setFormatter(formatter)

            self.logger.addHandler(self.file_handler)
            self.current_date = current_date

            # Log rotation event
            self.logger.info(f"Log rotation: New log file created at {log_file}")

    def get_logger(self, module_name: str) -> logging.Logger:
        """
        Get logger for a module, ensuring daily rotation

        Args:
            module_name: Name of the module requesting logger

        Returns:
            Logger instance that will write to daily rotated files
        """
        # Check if rotation needed (midnight passed)
        self._setup_handler()

        # Return child logger for the module
        return logging.getLogger(f"{self.name}.{module_name}")


# Singleton instance
_daily_logger: Optional[DailyRotatingLogger] = None


def get_logger(module_name: str) -> logging.Logger:
    """
    Get a logger instance with daily rotation

    This is the main function that all Logly modules should use for logging.
    All logs are written verbosely (DEBUG level) to daily files in the format:
    logly-YYYY-MM-DD.log

    Logs are ALWAYS written to the hardcoded path: logly/logs/

    Args:
        module_name: Name of the module requesting logger (typically __name__)

    Returns:
        Logger instance configured for daily rotation

    Example:
        from logly.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.debug("Detailed debug information")
        logger.info("Important event occurred")
    """
    global _daily_logger
    if _daily_logger is None:
        _daily_logger = DailyRotatingLogger(name="logly")
    return _daily_logger.get_logger(module_name)


def initialize_logging():
    """
    Initialize the logging system

    This should be called once at application startup.
    Logs are ALWAYS written to the hardcoded path: logly/logs/
    """
    global _daily_logger
    _daily_logger = DailyRotatingLogger(name="logly")


def get_current_log_file() -> Optional[Path]:
    """
    Get the path to the current day's log file

    Returns:
        Path to current log file, or None if logging not initialized
    """
    global _daily_logger
    if _daily_logger is None:
        return None
    return _daily_logger._get_log_filename()
