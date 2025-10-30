"""
Data aggregation engine for time-series rollups
"""

from datetime import datetime, timedelta

from logly.storage.sqlite_store import SQLiteStore
from logly.utils.logger import get_logger


logger = get_logger(__name__)


class Aggregator:
    """Handles data aggregation for hourly and daily rollups"""

    def __init__(self, store: SQLiteStore, config: dict):
        """
        Initialize aggregator

        Args:
            store: SQLiteStore instance
            config: Aggregation configuration
        """
        self.store = store
        self.config = config
        self.enabled = config.get("enabled", True)
        self.intervals = config.get("intervals", ["hourly", "daily"])
        self.keep_raw_data_days = config.get("keep_raw_data_days", 7)

    def run_hourly_aggregation(self):
        """Run hourly aggregation for the previous complete hour"""
        if not self.enabled or "hourly" not in self.intervals:
            return

        try:
            # Get the previous complete hour
            now = datetime.now()
            last_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(
                hours=1
            )
            hour_timestamp = int(last_hour.timestamp())

            logger.info(f"Running hourly aggregation for {last_hour}")
            self.store.compute_hourly_aggregates(hour_timestamp)

        except Exception as e:
            logger.error(f"Error running hourly aggregation: {e}")

    def run_daily_aggregation(self):
        """Run daily aggregation for the previous complete day"""
        if not self.enabled or "daily" not in self.intervals:
            return

        try:
            # Get yesterday's date
            yesterday = datetime.now().date() - timedelta(days=1)
            date_str = yesterday.strftime("%Y-%m-%d")

            logger.info(f"Running daily aggregation for {date_str}")
            self.store.compute_daily_aggregates(date_str)

        except Exception as e:
            logger.error(f"Error running daily aggregation: {e}")

    def cleanup_old_raw_data(self):
        """Remove raw data older than retention period, keeping only aggregates"""
        if not self.enabled:
            return

        try:
            # Keep raw data for configured days, then only keep aggregates
            logger.info(
                f"Cleaning up raw data older than {self.keep_raw_data_days} days"
            )
            # This would delete raw metrics but keep hourly/daily aggregates
            # For now, we rely on the main retention policy in the store

        except Exception as e:
            logger.error(f"Error cleaning up old raw data: {e}")
