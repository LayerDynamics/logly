"""
Scheduler for periodic data collection and aggregation
Uses stdlib sched module for minimal dependencies
"""

import sched
import time
import threading
from typing import Callable, Optional

from logly.core.config import Config
from logly.storage.sqlite_store import SQLiteStore
from logly.collectors.system_metrics import SystemMetricsCollector
from logly.collectors.network_monitor import NetworkMonitor
from logly.collectors.log_parser import LogParser
from logly.core.aggregator import Aggregator
from logly.utils.logger import get_logger


logger = get_logger(__name__)


class Scheduler:
    """Manages periodic collection and aggregation tasks"""

    def __init__(self, config: Config, store: SQLiteStore):
        """
        Initialize scheduler

        Args:
            config: Config instance
            store: SQLiteStore instance
        """
        self.config = config
        self.store = store
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._db_lock = threading.Lock()  # Serialize database access

        # Initialize collectors
        self.system_collector = None
        self.network_collector = None
        self.log_parser = None

        if config.get_system_config().get("enabled"):
            self.system_collector = SystemMetricsCollector(config.get_system_config())

        if config.get_network_config().get("enabled"):
            self.network_collector = NetworkMonitor(config.get_network_config())

        if config.get_logs_config().get("enabled"):
            self.log_parser = LogParser(config.get_logs_config())

        # Initialize aggregator
        self.aggregator = Aggregator(store, config.get_aggregation_config())

        # Get collection intervals
        collection_config = config.get_collection_config()
        self.system_interval = collection_config.get("system_metrics", 60)
        self.network_interval = collection_config.get("network_metrics", 60)
        self.log_interval = collection_config.get("log_parsing", 300)

    def _schedule_repeating(self, interval: int, func: Callable, name: str):
        """
        Schedule a repeating task

        Args:
            interval: Interval in seconds
            func: Function to call
            name: Task name for logging
        """

        def wrapper():
            if not self.running:
                return

            try:
                func()
            except Exception as e:
                logger.error(f"Error in {name}: {e}")

            # Reschedule
            if self.running:
                self.scheduler.enter(interval, 1, wrapper)

        # Run immediately (at time 0), then schedule repeating
        self.scheduler.enter(0, 1, wrapper)

    def _collect_system_metrics(self):
        """Collect and store system metrics"""
        if not self.system_collector:
            return

        try:
            metric = self.system_collector.collect()
            with self._db_lock:
                self.store.insert_system_metric(metric)
            logger.debug("Collected system metrics")
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")

    def _collect_network_metrics(self):
        """Collect and store network metrics"""
        if not self.network_collector:
            return

        try:
            metric = self.network_collector.collect()
            with self._db_lock:
                self.store.insert_network_metric(metric)
            logger.debug("Collected network metrics")
        except Exception as e:
            logger.error(f"Error collecting network metrics: {e}")

    def _parse_logs(self):
        """Parse and store log events"""
        if not self.log_parser:
            return

        try:
            events = self.log_parser.collect()
            with self._db_lock:
                for event in events:
                    self.store.insert_log_event(event)
            if events:
                logger.debug(f"Parsed {len(events)} log events")
        except Exception as e:
            logger.error(f"Error parsing logs: {e}")

    def _run_aggregations(self):
        """Run hourly and daily aggregations"""
        try:
            # Run hourly aggregation at the top of each hour
            current_minute = time.localtime().tm_min
            if current_minute == 0:
                with self._db_lock:
                    self.aggregator.run_hourly_aggregation()

            # Run daily aggregation at midnight
            current_hour = time.localtime().tm_hour
            if current_hour == 0 and current_minute == 0:
                with self._db_lock:
                    self.aggregator.run_daily_aggregation()

        except Exception as e:
            logger.error(f"Error running aggregations: {e}")

    def _cleanup_old_data(self):
        """Periodic cleanup of old data"""
        try:
            retention_days = self.config.get("database.retention_days", 90)
            with self._db_lock:
                self.store.cleanup_old_data(retention_days)
            logger.info("Cleaned up old data")
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")

    def start(self):
        """Start the scheduler in a background thread"""
        if self.running:
            logger.warning("Scheduler is already running")
            return

        self.running = True
        logger.info("Starting Logly scheduler")

        # Schedule collection tasks
        if self.system_collector:
            self._schedule_repeating(
                self.system_interval,
                self._collect_system_metrics,
                "system metrics collection",
            )
            logger.info(
                f"Scheduled system metrics collection every {self.system_interval}s"
            )

        if self.network_collector:
            self._schedule_repeating(
                self.network_interval,
                self._collect_network_metrics,
                "network metrics collection",
            )
            logger.info(
                f"Scheduled network metrics collection every {self.network_interval}s"
            )

        if self.log_parser:
            self._schedule_repeating(self.log_interval, self._parse_logs, "log parsing")
            logger.info(f"Scheduled log parsing every {self.log_interval}s")

        # Schedule aggregations to run every hour
        self._schedule_repeating(
            3600,  # Every hour
            self._run_aggregations,
            "aggregations",
        )
        logger.info("Scheduled aggregations every hour")

        # Schedule cleanup to run once per day
        self._schedule_repeating(
            86400,  # Once per day
            self._cleanup_old_data,
            "data cleanup",
        )
        logger.info("Scheduled data cleanup daily")

        # Run scheduler in background thread
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        logger.info("Logly scheduler started successfully")

    def _run(self):
        """Run the scheduler loop"""
        while self.running:
            try:
                self.scheduler.run(blocking=False)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(5)

    def stop(self):
        """Stop the scheduler"""
        if not self.running:
            return

        logger.info("Stopping Logly scheduler")
        self.running = False

        if self.thread:
            self.thread.join(timeout=5)

        logger.info("Logly scheduler stopped")

    def run_once(self):
        """Run all collection tasks once (for testing)"""
        logger.info("Running collection tasks once")

        if self.system_collector:
            self._collect_system_metrics()

        if self.network_collector:
            self._collect_network_metrics()

        if self.log_parser:
            self._parse_logs()

        logger.info("Collection tasks completed")
