"""
CSV exporter for metrics and log events
"""

import csv
import logging
from datetime import datetime

from logly.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)


class CSVExporter:
    """Export data to CSV format"""

    def __init__(self, store: SQLiteStore, timestamp_format: str = "%Y-%m-%d %H:%M:%S"):
        """
        Initialize CSV exporter

        Args:
            store: SQLiteStore instance
            timestamp_format: Format for timestamp strings
        """
        self.store = store
        self.timestamp_format = timestamp_format

    def export_system_metrics(self, output_path: str, start_time: int, end_time: int):
        """
        Export system metrics to CSV

        Args:
            output_path: Output file path
            start_time: Start timestamp
            end_time: End timestamp
        """
        logger.info(f"Exporting system metrics to {output_path}")

        metrics = self.store.get_system_metrics(start_time, end_time)

        if not metrics:
            logger.warning("No system metrics found")
            return

        # Write CSV
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=metrics[0].keys())
            writer.writeheader()

            for metric in metrics:
                # Convert timestamp to readable format
                metric["timestamp_str"] = datetime.fromtimestamp(
                    metric["timestamp"]
                ).strftime(self.timestamp_format)
                writer.writerow(metric)

        logger.info(f"Exported {len(metrics)} system metrics to {output_path}")

    def export_network_metrics(self, output_path: str, start_time: int, end_time: int):
        """
        Export network metrics to CSV

        Args:
            output_path: Output file path
            start_time: Start timestamp
            end_time: End timestamp
        """
        logger.info(f"Exporting network metrics to {output_path}")

        metrics = self.store.get_network_metrics(start_time, end_time)

        if not metrics:
            logger.warning("No network metrics found")
            return

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=metrics[0].keys())
            writer.writeheader()

            for metric in metrics:
                metric["timestamp_str"] = datetime.fromtimestamp(
                    metric["timestamp"]
                ).strftime(self.timestamp_format)
                writer.writerow(metric)

        logger.info(f"Exported {len(metrics)} network metrics to {output_path}")

    def export_log_events(
        self,
        output_path: str,
        start_time: int,
        end_time: int,
        source: str | None = None,
        level: str | None = None,
    ):
        """
        Export log events to CSV

        Args:
            output_path: Output file path
            start_time: Start timestamp
            end_time: End timestamp
            source: Optional source filter
            level: Optional level filter
        """
        logger.info(f"Exporting log events to {output_path}")

        events = self.store.get_log_events(
            start_time, end_time, source=source, level=level
        )

        if not events:
            logger.warning("No log events found")
            return

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=events[0].keys())
            writer.writeheader()

            for event in events:
                event["timestamp_str"] = datetime.fromtimestamp(
                    event["timestamp"]
                ).strftime(self.timestamp_format)
                writer.writerow(event)

        logger.info(f"Exported {len(events)} log events to {output_path}")
