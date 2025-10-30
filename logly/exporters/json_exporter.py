"""
JSON exporter for metrics and log events
"""

import json
import logging
from datetime import datetime
from typing import Optional

from logly.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)


class JSONExporter:
    """Export data to JSON format"""

    def __init__(self, store: SQLiteStore, timestamp_format: str = "%Y-%m-%d %H:%M:%S"):
        """
        Initialize JSON exporter

        Args:
            store: SQLiteStore instance
            timestamp_format: Format for timestamp strings
        """
        self.store = store
        self.timestamp_format = timestamp_format

    def export_system_metrics(self, output_path: str, start_time: int, end_time: int):
        """
        Export system metrics to JSON

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

        # Add readable timestamp
        for metric in metrics:
            metric["timestamp_str"] = datetime.fromtimestamp(
                metric["timestamp"]
            ).strftime(self.timestamp_format)

        # Write JSON
        with open(output_path, "w") as f:
            json.dump(
                {
                    "type": "system_metrics",
                    "start_time": start_time,
                    "end_time": end_time,
                    "count": len(metrics),
                    "data": metrics,
                },
                f,
                indent=2,
            )

        logger.info(f"Exported {len(metrics)} system metrics to {output_path}")

    def export_network_metrics(self, output_path: str, start_time: int, end_time: int):
        """
        Export network metrics to JSON

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

        for metric in metrics:
            metric["timestamp_str"] = datetime.fromtimestamp(
                metric["timestamp"]
            ).strftime(self.timestamp_format)

        with open(output_path, "w") as f:
            json.dump(
                {
                    "type": "network_metrics",
                    "start_time": start_time,
                    "end_time": end_time,
                    "count": len(metrics),
                    "data": metrics,
                },
                f,
                indent=2,
            )

        logger.info(f"Exported {len(metrics)} network metrics to {output_path}")

    def export_log_events(
        self,
        output_path: str,
        start_time: int,
        end_time: int,
        source: Optional[str] = None,
        level: Optional[str] = None,
    ):
        """
        Export log events to JSON

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

        for event in events:
            event["timestamp_str"] = datetime.fromtimestamp(
                event["timestamp"]
            ).strftime(self.timestamp_format)

        with open(output_path, "w") as f:
            json.dump(
                {
                    "type": "log_events",
                    "start_time": start_time,
                    "end_time": end_time,
                    "filters": {"source": source, "level": level},
                    "count": len(events),
                    "data": events,
                },
                f,
                indent=2,
            )

        logger.info(f"Exported {len(events)} log events to {output_path}")
