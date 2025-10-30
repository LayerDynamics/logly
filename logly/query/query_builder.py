"""
Fluent query builder interface for constructing complex queries.
"""

import time
from typing import List, Dict, Any, Optional
from logly.storage.sqlite_store import SQLiteStore


class BaseQuery:
    """Base class for all query types."""

    def __init__(self, store: SQLiteStore):
        self.store = store
        self._start_time: Optional[int] = None
        self._end_time: Optional[int] = None
        self._limit: Optional[int] = None

    def in_last_hours(self, hours: int):
        """Filter to events in the last N hours."""
        self._end_time = int(time.time())
        self._start_time = self._end_time - (hours * 3600)
        return self

    def in_last_days(self, days: int):
        """Filter to events in the last N days."""
        self._end_time = int(time.time())
        self._start_time = self._end_time - (days * 86400)
        return self

    def between(self, start_time: int, end_time: int):
        """Filter to events between specific timestamps."""
        self._start_time = start_time
        self._end_time = end_time
        return self

    def limit(self, count: int):
        """Limit the number of results."""
        self._limit = count
        return self

    def _get_time_range(self):
        """Get the time range for the query."""
        if self._start_time is None or self._end_time is None:
            # Default to last 24 hours
            end_time = int(time.time())
            start_time = end_time - 86400
            return start_time, end_time
        return self._start_time, self._end_time


class EventQuery(BaseQuery):
    """Query builder for log events."""

    def __init__(self, store: SQLiteStore):
        super().__init__(store)
        self._source: Optional[str] = None
        self._level: Optional[str] = None

    def with_level(self, level: str):
        """Filter by log level (INFO, WARNING, ERROR)."""
        self._level = level.upper()
        return self

    def by_source(self, source: str):
        """Filter by log source (fail2ban, syslog, auth, etc.)."""
        self._source = source
        return self

    def errors_only(self):
        """Shorthand for filtering to ERROR level."""
        return self.with_level("ERROR")

    def warnings_only(self):
        """Shorthand for filtering to WARNING level."""
        return self.with_level("WARNING")

    def all(self) -> List[Dict[str, Any]]:
        """Execute the query and return all matching events."""
        start_time, end_time = self._get_time_range()
        return self.store.get_log_events(
            start_time,
            end_time,
            source=self._source,
            level=self._level,
            limit=self._limit or 1000,
        )

    def count(self) -> int:
        """Get the count of matching events."""
        return len(self.all())

    def first(self) -> Optional[Dict[str, Any]]:
        """Get the first matching event."""
        results = self.limit(1).all()
        return results[0] if results else None


class MetricQuery(BaseQuery):
    """Query builder for system/network metrics."""

    def __init__(self, store: SQLiteStore):
        super().__init__(store)
        self._metric_type: str = "system"  # system or network

    def system(self):
        """Query system metrics."""
        self._metric_type = "system"
        return self

    def network(self):
        """Query network metrics."""
        self._metric_type = "network"
        return self

    def all(self) -> List[Dict[str, Any]]:
        """Execute the query and return all matching metrics."""
        start_time, end_time = self._get_time_range()

        if self._metric_type == "system":
            return self.store.get_system_metrics(
                start_time, end_time, limit=self._limit or 1000
            )
        else:
            return self.store.get_network_metrics(
                start_time, end_time, limit=self._limit or 1000
            )

    def latest(self) -> Optional[Dict[str, Any]]:
        """Get the most recent metric."""
        results = self.limit(1).all()
        return results[0] if results else None

    def avg(self, field: str) -> float:
        """Calculate average value for a field."""
        metrics = self.all()
        if not metrics:
            return 0.0

        values = [m.get(field, 0) for m in metrics]
        return sum(values) / len(values)

    def max(self, field: str) -> float:
        """Get maximum value for a field."""
        metrics = self.all()
        if not metrics:
            return 0.0

        values = [m.get(field, 0) for m in metrics]
        return max(values)

    def min(self, field: str) -> float:
        """Get minimum value for a field."""
        metrics = self.all()
        if not metrics:
            return 0.0

        values = [m.get(field, 0) for m in metrics]
        return min(values)


class TraceQuery(BaseQuery):
    """Query builder for event traces."""

    def __init__(self, store: SQLiteStore):
        super().__init__(store)
        self._source: Optional[str] = None
        self._min_severity: int = 0

    def by_source(self, source: str):
        """Filter by source."""
        self._source = source
        return self

    def with_severity(self, min_severity: int):
        """Filter by minimum severity (0-100)."""
        self._min_severity = min_severity
        return self

    def critical_only(self):
        """Shorthand for filtering to critical severity (80+)."""
        return self.with_severity(80)

    def high_severity(self):
        """Shorthand for filtering to high severity (60+)."""
        return self.with_severity(60)

    def all(self) -> List[Dict[str, Any]]:
        """Execute the query and return all matching traces."""
        start_time, end_time = self._get_time_range()
        return self.store.get_traces(
            start_time,
            end_time,
            source=self._source,
            min_severity=self._min_severity,
            limit=self._limit or 100,
        )

    def count(self) -> int:
        """Get the count of matching traces."""
        return len(self.all())


class ErrorQuery(BaseQuery):
    """Query builder specifically for errors."""

    def __init__(self, store: SQLiteStore):
        super().__init__(store)
        self._category: Optional[str] = None

    def by_category(self, category: str):
        """Filter by error category (database, resource, network, etc.)."""
        self._category = category
        return self

    def database_errors(self):
        """Shorthand for database errors."""
        return self.by_category("database")

    def resource_errors(self):
        """Shorthand for resource errors."""
        return self.by_category("resource")

    def network_errors(self):
        """Shorthand for network errors."""
        return self.by_category("network")

    def all(self) -> List[Dict[str, Any]]:
        """Execute the query and return all matching errors."""
        start_time, end_time = self._get_time_range()

        # Get individual error trace records
        return self.store.get_error_traces(
            start_time,
            end_time,
            category=self._category,
            limit=self._limit
        )

    def count(self) -> int:
        """Get the count of errors."""
        return len(self.all())

    def by_type(self) -> Dict[str, int]:
        """Group errors by error type."""
        patterns = self.all()
        result = {}
        for pattern in patterns:
            error_type = pattern.get("error_type", "unknown")
            result[error_type] = result.get(error_type, 0) + pattern.get(
                "error_count", 1
            )
        return result


class IPQuery(BaseQuery):
    """Query builder for IP reputation data."""

    def __init__(self, store: SQLiteStore):
        super().__init__(store)
        self._min_threat: int = 0
        self._ip_address: Optional[str] = None

    def with_threat_above(self, threshold: int):
        """Filter to IPs with threat score above threshold."""
        self._min_threat = threshold
        return self

    def high_threat(self):
        """Shorthand for high-threat IPs (70+)."""
        return self.with_threat_above(70)

    def for_ip(self, ip_address: str):
        """Query specific IP address."""
        self._ip_address = ip_address
        return self

    def all(self) -> List[Dict[str, Any]]:
        """Execute the query and return all matching IPs."""
        if self._ip_address:
            # Query specific IP
            ip_data = self.store.get_ip_reputation(self._ip_address)
            return [ip_data] if ip_data else []
        else:
            # Query high-threat IPs
            ips = self.store.get_high_threat_ips(self._min_threat)
            if self._limit:
                ips = ips[: self._limit]
            return ips

    def count(self) -> int:
        """Get the count of matching IPs."""
        return len(self.all())

    def sort_by_threat(self) -> List[Dict[str, Any]]:
        """Sort results by threat score (highest first)."""
        ips = self.all()
        return sorted(ips, key=lambda x: x.get("threat_score", 0), reverse=True)

    def sort_by_activity(self) -> List[Dict[str, Any]]:
        """Sort results by activity count (most active first)."""
        ips = self.all()
        return sorted(
            ips,
            key=lambda x: x.get("failed_login_count", 0) + x.get("ban_count", 0),
            reverse=True,
        )


class QueryBuilder:
    """Main query builder interface."""

    def __init__(self, store: SQLiteStore):
        """
        Initialize the query builder.

        Args:
            store: SQLiteStore instance for data access
        """
        self.store = store

    def events(self) -> EventQuery:
        """
        Start building a query for log events.

        Returns:
            EventQuery instance for chaining

        Example:
            query.events().in_last_hours(24).with_level("ERROR").by_source("django").all()
        """
        return EventQuery(self.store)

    def metrics(self) -> MetricQuery:
        """
        Start building a query for metrics.

        Returns:
            MetricQuery instance for chaining

        Example:
            query.metrics().system().in_last_days(7).avg("cpu_percent")
        """
        return MetricQuery(self.store)

    def traces(self) -> TraceQuery:
        """
        Start building a query for event traces.

        Returns:
            TraceQuery instance for chaining

        Example:
            query.traces().in_last_hours(24).critical_only().all()
        """
        return TraceQuery(self.store)

    def errors(self) -> ErrorQuery:
        """
        Start building a query for errors.

        Returns:
            ErrorQuery instance for chaining

        Example:
            query.errors().in_last_days(7).database_errors().all()
        """
        return ErrorQuery(self.store)

    def ips(self) -> IPQuery:
        """
        Start building a query for IP reputation data.

        Returns:
            IPQuery instance for chaining

        Example:
            query.ips().high_threat().sort_by_threat()
        """
        return IPQuery(self.store)

    # Convenience methods for common queries

    def recent_errors(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get recent error events (convenience method).

        Args:
            hours: Hours to look back (default: 24)

        Returns:
            List of error events
        """
        return self.events().in_last_hours(hours).errors_only().all()

    def system_health_snapshot(self) -> Dict[str, Any]:
        """
        Get a snapshot of current system health (convenience method).

        Returns:
            Dictionary with latest metrics
        """
        latest_system = self.metrics().system().latest()
        latest_network = self.metrics().network().latest()

        return {
            "system": latest_system,
            "network": latest_network,
            "timestamp": int(time.time()),
        }

    def threat_summary(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get a summary of recent security threats (convenience method).

        Args:
            hours: Hours to look back (default: 24)

        Returns:
            Dictionary with threat summary
        """
        high_threat_ips = self.ips().high_threat().all()
        failed_logins = self.events().in_last_hours(hours).all()
        failed_logins = [e for e in failed_logins if e.get("action") == "failed_login"]

        return {
            "high_threat_ips": len(high_threat_ips),
            "failed_login_attempts": len(failed_logins),
            "top_threat_ips": sorted(
                high_threat_ips, key=lambda x: x.get("threat_score", 0), reverse=True
            )[:5],
            "timestamp": int(time.time()),
            "time_window_hours": hours,
        }
