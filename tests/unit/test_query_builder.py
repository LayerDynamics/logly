"""
Unit tests for QueryBuilder and query classes
"""
import pytest
import time
from unittest.mock import Mock
from logly.query.query_builder import (
    QueryBuilder, EventQuery, MetricQuery, TraceQuery,
    ErrorQuery, IPQuery
)


@pytest.fixture
def mock_store():
    """Create a mock SQLiteStore"""
    return Mock()


@pytest.fixture
def query_builder(mock_store):
    """Create QueryBuilder with mock store"""
    return QueryBuilder(mock_store)


class TestEventQuery:
    """Tests for EventQuery class"""

    def test_event_query_basic(self, query_builder, mock_store):
        """Test basic event query"""
        mock_store.get_log_events.return_value = [
            {"timestamp": int(time.time()), "message": "test", "level": "INFO"}
        ]

        results = query_builder.events().all()

        assert len(results) == 1
        mock_store.get_log_events.assert_called_once()

    def test_event_query_with_level(self, query_builder, mock_store):
        """Test event query with level filter"""
        mock_store.get_log_events.return_value = []

        query_builder.events().with_level("ERROR").all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["level"] == "ERROR"

    def test_event_query_by_source(self, query_builder, mock_store):
        """Test event query with source filter"""
        mock_store.get_log_events.return_value = []

        query_builder.events().by_source("fail2ban").all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["source"] == "fail2ban"

    def test_event_query_errors_only(self, query_builder, mock_store):
        """Test errors_only shorthand"""
        mock_store.get_log_events.return_value = []

        query_builder.events().errors_only().all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["level"] == "ERROR"

    def test_event_query_warnings_only(self, query_builder, mock_store):
        """Test warnings_only shorthand"""
        mock_store.get_log_events.return_value = []

        query_builder.events().warnings_only().all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["level"] == "WARNING"

    def test_event_query_in_last_hours(self, query_builder, mock_store):
        """Test time range filtering by hours"""
        mock_store.get_log_events.return_value = []

        current_time = int(time.time())
        query_builder.events().in_last_hours(24).all()

        call_args = mock_store.get_log_events.call_args
        start_time = call_args[0][0]
        end_time = call_args[0][1]

        # Check that time range is approximately 24 hours
        assert abs((end_time - start_time) - 86400) < 10

    def test_event_query_in_last_days(self, query_builder, mock_store):
        """Test time range filtering by days"""
        mock_store.get_log_events.return_value = []

        query_builder.events().in_last_days(7).all()

        call_args = mock_store.get_log_events.call_args
        start_time = call_args[0][0]
        end_time = call_args[0][1]

        # Check that time range is approximately 7 days
        assert abs((end_time - start_time) - (7 * 86400)) < 10

    def test_event_query_between(self, query_builder, mock_store):
        """Test time range filtering with specific timestamps"""
        mock_store.get_log_events.return_value = []

        start = 1000000
        end = 2000000
        query_builder.events().between(start, end).all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[0][0] == start
        assert call_args[0][1] == end

    def test_event_query_limit(self, query_builder, mock_store):
        """Test result limiting"""
        mock_store.get_log_events.return_value = []

        query_builder.events().limit(50).all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["limit"] == 50

    def test_event_query_chaining(self, query_builder, mock_store):
        """Test method chaining"""
        mock_store.get_log_events.return_value = []

        query_builder.events()\
            .in_last_hours(24)\
            .with_level("ERROR")\
            .by_source("django")\
            .limit(100)\
            .all()

        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["level"] == "ERROR"
        assert call_args[1]["source"] == "django"
        assert call_args[1]["limit"] == 100

    def test_event_query_count(self, query_builder, mock_store):
        """Test count method"""
        mock_store.get_log_events.return_value = [
            {"id": 1}, {"id": 2}, {"id": 3}
        ]

        count = query_builder.events().count()

        assert count == 3

    def test_event_query_first(self, query_builder, mock_store):
        """Test first method"""
        mock_store.get_log_events.return_value = [
            {"id": 1, "message": "first"},
            {"id": 2, "message": "second"}
        ]

        first = query_builder.events().first()

        assert first["id"] == 1
        assert first["message"] == "first"

    def test_event_query_first_empty(self, query_builder, mock_store):
        """Test first method with no results"""
        mock_store.get_log_events.return_value = []

        first = query_builder.events().first()

        assert first is None


class TestMetricQuery:
    """Tests for MetricQuery class"""

    def test_metric_query_system_default(self, query_builder, mock_store):
        """Test that system metrics are default"""
        mock_store.get_system_metrics.return_value = []

        query_builder.metrics().all()

        mock_store.get_system_metrics.assert_called_once()

    def test_metric_query_system_explicit(self, query_builder, mock_store):
        """Test explicit system metrics query"""
        mock_store.get_system_metrics.return_value = []

        query_builder.metrics().system().all()

        mock_store.get_system_metrics.assert_called_once()

    def test_metric_query_network(self, query_builder, mock_store):
        """Test network metrics query"""
        mock_store.get_network_metrics.return_value = []

        query_builder.metrics().network().all()

        mock_store.get_network_metrics.assert_called_once()

    def test_metric_query_latest(self, query_builder, mock_store):
        """Test latest metric retrieval"""
        mock_store.get_system_metrics.return_value = [
            {"timestamp": int(time.time()), "cpu_percent": 50.0}
        ]

        latest = query_builder.metrics().system().latest()

        assert latest["cpu_percent"] == 50.0

    def test_metric_query_avg(self, query_builder, mock_store):
        """Test average calculation"""
        mock_store.get_system_metrics.return_value = [
            {"cpu_percent": 40.0},
            {"cpu_percent": 50.0},
            {"cpu_percent": 60.0}
        ]

        avg = query_builder.metrics().system().avg("cpu_percent")

        assert avg == 50.0

    def test_metric_query_avg_empty(self, query_builder, mock_store):
        """Test average with no data"""
        mock_store.get_system_metrics.return_value = []

        avg = query_builder.metrics().system().avg("cpu_percent")

        assert avg == 0.0

    def test_metric_query_max(self, query_builder, mock_store):
        """Test maximum value"""
        mock_store.get_system_metrics.return_value = [
            {"cpu_percent": 40.0},
            {"cpu_percent": 90.0},
            {"cpu_percent": 60.0}
        ]

        max_val = query_builder.metrics().system().max("cpu_percent")

        assert max_val == 90.0

    def test_metric_query_min(self, query_builder, mock_store):
        """Test minimum value"""
        mock_store.get_system_metrics.return_value = [
            {"cpu_percent": 40.0},
            {"cpu_percent": 90.0},
            {"cpu_percent": 20.0}
        ]

        min_val = query_builder.metrics().system().min("cpu_percent")

        assert min_val == 20.0


class TestTraceQuery:
    """Tests for TraceQuery class"""

    def test_trace_query_basic(self, query_builder, mock_store):
        """Test basic trace query"""
        mock_store.get_traces.return_value = []

        query_builder.traces().all()

        mock_store.get_traces.assert_called_once()

    def test_trace_query_by_source(self, query_builder, mock_store):
        """Test trace query with source filter"""
        mock_store.get_traces.return_value = []

        query_builder.traces().by_source("app").all()

        call_args = mock_store.get_traces.call_args
        assert call_args[1]["source"] == "app"

    def test_trace_query_with_severity(self, query_builder, mock_store):
        """Test trace query with severity filter"""
        mock_store.get_traces.return_value = []

        query_builder.traces().with_severity(70).all()

        call_args = mock_store.get_traces.call_args
        assert call_args[1]["min_severity"] == 70

    def test_trace_query_critical_only(self, query_builder, mock_store):
        """Test critical_only shorthand"""
        mock_store.get_traces.return_value = []

        query_builder.traces().critical_only().all()

        call_args = mock_store.get_traces.call_args
        assert call_args[1]["min_severity"] == 80

    def test_trace_query_high_severity(self, query_builder, mock_store):
        """Test high_severity shorthand"""
        mock_store.get_traces.return_value = []

        query_builder.traces().high_severity().all()

        call_args = mock_store.get_traces.call_args
        assert call_args[1]["min_severity"] == 60

    def test_trace_query_count(self, query_builder, mock_store):
        """Test trace count"""
        mock_store.get_traces.return_value = [{"id": 1}, {"id": 2}]

        count = query_builder.traces().count()

        assert count == 2


class TestErrorQuery:
    """Tests for ErrorQuery class"""

    def test_error_query_basic(self, query_builder, mock_store):
        """Test basic error query"""
        mock_store.get_error_traces.return_value = []

        query_builder.errors().all()

        mock_store.get_error_traces.assert_called_once()

    def test_error_query_by_category(self, query_builder, mock_store):
        """Test error query with category filter"""
        mock_store.get_error_traces.return_value = [
            {"error_category": "database", "error_count": 5},
            {"error_category": "network", "error_count": 3}
        ]

        results = query_builder.errors().by_category("database").all()

        assert len(results) == 2  # Mock returns all results, filtering happens in store
        # Verify the category parameter was passed
        call_args = mock_store.get_error_traces.call_args
        assert call_args[1]["category"] == "database"

    def test_error_query_database_errors(self, query_builder, mock_store):
        """Test database_errors shorthand"""
        mock_store.get_error_traces.return_value = [
            {"error_category": "database", "error_count": 5}
        ]

        results = query_builder.errors().database_errors().all()

        assert len(results) == 1
        # Verify the category parameter was passed
        call_args = mock_store.get_error_traces.call_args
        assert call_args[1]["category"] == "database"

    def test_error_query_resource_errors(self, query_builder, mock_store):
        """Test resource_errors shorthand"""
        mock_store.get_error_traces.return_value = [
            {"error_category": "resource", "error_count": 5}
        ]

        results = query_builder.errors().resource_errors().all()

        assert len(results) == 1
        # Verify the category parameter was passed
        call_args = mock_store.get_error_traces.call_args
        assert call_args[1]["category"] == "resource"

    def test_error_query_by_type(self, query_builder, mock_store):
        """Test grouping errors by type"""
        mock_store.get_error_traces.return_value = [
            {"error_type": "timeout", "error_count": 10},
            {"error_type": "connection", "error_count": 5},
            {"error_type": "timeout", "error_count": 3}
        ]

        by_type = query_builder.errors().by_type()

        assert by_type["timeout"] == 13  # 10 + 3
        assert by_type["connection"] == 5


class TestIPQuery:
    """Tests for IPQuery class"""

    def test_ip_query_with_threat_above(self, query_builder, mock_store):
        """Test IP query with threat threshold"""
        mock_store.get_high_threat_ips.return_value = []

        query_builder.ips().with_threat_above(80).all()

        mock_store.get_high_threat_ips.assert_called_once_with(80)

    def test_ip_query_high_threat(self, query_builder, mock_store):
        """Test high_threat shorthand"""
        mock_store.get_high_threat_ips.return_value = []

        query_builder.ips().high_threat().all()

        mock_store.get_high_threat_ips.assert_called_once_with(70)

    def test_ip_query_for_specific_ip(self, query_builder, mock_store):
        """Test query for specific IP"""
        mock_store.get_ip_reputation.return_value = {
            "ip_address": "1.2.3.4",
            "threat_score": 85
        }

        results = query_builder.ips().for_ip("1.2.3.4").all()

        assert len(results) == 1
        assert results[0]["ip_address"] == "1.2.3.4"

    def test_ip_query_for_specific_ip_not_found(self, query_builder, mock_store):
        """Test query for IP that doesn't exist"""
        mock_store.get_ip_reputation.return_value = None

        results = query_builder.ips().for_ip("1.2.3.4").all()

        assert len(results) == 0

    def test_ip_query_sort_by_threat(self, query_builder, mock_store):
        """Test sorting by threat score"""
        mock_store.get_high_threat_ips.return_value = [
            {"ip_address": "1.2.3.4", "threat_score": 70},
            {"ip_address": "5.6.7.8", "threat_score": 90},
            {"ip_address": "9.10.11.12", "threat_score": 80}
        ]

        results = query_builder.ips().sort_by_threat()

        assert results[0]["threat_score"] == 90
        assert results[1]["threat_score"] == 80
        assert results[2]["threat_score"] == 70

    def test_ip_query_sort_by_activity(self, query_builder, mock_store):
        """Test sorting by activity count"""
        mock_store.get_high_threat_ips.return_value = [
            {"ip_address": "1.2.3.4", "failed_login_count": 5, "ban_count": 1},
            {"ip_address": "5.6.7.8", "failed_login_count": 20, "ban_count": 2},
            {"ip_address": "9.10.11.12", "failed_login_count": 10, "ban_count": 0}
        ]

        results = query_builder.ips().sort_by_activity()

        assert results[0]["ip_address"] == "5.6.7.8"  # 20 + 2 = 22
        assert results[1]["ip_address"] == "9.10.11.12"  # 10 + 0 = 10
        assert results[2]["ip_address"] == "1.2.3.4"  # 5 + 1 = 6


class TestConvenienceMethods:
    """Tests for convenience methods"""

    def test_recent_errors(self, query_builder, mock_store):
        """Test recent_errors convenience method"""
        mock_store.get_log_events.return_value = [
            {"level": "ERROR", "message": "test"}
        ]

        errors = query_builder.recent_errors(hours=12)

        assert len(errors) == 1
        call_args = mock_store.get_log_events.call_args
        assert call_args[1]["level"] == "ERROR"

    def test_system_health_snapshot(self, query_builder, mock_store):
        """Test system_health_snapshot convenience method"""
        mock_store.get_system_metrics.return_value = [
            {"cpu_percent": 50.0, "memory_percent": 60.0}
        ]
        mock_store.get_network_metrics.return_value = [
            {"bytes_sent": 1000, "bytes_recv": 2000}
        ]

        snapshot = query_builder.system_health_snapshot()

        assert "system" in snapshot
        assert "network" in snapshot
        assert "timestamp" in snapshot
        assert snapshot["system"]["cpu_percent"] == 50.0

    def test_threat_summary(self, query_builder, mock_store):
        """Test threat_summary convenience method"""
        current_time = int(time.time())

        mock_store.get_high_threat_ips.return_value = [
            {"ip_address": "1.2.3.4", "threat_score": 85},
            {"ip_address": "5.6.7.8", "threat_score": 90}
        ]

        mock_store.get_log_events.return_value = [
            {"timestamp": current_time - 3600, "action": "failed_login"},
            {"timestamp": current_time - 3500, "action": "failed_login"}
        ]

        summary = query_builder.threat_summary(hours=24)

        assert summary["high_threat_ips"] == 2
        assert summary["failed_login_attempts"] == 2
        assert len(summary["top_threat_ips"]) <= 5
        assert summary["time_window_hours"] == 24


class TestQueryBuilderIntegration:
    """Integration tests for QueryBuilder"""

    def test_multiple_query_types(self, query_builder, mock_store):
        """Test using multiple query types with same builder"""
        mock_store.get_log_events.return_value = []
        mock_store.get_system_metrics.return_value = []
        mock_store.get_high_threat_ips.return_value = []

        # Should be able to use different query types
        query_builder.events().all()
        query_builder.metrics().system().all()
        query_builder.ips().high_threat().all()

        # Verify all were called
        assert mock_store.get_log_events.called
        assert mock_store.get_system_metrics.called
        assert mock_store.get_high_threat_ips.called
