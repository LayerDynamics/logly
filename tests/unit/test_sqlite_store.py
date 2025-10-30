"""
Unit tests for logly.storage.sqlite_store module
Tests SQLite storage operations for metrics, logs, and traces
"""

import pytest
import time
import json
from unittest.mock import patch
from pathlib import Path

from logly.storage.sqlite_store import SQLiteStore
from logly.storage.models import SystemMetric, NetworkMetric, LogEvent


class TestSQLiteStore:
    """Test suite for SQLiteStore class"""

    @pytest.mark.unit
    def test_init_with_valid_path(self, temp_db_path):
        """Test SQLiteStore initialization with valid hardcoded path"""
        with patch("logly.storage.sqlite_store.get_db_path", return_value=Path(temp_db_path)):
            with patch("logly.storage.sqlite_store.validate_db_path", return_value=True):
                store = SQLiteStore(temp_db_path)

                assert store.db_path == Path(temp_db_path)
                assert store.db_path.exists()

    @pytest.mark.unit
    def test_init_with_invalid_path(self):
        """Test SQLiteStore initialization with invalid path"""
        invalid_path = "/invalid/custom/path/db.db"

        with patch("logly.storage.sqlite_store.get_db_path", return_value=Path("/expected/path/db.db")):
            with patch("logly.storage.sqlite_store.validate_db_path", return_value=False):
                with pytest.raises(ValueError) as exc_info:
                    SQLiteStore(invalid_path)

                assert "Database path must be" in str(exc_info.value)
                assert "HARDCODED" in str(exc_info.value)

    @pytest.mark.unit
    def test_init_database_creates_tables(self, test_store):
        """Test that database initialization creates all required tables"""
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = {row["name"] for row in cursor.fetchall()}

            # Check essential tables exist
            assert "system_metrics" in tables
            assert "network_metrics" in tables
            assert "log_events" in tables
            assert "hourly_aggregates" in tables
            assert "daily_aggregates" in tables
            assert "event_traces" in tables
            assert "process_traces" in tables
            assert "network_traces" in tables
            assert "error_traces" in tables
            assert "ip_reputation" in tables

    @pytest.mark.unit
    def test_connection_context_manager(self, test_store):
        """Test database connection context manager"""
        with test_store._connection() as conn:
            assert conn is not None
            # Test that connection works
            result = conn.execute("SELECT 1").fetchone()
            assert result is not None

    @pytest.mark.unit
    def test_insert_system_metric(self, test_store, mock_system_metric):
        """Test inserting a system metric"""
        row_id = test_store.insert_system_metric(mock_system_metric)

        assert row_id > 0

        # Verify inserted data
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM system_metrics WHERE id = ?", (row_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["timestamp"] == mock_system_metric.timestamp
            assert row["cpu_percent"] == mock_system_metric.cpu_percent
            assert row["memory_percent"] == mock_system_metric.memory_percent

    @pytest.mark.unit
    def test_get_system_metrics(self, test_store, mock_system_metric):
        """Test retrieving system metrics by time range"""
        # Insert multiple metrics
        base_time = int(time.time())

        metric1 = SystemMetric(timestamp=base_time - 3600, cpu_percent=30.0)
        metric2 = SystemMetric(timestamp=base_time - 1800, cpu_percent=45.0)
        metric3 = SystemMetric(timestamp=base_time - 900, cpu_percent=60.0)

        test_store.insert_system_metric(metric1)
        test_store.insert_system_metric(metric2)
        test_store.insert_system_metric(metric3)

        # Get metrics in range
        metrics = test_store.get_system_metrics(
            start_time=base_time - 3600,
            end_time=base_time
        )

        assert len(metrics) == 3
        # Should be ordered by timestamp DESC
        assert metrics[0]["cpu_percent"] == 60.0
        assert metrics[1]["cpu_percent"] == 45.0
        assert metrics[2]["cpu_percent"] == 30.0

    @pytest.mark.unit
    def test_get_system_metrics_with_limit(self, test_store):
        """Test retrieving system metrics with limit"""
        base_time = int(time.time())

        # Insert 5 metrics
        for i in range(5):
            metric = SystemMetric(timestamp=base_time - (i * 600), cpu_percent=float(i * 10))
            test_store.insert_system_metric(metric)

        # Get only 2 most recent
        metrics = test_store.get_system_metrics(
            start_time=base_time - 3600,
            end_time=base_time,
            limit=2
        )

        assert len(metrics) == 2

    @pytest.mark.unit
    def test_insert_network_metric(self, test_store, mock_network_metric):
        """Test inserting a network metric"""
        row_id = test_store.insert_network_metric(mock_network_metric)

        assert row_id > 0

        # Verify inserted data
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM network_metrics WHERE id = ?", (row_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["timestamp"] == mock_network_metric.timestamp
            assert row["bytes_sent"] == mock_network_metric.bytes_sent
            assert row["bytes_recv"] == mock_network_metric.bytes_recv
            assert row["connections_established"] == mock_network_metric.connections_established

    @pytest.mark.unit
    def test_get_network_metrics(self, test_store):
        """Test retrieving network metrics by time range"""
        base_time = int(time.time())

        metric1 = NetworkMetric(timestamp=base_time - 3600, bytes_sent=1000000)
        metric2 = NetworkMetric(timestamp=base_time - 1800, bytes_sent=2000000)

        test_store.insert_network_metric(metric1)
        test_store.insert_network_metric(metric2)

        metrics = test_store.get_network_metrics(
            start_time=base_time - 3600,
            end_time=base_time
        )

        assert len(metrics) == 2

    @pytest.mark.unit
    def test_insert_log_event(self, test_store, mock_log_event):
        """Test inserting a log event"""
        row_id = test_store.insert_log_event(mock_log_event)

        assert row_id > 0

        # Verify inserted data
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM log_events WHERE id = ?", (row_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["timestamp"] == mock_log_event.timestamp
            assert row["source"] == mock_log_event.source
            assert row["level"] == mock_log_event.level
            assert row["message"] == mock_log_event.message
            assert row["ip_address"] == mock_log_event.ip_address

    @pytest.mark.unit
    def test_insert_log_event_with_metadata(self, test_store):
        """Test inserting log event with metadata"""
        event = LogEvent(
            timestamp=int(time.time()),
            source="test",
            message="Test message",
            metadata={"key1": "value1", "key2": 42}
        )

        row_id = test_store.insert_log_event(event)

        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT metadata FROM log_events WHERE id = ?", (row_id,)
            )
            row = cursor.fetchone()

            # Metadata should be stored as JSON string
            metadata = json.loads(row["metadata"])
            assert metadata["key1"] == "value1"
            assert metadata["key2"] == 42

    @pytest.mark.unit
    def test_get_log_events(self, test_store):
        """Test retrieving log events by time range"""
        base_time = int(time.time())

        event1 = LogEvent(
            timestamp=base_time - 3600,
            source="fail2ban",
            message="Ban IP",
            level="WARNING"
        )
        event2 = LogEvent(
            timestamp=base_time - 1800,
            source="syslog",
            message="System error",
            level="ERROR"
        )

        test_store.insert_log_event(event1)
        test_store.insert_log_event(event2)

        events = test_store.get_log_events(
            start_time=base_time - 3600,
            end_time=base_time
        )

        assert len(events) == 2

    @pytest.mark.unit
    def test_get_log_events_filtered_by_source(self, test_store):
        """Test retrieving log events filtered by source"""
        base_time = int(time.time())

        event1 = LogEvent(timestamp=base_time, source="fail2ban", message="Ban IP")
        event2 = LogEvent(timestamp=base_time, source="syslog", message="Error")
        event3 = LogEvent(timestamp=base_time, source="fail2ban", message="Unban IP")

        test_store.insert_log_event(event1)
        test_store.insert_log_event(event2)
        test_store.insert_log_event(event3)

        events = test_store.get_log_events(
            start_time=base_time - 60,
            end_time=base_time + 60,
            source="fail2ban"
        )

        assert len(events) == 2
        assert all(e["source"] == "fail2ban" for e in events)

    @pytest.mark.unit
    def test_get_log_events_filtered_by_level(self, test_store):
        """Test retrieving log events filtered by level"""
        base_time = int(time.time())

        event1 = LogEvent(timestamp=base_time, source="test", message="Info", level="INFO")
        event2 = LogEvent(timestamp=base_time, source="test", message="Error", level="ERROR")
        event3 = LogEvent(timestamp=base_time, source="test", message="Warning", level="WARNING")

        test_store.insert_log_event(event1)
        test_store.insert_log_event(event2)
        test_store.insert_log_event(event3)

        events = test_store.get_log_events(
            start_time=base_time - 60,
            end_time=base_time + 60,
            level="ERROR"
        )

        assert len(events) == 1
        assert events[0]["level"] == "ERROR"

    @pytest.mark.unit
    def test_compute_hourly_aggregates(self, test_store):
        """Test computing hourly aggregates"""
        base_time = int(time.time())
        hour_start = base_time - (base_time % 3600)  # Round to hour

        # Insert metrics for the hour
        for i in range(5):
            metric = SystemMetric(
                timestamp=hour_start + (i * 600),
                cpu_percent=float(40 + i * 5),
                memory_percent=float(50 + i * 2)
            )
            test_store.insert_system_metric(metric)

        # Compute aggregates
        test_store.compute_hourly_aggregates(hour_start)

        # Verify aggregates
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM hourly_aggregates WHERE hour_timestamp = ?",
                (hour_start,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["avg_cpu_percent"] == pytest.approx(50.0, 0.1)
            assert row["max_cpu_percent"] == 60.0

    @pytest.mark.unit
    def test_compute_daily_aggregates(self, test_store):
        """Test computing daily aggregates"""
        base_time = int(time.time())
        hour_start = base_time - (base_time % 3600)

        # Insert hourly aggregates for the day
        test_store.compute_hourly_aggregates(hour_start)

        # Compute daily aggregates
        date_str = time.strftime("%Y-%m-%d", time.gmtime(hour_start))
        test_store.compute_daily_aggregates(date_str)

        # Verify daily aggregates exist
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM daily_aggregates WHERE date = ?",
                (date_str,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["date"] == date_str

    @pytest.mark.unit
    def test_cleanup_old_data(self, test_store):
        """Test cleaning up old data"""
        current_time = int(time.time())
        old_time = current_time - (100 * 86400)  # 100 days ago
        recent_time = current_time - (10 * 86400)  # 10 days ago

        # Insert old and recent metrics
        old_metric = SystemMetric(timestamp=old_time, cpu_percent=30.0)
        recent_metric = SystemMetric(timestamp=recent_time, cpu_percent=40.0)

        test_store.insert_system_metric(old_metric)
        test_store.insert_system_metric(recent_metric)

        # Cleanup data older than 30 days
        test_store.cleanup_old_data(retention_days=30)

        # Verify old data is deleted
        metrics = test_store.get_system_metrics(
            start_time=old_time - 60,
            end_time=current_time
        )

        # Should only have recent metric
        assert len(metrics) == 1
        assert metrics[0]["timestamp"] == recent_time

    @pytest.mark.unit
    def test_get_stats(self, test_store, mock_system_metric, mock_network_metric, mock_log_event):
        """Test getting database statistics"""
        # Insert some data
        test_store.insert_system_metric(mock_system_metric)
        test_store.insert_network_metric(mock_network_metric)
        test_store.insert_log_event(mock_log_event)

        stats = test_store.get_stats()

        assert "system_metrics" in stats
        assert "network_metrics" in stats
        assert "log_events" in stats
        assert "database_size_mb" in stats

        assert stats["system_metrics"] == 1
        assert stats["network_metrics"] == 1
        assert stats["log_events"] == 1
        assert isinstance(stats["database_size_mb"], float)

    @pytest.mark.unit
    def test_insert_event_trace(self, test_store):
        """Test inserting event trace"""
        trace = {
            'event_id': 1,
            'timestamp': int(time.time()),
            'source': 'test',
            'level': 'ERROR',
            'severity_score': 75,
            'message': 'Test error',
            'action': 'test_action',
            'service': 'test_service',
            'causality': {
                'root_cause': 'network_error',
                'trigger': 'timeout',
                'chain': ['connect', 'timeout', 'error']
            },
            'related_services': ['nginx', 'django'],
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['event', 'error'],
                'traced_at': int(time.time())
            }
        }

        trace_id = test_store.insert_event_trace(trace)

        assert trace_id > 0

        # Verify trace was stored
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM event_traces WHERE id = ?", (trace_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["source"] == "test"
            assert row["severity_score"] == 75
            assert row["root_cause"] == "network_error"

    @pytest.mark.unit
    def test_insert_event_trace_with_processes(self, test_store):
        """Test inserting event trace with process information"""
        trace = {
            'timestamp': int(time.time()),
            'source': 'test',
            'severity_score': 50,
            'message': 'Test',
            'processes': [
                {
                    'pid': 1234,
                    'name': 'nginx',
                    'cmdline': '/usr/sbin/nginx',
                    'parent_pid': 1,
                    'status': {
                        'state': 'S',
                        'vm_rss': 8192,
                        'vm_size': 16384,
                        'threads': 4
                    },
                    'stats': {
                        'utime': 1000,
                        'stime': 500
                    },
                    'io': {
                        'read_bytes': 100000,
                        'write_bytes': 50000,
                        'read_syscalls': 100,
                        'write_syscalls': 50
                    }
                }
            ],
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['process'],
                'traced_at': int(time.time())
            }
        }

        trace_id = test_store.insert_event_trace(trace)

        # Verify process trace was stored
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM process_traces WHERE trace_id = ?", (trace_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["pid"] == 1234
            assert row["name"] == "nginx"

    @pytest.mark.unit
    def test_insert_event_trace_with_network(self, test_store):
        """Test inserting event trace with network connections"""
        trace = {
            'timestamp': int(time.time()),
            'source': 'test',
            'severity_score': 50,
            'message': 'Test',
            'network_connections': [
                {
                    'local_ip': '127.0.0.1',
                    'local_port': 8080,
                    'remote_ip': '192.168.1.100',
                    'remote_port': 443,
                    'state': 'ESTABLISHED',
                    'protocol': 'tcp'
                }
            ],
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['network'],
                'traced_at': int(time.time())
            }
        }

        trace_id = test_store.insert_event_trace(trace)

        # Verify network trace was stored
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM network_traces WHERE trace_id = ?", (trace_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["local_ip"] == "127.0.0.1"
            assert row["remote_ip"] == "192.168.1.100"
            assert row["state"] == "ESTABLISHED"

    @pytest.mark.unit
    def test_insert_event_trace_with_error(self, test_store):
        """Test inserting event trace with error information"""
        trace = {
            'timestamp': int(time.time()),
            'source': 'test',
            'severity_score': 80,
            'message': 'Test error',
            'error_info': {
                'error_type': 'connection_error',
                'error_category': 'network',
                'exception_type': 'ConnectionTimeout',
                'severity': 75,
                'file_path': '/app/main.py',
                'line_number': 42,
                'error_code': 'E001',
                'has_stacktrace': True,
                'root_cause_hints': ['network_issue', 'timeout'],
                'recovery_suggestions': ['retry', 'check_network'],
                'timestamp': int(time.time())
            },
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['error'],
                'traced_at': int(time.time())
            }
        }

        trace_id = test_store.insert_event_trace(trace)

        # Verify error trace was stored
        with test_store._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM error_traces WHERE trace_id = ?", (trace_id,)
            )
            row = cursor.fetchone()

            assert row is not None
            assert row["error_type"] == "connection_error"
            assert row["error_category"] == "network"
            assert row["has_stacktrace"] == 1

    @pytest.mark.unit
    def test_update_ip_reputation(self, test_store):
        """Test updating IP reputation"""
        trace = {
            'timestamp': int(time.time()),
            'source': 'test',
            'severity_score': 50,
            'message': 'Test',
            'ip_info': {
                'ip': '192.168.1.100',
                'type': 'private',
                'is_whitelisted': False,
                'is_known_malicious': True,
                'threat_score': 85,
                'failed_login_count': 5,
                'banned_count': 1
            },
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['ip'],
                'traced_at': int(time.time())
            }
        }

        test_store.insert_event_trace(trace)

        # Verify IP reputation was stored
        ip_rep = test_store.get_ip_reputation('192.168.1.100')

        assert ip_rep is not None
        assert ip_rep['ip'] == '192.168.1.100'
        assert ip_rep['type'] == 'private'
        assert ip_rep['threat_score'] == 85
        assert ip_rep['is_blacklisted'] == 1

    @pytest.mark.unit
    def test_get_traces(self, test_store):
        """Test retrieving traces"""
        base_time = int(time.time())

        trace1 = {
            'timestamp': base_time - 3600,
            'source': 'fail2ban',
            'severity_score': 60,
            'message': 'Ban IP',
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['event'],
                'traced_at': base_time - 3600
            }
        }

        trace2 = {
            'timestamp': base_time - 1800,
            'source': 'syslog',
            'severity_score': 80,
            'message': 'System error',
            'trace_metadata': {
                'tracer_version': '1.0',
                'tracers_used': ['event'],
                'traced_at': base_time - 1800
            }
        }

        test_store.insert_event_trace(trace1)
        test_store.insert_event_trace(trace2)

        traces = test_store.get_traces(
            start_time=base_time - 3600,
            end_time=base_time
        )

        assert len(traces) == 2

    @pytest.mark.unit
    def test_get_traces_filtered(self, test_store):
        """Test retrieving traces with filters"""
        base_time = int(time.time())

        for i in range(3):
            trace = {
                'timestamp': base_time,
                'source': 'fail2ban' if i < 2 else 'syslog',
                'severity_score': 50 + (i * 20),
                'message': f'Test {i}',
                'trace_metadata': {
                    'tracer_version': '1.0',
                    'tracers_used': ['event'],
                    'traced_at': base_time
                }
            }
            test_store.insert_event_trace(trace)

        # Filter by source
        traces = test_store.get_traces(
            start_time=base_time - 60,
            end_time=base_time + 60,
            source='fail2ban'
        )
        assert len(traces) == 2

        # Filter by severity
        traces = test_store.get_traces(
            start_time=base_time - 60,
            end_time=base_time + 60,
            min_severity=70
        )
        assert len(traces) == 2

    @pytest.mark.unit
    def test_get_high_threat_ips(self, test_store):
        """Test retrieving high threat IPs"""
        # Insert IP reputation records
        for i in range(3):
            trace = {
                'timestamp': int(time.time()),
                'source': 'test',
                'severity_score': 50,
                'message': 'Test',
                'ip_info': {
                    'ip': f'192.168.1.{100 + i}',
                    'type': 'private',
                    'threat_score': 50 + (i * 20),
                    'failed_login_count': i,
                    'banned_count': 0
                },
                'trace_metadata': {
                    'tracer_version': '1.0',
                    'tracers_used': ['ip'],
                    'traced_at': int(time.time())
                }
            }
            test_store.insert_event_trace(trace)

        # Get high threat IPs (>= 70)
        high_threat = test_store.get_high_threat_ips(threshold=70)

        assert len(high_threat) == 2
        assert high_threat[0]['threat_score'] >= 70

    @pytest.mark.unit
    def test_get_error_patterns(self, test_store):
        """Test retrieving error patterns"""
        base_time = int(time.time())

        # Insert error traces
        for i in range(3):
            trace = {
                'timestamp': base_time,
                'source': 'test',
                'severity_score': 70,
                'message': 'Error',
                'error_info': {
                    'error_type': 'connection_error' if i < 2 else 'database_error',
                    'error_category': 'network' if i < 2 else 'database',
                    'severity': 70,
                    'timestamp': base_time
                },
                'trace_metadata': {
                    'tracer_version': '1.0',
                    'tracers_used': ['error'],
                    'traced_at': base_time
                }
            }
            test_store.insert_event_trace(trace)

        patterns = test_store.get_error_patterns(
            start_time=base_time - 60,
            end_time=base_time + 60
        )

        assert 'by_type' in patterns
        assert 'by_category' in patterns
        assert len(patterns['by_type']) > 0
        assert len(patterns['by_category']) > 0
