"""
Unit tests for logly.storage.models module
Tests data models for metrics and log events
"""

import pytest
import json
import time
from unittest.mock import patch

from logly.storage.models import (
    SystemMetric,
    NetworkMetric,
    LogEvent,
    EventTrace,
    ProcessTrace,
    NetworkTrace,
    ErrorTrace,
)


class TestSystemMetric:
    """Test suite for SystemMetric model"""

    @pytest.mark.unit
    def test_init(self):
        """Test SystemMetric initialization"""
        metric = SystemMetric(
            timestamp=1234567890,
            cpu_percent=45.5,
            cpu_count=4,
            memory_total=8589934592,
            memory_available=4294967296,
            memory_percent=50.0,
        )

        assert metric.timestamp == 1234567890
        assert metric.cpu_percent == 45.5
        assert metric.cpu_count == 4
        assert metric.memory_total == 8589934592
        assert metric.memory_available == 4294967296
        assert metric.memory_percent == 50.0

    @pytest.mark.unit
    @patch("time.time", return_value=1234567890)
    def test_now_classmethod(self, mock_time):
        """Test SystemMetric.now() class method"""
        metric = SystemMetric.now(cpu_percent=30.0, memory_percent=40.0)

        assert metric.timestamp == 1234567890
        assert metric.cpu_percent == 30.0
        assert metric.memory_percent == 40.0

    @pytest.mark.unit
    def test_to_dict(self):
        """Test SystemMetric.to_dict() method"""
        metric = SystemMetric(
            timestamp=1234567890,
            cpu_percent=45.5,
            cpu_count=4,
            memory_total=8000000,
            memory_available=4000000,
            memory_percent=50.0,
            disk_total=100000000,
            disk_used=50000000,
            disk_percent=50.0,
            disk_read_bytes=1000,
            disk_write_bytes=2000,
            load_1min=1.5,
            load_5min=2.0,
            load_15min=1.8,
        )

        data = metric.to_dict()

        assert isinstance(data, dict)
        assert data["timestamp"] == 1234567890
        assert data["cpu_percent"] == 45.5
        assert data["cpu_count"] == 4
        assert data["memory_total"] == 8000000
        assert data["memory_available"] == 4000000
        assert data["memory_percent"] == 50.0
        assert data["disk_total"] == 100000000
        assert data["disk_used"] == 50000000
        assert data["disk_percent"] == 50.0
        assert data["disk_read_bytes"] == 1000
        assert data["disk_write_bytes"] == 2000
        assert data["load_1min"] == 1.5
        assert data["load_5min"] == 2.0
        assert data["load_15min"] == 1.8

    @pytest.mark.unit
    def test_optional_fields(self):
        """Test that optional fields default to None"""
        metric = SystemMetric(timestamp=1234567890)

        assert metric.timestamp == 1234567890
        assert metric.cpu_percent is None
        assert metric.cpu_count is None
        assert metric.memory_total is None
        assert metric.memory_available is None
        assert metric.memory_percent is None
        assert metric.disk_total is None
        assert metric.disk_used is None
        assert metric.disk_percent is None
        assert metric.disk_read_bytes is None
        assert metric.disk_write_bytes is None
        assert metric.load_1min is None
        assert metric.load_5min is None
        assert metric.load_15min is None


class TestNetworkMetric:
    """Test suite for NetworkMetric model"""

    @pytest.mark.unit
    def test_init(self):
        """Test NetworkMetric initialization"""
        metric = NetworkMetric(
            timestamp=1234567890,
            bytes_sent=1000000,
            bytes_recv=2000000,
            packets_sent=1000,
            packets_recv=2000,
            connections_established=10,
            connections_listen=5,
        )

        assert metric.timestamp == 1234567890
        assert metric.bytes_sent == 1000000
        assert metric.bytes_recv == 2000000
        assert metric.packets_sent == 1000
        assert metric.packets_recv == 2000
        assert metric.connections_established == 10
        assert metric.connections_listen == 5

    @pytest.mark.unit
    @patch("time.time", return_value=1234567890)
    def test_now_classmethod(self, mock_time):
        """Test NetworkMetric.now() class method"""
        metric = NetworkMetric.now(bytes_sent=5000, bytes_recv=10000)

        assert metric.timestamp == 1234567890
        assert metric.bytes_sent == 5000
        assert metric.bytes_recv == 10000

    @pytest.mark.unit
    def test_to_dict(self):
        """Test NetworkMetric.to_dict() method"""
        metric = NetworkMetric(
            timestamp=1234567890,
            bytes_sent=1000000,
            bytes_recv=2000000,
            packets_sent=1000,
            packets_recv=2000,
            errors_in=5,
            errors_out=2,
            drops_in=3,
            drops_out=1,
            connections_established=10,
            connections_listen=5,
            connections_time_wait=2,
        )

        data = metric.to_dict()

        assert isinstance(data, dict)
        assert data["timestamp"] == 1234567890
        assert data["bytes_sent"] == 1000000
        assert data["bytes_recv"] == 2000000
        assert data["packets_sent"] == 1000
        assert data["packets_recv"] == 2000
        assert data["errors_in"] == 5
        assert data["errors_out"] == 2
        assert data["drops_in"] == 3
        assert data["drops_out"] == 1
        assert data["connections_established"] == 10
        assert data["connections_listen"] == 5
        assert data["connections_time_wait"] == 2

    @pytest.mark.unit
    def test_optional_fields(self):
        """Test that optional fields default to None"""
        metric = NetworkMetric(timestamp=1234567890)

        assert metric.timestamp == 1234567890
        assert metric.bytes_sent is None
        assert metric.bytes_recv is None
        assert metric.packets_sent is None
        assert metric.packets_recv is None
        assert metric.errors_in is None
        assert metric.errors_out is None
        assert metric.drops_in is None
        assert metric.drops_out is None
        assert metric.connections_established is None
        assert metric.connections_listen is None
        assert metric.connections_time_wait is None


class TestLogEvent:
    """Test suite for LogEvent model"""

    @pytest.mark.unit
    def test_init(self):
        """Test LogEvent initialization"""
        event = LogEvent(
            timestamp=1234567890,
            source="test_source",
            message="Test message",
            level="INFO",
            ip_address="192.168.1.100",
            user="testuser",
            service="test_service",
            action="test_action",
            metadata={"key": "value"},
        )

        assert event.timestamp == 1234567890
        assert event.source == "test_source"
        assert event.message == "Test message"
        assert event.level == "INFO"
        assert event.ip_address == "192.168.1.100"
        assert event.user == "testuser"
        assert event.service == "test_service"
        assert event.action == "test_action"
        assert event.metadata == {"key": "value"}

    @pytest.mark.unit
    @patch("time.time", return_value=1234567890)
    def test_now_classmethod(self, mock_time):
        """Test LogEvent.now() class method"""
        event = LogEvent.now(
            source="test", message="Test event", level="WARNING", ip_address="10.0.0.1"
        )

        assert event.timestamp == 1234567890
        assert event.source == "test"
        assert event.message == "Test event"
        assert event.level == "WARNING"
        assert event.ip_address == "10.0.0.1"

    @pytest.mark.unit
    def test_to_dict_with_metadata(self):
        """Test LogEvent.to_dict() with metadata"""
        event = LogEvent(
            timestamp=1234567890,
            source="test",
            message="Test message",
            metadata={"key1": "value1", "key2": 42},
        )

        data = event.to_dict()

        assert isinstance(data, dict)
        assert data["timestamp"] == 1234567890
        assert data["source"] == "test"
        assert data["message"] == "Test message"
        assert data["metadata"] == '{"key1": "value1", "key2": 42}'  # JSON string

    @pytest.mark.unit
    def test_to_dict_without_metadata(self):
        """Test LogEvent.to_dict() without metadata"""
        event = LogEvent(timestamp=1234567890, source="test", message="Test message")

        data = event.to_dict()

        assert data["timestamp"] == 1234567890
        assert data["source"] == "test"
        assert data["message"] == "Test message"
        assert data.get("metadata") is None

    @pytest.mark.unit
    def test_from_dict_with_metadata(self):
        """Test LogEvent.from_dict() with metadata"""
        data = {
            "timestamp": 1234567890,
            "source": "test",
            "message": "Test message",
            "level": "INFO",
            "metadata": '{"key": "value"}',  # JSON string
        }

        event = LogEvent.from_dict(data)

        assert event.timestamp == 1234567890
        assert event.source == "test"
        assert event.message == "Test message"
        assert event.level == "INFO"
        assert event.metadata == {"key": "value"}  # Parsed dict

    @pytest.mark.unit
    def test_from_dict_with_dict_metadata(self):
        """Test LogEvent.from_dict() when metadata is already a dict"""
        data = {
            "timestamp": 1234567890,
            "source": "test",
            "message": "Test message",
            "metadata": {"key": "value"},  # Already a dict
        }

        event = LogEvent.from_dict(data)

        assert event.metadata == {"key": "value"}

    @pytest.mark.unit
    def test_optional_fields(self):
        """Test that optional fields default to None"""
        event = LogEvent(timestamp=1234567890, source="test", message="Test")

        assert event.timestamp == 1234567890
        assert event.source == "test"
        assert event.message == "Test"
        assert event.level is None
        assert event.ip_address is None
        assert event.user is None
        assert event.service is None
        assert event.action is None
        assert event.metadata is None


class TestEventTrace:
    """Test suite for EventTrace model"""

    @pytest.mark.unit
    def test_init(self):
        """Test EventTrace initialization"""
        trace = EventTrace(
            timestamp=1234567890,
            source="test_source",
            message="Test message",
            severity_score=75,
            event_id=123,
            level="ERROR",
            action="test_action",
            service="test_service",
            user="testuser",
            ip_address="192.168.1.100",
            root_cause="test_cause",
            trigger_event="test_trigger",
            causality_chain=["step1", "step2"],
            related_services=["service1", "service2"],
            tracer_version="1.0",
            tracers_used=["event", "process"],
            traced_at=1234567890,
        )

        assert trace.timestamp == 1234567890
        assert trace.source == "test_source"
        assert trace.message == "Test message"
        assert trace.severity_score == 75
        assert trace.event_id == 123
        assert trace.level == "ERROR"
        assert trace.action == "test_action"
        assert trace.service == "test_service"
        assert trace.user == "testuser"
        assert trace.ip_address == "192.168.1.100"
        assert trace.root_cause == "test_cause"
        assert trace.trigger_event == "test_trigger"
        assert trace.causality_chain == ["step1", "step2"]
        assert trace.related_services == ["service1", "service2"]
        assert trace.tracer_version == "1.0"
        assert trace.tracers_used == ["event", "process"]
        assert trace.traced_at == 1234567890

    @pytest.mark.unit
    def test_from_trace_dict(self):
        """Test EventTrace.from_trace_dict() class method"""
        trace_dict = {
            "event_id": 123,
            "timestamp": 1234567890,
            "source": "test",
            "message": "Test message",
            "level": "ERROR",
            "severity_score": 80,
            "causality": {
                "root_cause": "network_error",
                "trigger": "connection_timeout",
                "chain": ["connect", "timeout", "error"],
            },
            "related_services": ["nginx", "django"],
            "trace_metadata": {
                "tracer_version": "1.0",
                "tracers_used": ["event", "network"],
                "traced_at": 1234567890,
            },
        }

        trace = EventTrace.from_trace_dict(trace_dict)

        assert trace.event_id == 123
        assert trace.timestamp == 1234567890
        assert trace.source == "test"
        assert trace.message == "Test message"
        assert trace.level == "ERROR"
        assert trace.severity_score == 80
        assert trace.root_cause == "network_error"
        assert trace.trigger_event == "connection_timeout"
        assert trace.causality_chain == ["connect", "timeout", "error"]
        assert trace.related_services == ["nginx", "django"]
        assert trace.tracer_version == "1.0"
        assert trace.tracers_used == ["event", "network"]
        assert trace.traced_at == 1234567890

    @pytest.mark.unit
    def test_to_dict(self):
        """Test EventTrace.to_dict() method"""
        trace = EventTrace(
            timestamp=1234567890,
            source="test",
            message="Test",
            severity_score=50,
            causality_chain=["step1", "step2"],
            related_services=["service1"],
            tracers_used=["event"],
        )

        data = trace.to_dict()

        assert isinstance(data, dict)
        assert data["timestamp"] == 1234567890
        assert data["source"] == "test"
        assert data["message"] == "Test"
        assert data["severity_score"] == 50
        assert data["causality_chain"] == '["step1", "step2"]'  # JSON string
        assert data["related_services"] == '["service1"]'  # JSON string
        assert data["tracers_used"] == '["event"]'  # JSON string


class TestProcessTrace:
    """Test suite for ProcessTrace model"""

    @pytest.mark.unit
    def test_init(self):
        """Test ProcessTrace initialization"""
        trace = ProcessTrace(
            trace_id=1,
            pid=1234,
            timestamp=1234567890,
            name="nginx",
            cmdline="/usr/sbin/nginx",
            state="S",
            parent_pid=1,
            memory_rss=8192,
            memory_vm=16384,
            cpu_utime=1000,
            cpu_stime=500,
            threads=4,
            read_bytes=100000,
            write_bytes=50000,
        )

        assert trace.trace_id == 1
        assert trace.pid == 1234
        assert trace.timestamp == 1234567890
        assert trace.name == "nginx"
        assert trace.cmdline == "/usr/sbin/nginx"
        assert trace.state == "S"
        assert trace.parent_pid == 1
        assert trace.memory_rss == 8192
        assert trace.memory_vm == 16384
        assert trace.cpu_utime == 1000
        assert trace.cpu_stime == 500
        assert trace.threads == 4
        assert trace.read_bytes == 100000
        assert trace.write_bytes == 50000

    @pytest.mark.unit
    def test_to_dict(self):
        """Test ProcessTrace.to_dict() method"""
        trace = ProcessTrace(
            trace_id=1, pid=1234, timestamp=1234567890, name="test_process"
        )

        data = trace.to_dict()

        assert isinstance(data, dict)
        assert data["trace_id"] == 1
        assert data["pid"] == 1234
        assert data["timestamp"] == 1234567890
        assert data["name"] == "test_process"


class TestNetworkTrace:
    """Test suite for NetworkTrace model"""

    @pytest.mark.unit
    def test_init(self):
        """Test NetworkTrace initialization"""
        trace = NetworkTrace(
            trace_id=1,
            timestamp=1234567890,
            local_ip="127.0.0.1",
            local_port=8080,
            remote_ip="192.168.1.100",
            remote_port=443,
            state="ESTABLISHED",
            protocol="tcp",
        )

        assert trace.trace_id == 1
        assert trace.timestamp == 1234567890
        assert trace.local_ip == "127.0.0.1"
        assert trace.local_port == 8080
        assert trace.remote_ip == "192.168.1.100"
        assert trace.remote_port == 443
        assert trace.state == "ESTABLISHED"
        assert trace.protocol == "tcp"

    @pytest.mark.unit
    def test_default_protocol(self):
        """Test that protocol defaults to tcp"""
        trace = NetworkTrace(trace_id=1, timestamp=1234567890)

        assert trace.protocol == "tcp"

    @pytest.mark.unit
    def test_to_dict(self):
        """Test NetworkTrace.to_dict() method"""
        trace = NetworkTrace(
            trace_id=1, timestamp=1234567890, local_ip="10.0.0.1", local_port=80
        )

        data = trace.to_dict()

        assert isinstance(data, dict)
        assert data["trace_id"] == 1
        assert data["timestamp"] == 1234567890
        assert data["local_ip"] == "10.0.0.1"
        assert data["local_port"] == 80
        assert data["protocol"] == "tcp"


class TestErrorTrace:
    """Test suite for ErrorTrace model"""

    @pytest.mark.unit
    def test_init(self):
        """Test ErrorTrace initialization"""
        trace = ErrorTrace(
            trace_id=1,
            timestamp=1234567890,
            error_type="connection_error",
            error_category="network",
            exception_type="ConnectionTimeout",
            severity=75,
            file_path="/app/main.py",
            line_number=42,
            error_code="E001",
            has_stacktrace=True,
            root_cause_hints=["network_issue", "timeout"],
            recovery_suggestions=["retry", "check_network"],
        )

        assert trace.trace_id == 1
        assert trace.timestamp == 1234567890
        assert trace.error_type == "connection_error"
        assert trace.error_category == "network"
        assert trace.exception_type == "ConnectionTimeout"
        assert trace.severity == 75
        assert trace.file_path == "/app/main.py"
        assert trace.line_number == 42
        assert trace.error_code == "E001"
        assert trace.has_stacktrace
        assert trace.root_cause_hints == ["network_issue", "timeout"]
        assert trace.recovery_suggestions == ["retry", "check_network"]

    @pytest.mark.unit
    def test_to_dict(self):
        """Test ErrorTrace.to_dict() method"""
        trace = ErrorTrace(
            trace_id=1,
            timestamp=1234567890,
            error_type="test_error",
            has_stacktrace=True,
            root_cause_hints=["hint1", "hint2"],
            recovery_suggestions=["suggestion1"],
        )

        data = trace.to_dict()

        assert isinstance(data, dict)
        assert data["trace_id"] == 1
        assert data["timestamp"] == 1234567890
        assert data["error_type"] == "test_error"
        assert data["has_stacktrace"] == 1  # Boolean converted to int
        assert data["root_cause_hints"] == '["hint1", "hint2"]'  # JSON string
        assert data["recovery_suggestions"] == '["suggestion1"]'  # JSON string

    @pytest.mark.unit
    def test_to_dict_no_lists(self):
        """Test ErrorTrace.to_dict() with no list fields"""
        trace = ErrorTrace(
            trace_id=1,
            timestamp=1234567890,
            error_type="test_error",
            has_stacktrace=False,
        )

        data = trace.to_dict()

        assert data["has_stacktrace"] == 0  # False converted to 0
        assert data["root_cause_hints"] is None
        assert data["recovery_suggestions"] is None
