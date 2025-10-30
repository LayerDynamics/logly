"""
Unit tests for logly.tracers.event_tracer module
"""

import pytest
from unittest.mock import Mock
from logly.tracers.event_tracer import EventTracer
from logly.storage.models import LogEvent


class TestEventTracer:
    """Test suite for EventTracer"""

    @pytest.mark.unit
    def test_init(self):
        """Test EventTracer initialization"""
        tracer = EventTracer()
        assert tracer is not None

    @pytest.mark.unit
    def test_trace_event(self):
        """Test tracing an event"""
        tracer = EventTracer()
        event = LogEvent(
            timestamp=1000000,
            source="fail2ban",
            message="[sshd] Ban 192.168.1.100",
            level="WARNING"
        )
        
        trace = tracer.trace_event(event)
        
        assert trace is not None
        assert "severity_score" in trace

    @pytest.mark.unit
    def test_calculate_severity(self):
        """Test severity calculation"""
        tracer = EventTracer()
        event = LogEvent(
            timestamp=1000000,
            source="test",
            message="Test",
            level="ERROR",
            action="ban"
        )
        
        score = tracer._calculate_severity(event)
        
        assert score > 0
        assert score <= 100
