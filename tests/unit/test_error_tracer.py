"""
Unit tests for logly.tracers.error_tracer module
"""

import pytest
from logly.tracers.error_tracer import ErrorTracer


class TestErrorTracer:
    """Test suite for ErrorTracer"""

    @pytest.mark.unit
    def test_init(self):
        """Test ErrorTracer initialization"""
        tracer = ErrorTracer()
        assert tracer is not None

    @pytest.mark.unit
    def test_trace_error(self):
        """Test tracing an error"""
        tracer = ErrorTracer()
        
        trace = tracer.trace_error(
            "ConnectionError: timeout",
            "syslog",
            "ERROR"
        )
        
        assert trace is not None
        assert "error_type" in trace
        assert "severity" in trace

    @pytest.mark.unit
    def test_categorize_error(self):
        """Test error categorization"""
        tracer = ErrorTracer()
        
        category = tracer._categorize_error("ConnectionError")
        
        assert category is not None
