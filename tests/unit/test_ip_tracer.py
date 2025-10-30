"""
Unit tests for logly.tracers.ip_tracer module
"""

import pytest
from logly.tracers.ip_tracer import IPTracer


class TestIPTracer:
    """Test suite for IPTracer"""

    @pytest.mark.unit
    def test_init(self):
        """Test IPTracer initialization"""
        tracer = IPTracer()
        assert tracer is not None

    @pytest.mark.unit
    def test_trace_ip(self):
        """Test tracing an IP address"""
        tracer = IPTracer()
        
        trace = tracer.trace_ip("192.168.1.100")
        
        assert trace is not None
        assert "ip" in trace
        assert "type" in trace
        assert "threat_score" in trace

    @pytest.mark.unit
    def test_classify_ip_private(self):
        """Test classifying private IP"""
        tracer = IPTracer()
        
        ip_type = tracer._classify_ip("192.168.1.1")
        
        assert ip_type == "private"

    @pytest.mark.unit
    def test_classify_ip_localhost(self):
        """Test classifying localhost"""
        tracer = IPTracer()
        
        ip_type = tracer._classify_ip("127.0.0.1")
        
        assert ip_type == "localhost"
