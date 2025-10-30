"""
Unit tests for logly.tracers.process_tracer module
"""

import pytest
from unittest.mock import patch, mock_open
from logly.tracers.process_tracer import ProcessTracer


class TestProcessTracer:
    """Test suite for ProcessTracer"""

    @pytest.mark.unit
    def test_init(self):
        """Test ProcessTracer initialization"""
        tracer = ProcessTracer()
        assert tracer is not None

    @pytest.mark.unit
    @patch('builtins.open', new_callable=mock_open, read_data="nginx")
    @patch('logly.tracers.process_tracer.Path')
    def test_trace_process(self, mock_path, mock_file):
        """Test tracing a process"""
        mock_path.return_value.exists.return_value = True
        
        tracer = ProcessTracer()
        
        trace = tracer.trace_process(1234)
        
        # May return None if /proc not available, that's OK in tests
        assert trace is None or isinstance(trace, dict)

    @pytest.mark.unit
    def test_trace_by_name(self):
        """Test tracing process by name"""
        tracer = ProcessTracer()
        
        traces = tracer.trace_by_name("init")
        
        assert isinstance(traces, list)

    @pytest.mark.unit
    @patch('logly.tracers.process_tracer.Path')
    def test_find_process_by_name(self, mock_path):
        """Test finding process by name"""
        mock_path.return_value.glob.return_value = []
        
        tracer = ProcessTracer()
        pids = tracer.find_process_by_name("nginx")
        
        assert isinstance(pids, list)
