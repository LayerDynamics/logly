"""
Unit tests for logly.utils.logger module
Tests daily rotating logger functionality
"""

import pytest
from unittest.mock import patch, Mock
from pathlib import Path
from logly.utils.logger import get_logger, initialize_logging, get_current_log_file


class TestLogger:
    """Test suite for logger utility"""

    @pytest.mark.unit
    @patch('logly.utils.logger.get_logs_dir')
    def test_get_logger(self, mock_get_logs_dir, temp_log_dir):
        """Test getting a logger instance"""
        mock_get_logs_dir.return_value = temp_log_dir
        
        logger = get_logger("test_module")
        
        assert logger is not None
        assert "test_module" in logger.name

    @pytest.mark.unit
    @patch('logly.utils.logger.get_logs_dir')
    def test_initialize_logging(self, mock_get_logs_dir, temp_log_dir):
        """Test initializing logging system"""
        mock_get_logs_dir.return_value = temp_log_dir
        
        initialize_logging()
        
        # Should not raise any exceptions
        assert True

    @pytest.mark.unit
    @patch('logly.utils.logger.get_logs_dir')
    def test_get_current_log_file(self, mock_get_logs_dir, temp_log_dir):
        """Test getting current log file path"""
        mock_get_logs_dir.return_value = temp_log_dir
        
        initialize_logging()
        log_file = get_current_log_file()
        
        assert log_file is not None
        assert isinstance(log_file, Path)

    @pytest.mark.unit
    @patch('logly.utils.logger.get_logs_dir')
    def test_logger_writes_to_file(self, mock_get_logs_dir, temp_log_dir):
        """Test that logger writes to file"""
        mock_get_logs_dir.return_value = temp_log_dir

        # Reset the global logger instance to force reinitialization
        import logly.utils.logger
        logly.utils.logger._daily_logger = None

        # Initialize logging with the mocked directory
        initialize_logging()
        logger = get_logger("test")
        logger.info("Test message")

        # Force flush the handler
        import logging
        for handler in logging.getLogger("logly").handlers:
            handler.flush()

        # Check that log file was created
        log_files = list(temp_log_dir.glob("*.log"))
        assert len(log_files) > 0
