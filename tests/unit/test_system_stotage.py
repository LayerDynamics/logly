"""
Unit tests for logly.utils.system_storage module  
Tests filesystem storage checking
"""

import pytest
from unittest.mock import patch, Mock
from logly.utils.system_storage import (
    get_storage_info, format_bytes, get_storage_summary,
    check_storage_warning, check_storage_critical,
    get_free_space_mb, get_free_space_gb
)


class TestSystemStorage:
    """Test suite for system_storage utility"""

    @pytest.mark.unit
    @patch('os.statvfs')
    def test_get_storage_info(self, mock_statvfs):
        """Test getting storage information"""
        mock_stat = Mock()
        mock_stat.f_blocks = 100000
        mock_stat.f_frsize = 4096
        mock_stat.f_bavail = 50000
        mock_statvfs.return_value = mock_stat
        
        info = get_storage_info("/")
        
        assert "total_bytes" in info
        assert "free_bytes" in info
        assert "used_bytes" in info
        assert "percent_used" in info
        assert info["total_bytes"] == 100000 * 4096

    @pytest.mark.unit
    def test_format_bytes_b(self):
        """Test formatting bytes"""
        assert format_bytes(512) == "512 B"

    @pytest.mark.unit
    def test_format_bytes_kb(self):
        """Test formatting kilobytes"""
        assert "KB" in format_bytes(2048)

    @pytest.mark.unit
    def test_format_bytes_mb(self):
        """Test formatting megabytes"""
        assert "MB" in format_bytes(5 * 1024 * 1024)

    @pytest.mark.unit
    @patch('logly.utils.system_storage.get_storage_info')
    def test_check_storage_warning_above_threshold(self, mock_get_info):
        """Test warning check when above threshold"""
        mock_get_info.return_value = {"percent_used": 95.0}
        
        assert check_storage_warning("/", threshold_percent=90.0) is True

    @pytest.mark.unit
    @patch('logly.utils.system_storage.get_storage_info')
    def test_check_storage_warning_below_threshold(self, mock_get_info):
        """Test warning check when below threshold"""
        mock_get_info.return_value = {"percent_used": 85.0}
        
        assert check_storage_warning("/", threshold_percent=90.0) is False

    @pytest.mark.unit
    @patch('logly.utils.system_storage.get_storage_info')
    def test_get_free_space_mb(self, mock_get_info):
        """Test getting free space in MB"""
        mock_get_info.return_value = {"free_bytes": 10 * 1024 * 1024}
        
        free_mb = get_free_space_mb("/")
        assert free_mb == 10.0

    @pytest.mark.unit
    @patch('logly.utils.system_storage.get_storage_info')
    def test_get_free_space_gb(self, mock_get_info):
        """Test getting free space in GB"""
        mock_get_info.return_value = {"free_bytes": 5 * 1024 * 1024 * 1024}
        
        free_gb = get_free_space_gb("/")
        assert free_gb == 5.0
