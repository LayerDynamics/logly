"""
Unit tests for logly.utils.db_size module
Tests database size calculation and formatting
"""

import pytest
from pathlib import Path
from logly.utils.db_size import get_db_size, format_size, get_db_info


class TestDBSize:
    """Test suite for db_size utility"""

    @pytest.mark.unit
    def test_get_db_size_existing_file(self, temp_db_path):
        """Test getting size of existing database file"""
        # Create a test file with some content
        Path(temp_db_path).write_text("x" * 1024)  # 1KB
        
        size_info = get_db_size(temp_db_path)
        
        assert size_info["exists"] is True
        assert size_info["size_bytes"] == 1024
        assert size_info["size_kb"] == 1.0
        assert size_info["size_mb"] == 0.0

    @pytest.mark.unit
    def test_get_db_size_nonexistent_file(self):
        """Test getting size of non-existent file"""
        size_info = get_db_size("/nonexistent/path/db.db")
        
        assert size_info["exists"] is False
        assert size_info["size_bytes"] == 0
        assert size_info["size_kb"] == 0.0
        assert size_info["size_mb"] == 0.0

    @pytest.mark.unit
    def test_format_size_bytes(self):
        """Test formatting bytes"""
        assert format_size(512) == "512 B"

    @pytest.mark.unit
    def test_format_size_kb(self):
        """Test formatting kilobytes"""
        assert "KB" in format_size(2048)

    @pytest.mark.unit
    def test_format_size_mb(self):
        """Test formatting megabytes"""
        assert "MB" in format_size(5 * 1024 * 1024)

    @pytest.mark.unit
    def test_format_size_gb(self):
        """Test formatting gigabytes"""
        assert "GB" in format_size(2 * 1024 * 1024 * 1024)

    @pytest.mark.unit
    def test_get_db_info(self, temp_db_path):
        """Test getting comprehensive database info"""
        Path(temp_db_path).write_text("x" * 2048)
        
        info = get_db_info(temp_db_path)
        
        assert "path" in info
        assert "exists" in info
        assert "size_bytes" in info
        assert "formatted_size" in info
        assert info["exists"] is True
