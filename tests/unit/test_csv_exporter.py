"""
Unit tests for logly.exporters.csv_exporter module
Tests CSV export functionality
"""

import pytest
import csv
from unittest.mock import Mock, patch, mock_open
from logly.exporters.csv_exporter import CSVExporter


class TestCSVExporter:
    """Test suite for CSVExporter class"""

    @pytest.mark.unit
    def test_init(self):
        """Test CSVExporter initialization"""
        mock_store = Mock()
        exporter = CSVExporter(mock_store, "%Y-%m-%d")
        
        assert exporter.store == mock_store
        assert exporter.timestamp_format == "%Y-%m-%d"

    @pytest.mark.unit
    @patch('builtins.open', new_callable=mock_open)
    def test_export_system_metrics(self, mock_file):
        """Test exporting system metrics"""
        mock_store = Mock()
        mock_store.get_system_metrics.return_value = [
            {"timestamp": 1000000, "cpu_percent": 45.5},
            {"timestamp": 1000060, "cpu_percent": 50.0}
        ]
        
        exporter = CSVExporter(mock_store)
        exporter.export_system_metrics("/tmp/out.csv", 1000000, 2000000)
        
        mock_store.get_system_metrics.assert_called_once_with(1000000, 2000000)
        mock_file.assert_called_once_with("/tmp/out.csv", "w", newline="")

    @pytest.mark.unit
    def test_export_system_metrics_empty(self, caplog):
        """Test exporting when no data found"""
        mock_store = Mock()
        mock_store.get_system_metrics.return_value = []
        
        exporter = CSVExporter(mock_store)
        exporter.export_system_metrics("/tmp/out.csv", 1000000, 2000000)
        
        assert "No system metrics found" in caplog.text

    @pytest.mark.unit
    @patch('builtins.open', new_callable=mock_open)
    def test_export_network_metrics(self, mock_file):
        """Test exporting network metrics"""
        mock_store = Mock()
        mock_store.get_network_metrics.return_value = [
            {"timestamp": 1000000, "bytes_sent": 1000}
        ]
        
        exporter = CSVExporter(mock_store)
        exporter.export_network_metrics("/tmp/out.csv", 1000000, 2000000)
        
        mock_store.get_network_metrics.assert_called_once()

    @pytest.mark.unit
    @patch('builtins.open', new_callable=mock_open)
    def test_export_log_events(self, mock_file):
        """Test exporting log events"""
        mock_store = Mock()
        mock_store.get_log_events.return_value = [
            {"timestamp": 1000000, "source": "test", "message": "Test"}
        ]
        
        exporter = CSVExporter(mock_store)
        exporter.export_log_events("/tmp/out.csv", 1000000, 2000000)
        
        mock_store.get_log_events.assert_called_once()
