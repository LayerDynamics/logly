"""
Unit tests for logly.exporters.report_generator module
Tests summary report generation functionality
"""

import pytest
from unittest.mock import Mock, patch, mock_open
from datetime import datetime

from logly.exporters.report_generator import ReportGenerator


class TestReportGenerator:
    """Test suite for ReportGenerator class"""
    
    @pytest.mark.unit
    def test_init(self, test_store):
        """Test ReportGenerator initialization"""
        generator = ReportGenerator(test_store)
        
        assert generator.store == test_store
    
    @pytest.mark.unit
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_summary_report(self, mock_file, test_store):
        """Test generating a summary report"""
        # Mock store methods
        test_store.get_system_metrics = Mock(return_value=[
            {'cpu_percent': 40.0, 'memory_percent': 50.0, 'disk_percent': 60.0},
            {'cpu_percent': 50.0, 'memory_percent': 60.0, 'disk_percent': 65.0},
            {'cpu_percent': 45.0, 'memory_percent': 55.0, 'disk_percent': 62.0}
        ])
        
        test_store.get_network_metrics = Mock(return_value=[
            {'bytes_sent': 1000, 'bytes_recv': 2000, 'packets_sent': 10, 'packets_recv': 20},
            {'bytes_sent': 2000, 'bytes_recv': 3000, 'packets_sent': 20, 'packets_recv': 30},
            {'bytes_sent': 3000, 'bytes_recv': 5000, 'packets_sent': 30, 'packets_recv': 50}
        ])
        
        test_store.get_log_events = Mock(return_value=[
            {'action': 'failed_login'},
            {'action': 'failed_login'},
            {'action': 'ban'},
            {'level': 'ERROR'},
            {'level': 'WARNING'}
        ])
        
        test_store.get_stats = Mock(return_value={
            'system_metrics': 100,
            'network_metrics': 100,
            'log_events': 50,
            'hourly_aggregates': 24,
            'daily_aggregates': 7,
            'database_size_mb': 15.5
        })
        
        generator = ReportGenerator(test_store)
        
        # Generate report
        generator.generate_summary_report("/tmp/report.txt", 1234567800, 1234568000)
        
        # Verify file was opened for writing
        mock_file.assert_called_once_with("/tmp/report.txt", 'w')
        
        # Verify write was called
        handle = mock_file()
        assert handle.write.called
    
    @pytest.mark.unit
    def test_compute_statistics_system_metrics(self, test_store):
        """Test computing system metrics statistics"""
        test_store.get_system_metrics = Mock(return_value=[
            {'cpu_percent': 40.0, 'memory_percent': 50.0, 'disk_percent': 60.0},
            {'cpu_percent': 50.0, 'memory_percent': 60.0, 'disk_percent': 65.0},
            {'cpu_percent': 60.0, 'memory_percent': 70.0, 'disk_percent': 70.0}
        ])
        
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[])
        
        generator = ReportGenerator(test_store)
        stats = generator._compute_statistics(1234567800, 1234568000)
        
        assert stats['system'] is not None
        assert stats['system']['avg_cpu'] == 50.0  # (40+50+60)/3
        assert stats['system']['max_cpu'] == 60.0
        assert stats['system']['avg_memory'] == 60.0  # (50+60+70)/3
        assert stats['system']['max_memory'] == 70.0
        assert stats['system']['avg_disk'] == 65.0  # (60+65+70)/3
    
    @pytest.mark.unit
    def test_compute_statistics_no_system_metrics(self, test_store):
        """Test computing statistics with no system metrics"""
        test_store.get_system_metrics = Mock(return_value=[])
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[])
        
        generator = ReportGenerator(test_store)
        stats = generator._compute_statistics(1234567800, 1234568000)
        
        assert stats['system'] is None
        assert stats['network'] is None
        assert stats['logs'] is None
    
    @pytest.mark.unit
    def test_compute_statistics_network_metrics(self, test_store):
        """Test computing network metrics statistics"""
        test_store.get_system_metrics = Mock(return_value=[])
        test_store.get_network_metrics = Mock(return_value=[
            {'bytes_sent': 3000, 'bytes_recv': 5000, 'packets_sent': 30, 'packets_recv': 50},
            {'bytes_sent': 2000, 'bytes_recv': 3000, 'packets_sent': 20, 'packets_recv': 30},
            {'bytes_sent': 1000, 'bytes_recv': 2000, 'packets_sent': 10, 'packets_recv': 20}
        ])
        test_store.get_log_events = Mock(return_value=[])
        
        generator = ReportGenerator(test_store)
        stats = generator._compute_statistics(1234567800, 1234568000)
        
        assert stats['network'] is not None
        # Should calculate delta between first and last
        assert stats['network']['total_sent'] == 2000  # 3000 - 1000
        assert stats['network']['total_recv'] == 3000  # 5000 - 2000
        assert stats['network']['total_packets_sent'] == 20  # 30 - 10
        assert stats['network']['total_packets_recv'] == 30  # 50 - 20
    
    @pytest.mark.unit
    def test_compute_statistics_log_events(self, test_store):
        """Test computing log event statistics"""
        test_store.get_system_metrics = Mock(return_value=[])
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[
            {'action': 'failed_login'},
            {'action': 'failed_login'},
            {'action': 'failed_login'},
            {'action': 'ban'},
            {'action': 'ban'},
            {'level': 'ERROR'},
            {'level': 'ERROR'},
            {'level': 'WARNING'},
            {'level': 'WARNING'},
            {'level': 'WARNING'}
        ])
        
        generator = ReportGenerator(test_store)
        stats = generator._compute_statistics(1234567800, 1234568000)
        
        assert stats['logs'] is not None
        assert stats['logs']['total'] == 10
        assert stats['logs']['failed_logins'] == 3
        assert stats['logs']['banned_ips'] == 2
        assert stats['logs']['errors'] == 2
        assert stats['logs']['warnings'] == 3
    
    @pytest.mark.unit
    def test_format_bytes(self, test_store):
        """Test _format_bytes method"""
        generator = ReportGenerator(test_store)
        
        assert generator._format_bytes(0) == "0 B"
        assert generator._format_bytes(512) == "512 B"
        assert generator._format_bytes(1024) == "1.00 KB"
        assert generator._format_bytes(1024 * 1024) == "1.00 MB"
        assert generator._format_bytes(1024 * 1024 * 1024) == "1.00 GB"
        assert generator._format_bytes(1024 * 1024 * 1024 * 1024) == "1.00 TB"
        assert generator._format_bytes(1536) == "1.50 KB"
        assert generator._format_bytes(1536 * 1024) == "1.50 MB"
    
    @pytest.mark.unit
    @patch('builtins.open', new_callable=mock_open)
    @patch('logly.exporters.report_generator.datetime')
    def test_report_format(self, mock_datetime, mock_file, test_store):
        """Test the format of the generated report"""
        # Mock datetime
        mock_datetime.fromtimestamp.return_value = datetime(2025, 1, 15, 12, 0, 0)
        
        # Setup minimal mocks
        test_store.get_system_metrics = Mock(return_value=[
            {'cpu_percent': 45.0, 'memory_percent': 50.0, 'disk_percent': 60.0}
        ])
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[])
        test_store.get_stats = Mock(return_value={
            'system_metrics': 100,
            'network_metrics': 0,
            'log_events': 0,
            'hourly_aggregates': 0,
            'daily_aggregates': 0,
            'database_size_mb': 10.0
        })
        
        generator = ReportGenerator(test_store)
        generator.generate_summary_report("/tmp/report.txt", 1234567800, 1234568000)
        
        # Get the written content
        handle = mock_file()
        written_content = handle.write.call_args[0][0]
        
        # Check report structure
        assert "=" * 70 in written_content
        assert "LOGLY SUMMARY REPORT" in written_content
        assert "Report Period:" in written_content
        assert "Duration:" in written_content
        assert "SYSTEM METRICS" in written_content
        assert "NETWORK METRICS" in written_content
        assert "LOG EVENTS" in written_content
        assert "DATABASE STATISTICS" in written_content
    
    @pytest.mark.unit
    def test_actual_report_generation(self, test_store, temp_dir):
        """Test actual report file generation"""
        # Setup mock data
        test_store.get_system_metrics = Mock(return_value=[
            {'cpu_percent': 45.0, 'memory_percent': 50.0, 'disk_percent': 60.0}
        ])
        test_store.get_network_metrics = Mock(return_value=[
            {'bytes_sent': 1000000, 'bytes_recv': 2000000, 
             'packets_sent': 1000, 'packets_recv': 2000},
            {'bytes_sent': 2000000, 'bytes_recv': 4000000,
             'packets_sent': 2000, 'packets_recv': 4000}
        ])
        test_store.get_log_events = Mock(return_value=[
            {'action': 'failed_login', 'level': 'WARNING'},
            {'action': 'ban', 'level': 'WARNING'},
            {'level': 'ERROR'}
        ])
        test_store.get_stats = Mock(return_value={
            'system_metrics': 100,
            'network_metrics': 100,
            'log_events': 50,
            'hourly_aggregates': 24,
            'daily_aggregates': 7,
            'database_size_mb': 15.5
        })
        
        generator = ReportGenerator(test_store)
        output_path = temp_dir / "test_report.txt"
        
        generator.generate_summary_report(str(output_path), 1234567800, 1234568000)
        
        # Verify file was created and contains expected content
        assert output_path.exists()
        
        content = output_path.read_text()
        assert "LOGLY SUMMARY REPORT" in content
        assert "CPU Usage (avg)" in content
        assert "45.0%" in content  # CPU average
        assert "Bytes Sent (total)" in content
        assert "Failed Logins" in content
        assert "Database Size" in content
        assert "15.50 MB" in content
    
    @pytest.mark.unit
    def test_report_with_no_data(self, test_store, temp_dir):
        """Test report generation with no data"""
        test_store.get_system_metrics = Mock(return_value=[])
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[])
        test_store.get_stats = Mock(return_value={
            'system_metrics': 0,
            'network_metrics': 0,
            'log_events': 0,
            'hourly_aggregates': 0,
            'daily_aggregates': 0,
            'database_size_mb': 0.0
        })
        
        generator = ReportGenerator(test_store)
        output_path = temp_dir / "empty_report.txt"
        
        generator.generate_summary_report(str(output_path), 1234567800, 1234568000)
        
        assert output_path.exists()
        
        content = output_path.read_text()
        assert "No system metrics found" in content
        assert "No network metrics found" in content
        assert "No log events found" in content
    
    @pytest.mark.unit
    def test_duration_calculation(self, test_store, temp_dir):
        """Test that duration is calculated correctly"""
        # Setup minimal mocks
        test_store.get_system_metrics = Mock(return_value=[])
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[])
        test_store.get_stats = Mock(return_value={
            'system_metrics': 0,
            'network_metrics': 0,
            'log_events': 0,
            'hourly_aggregates': 0,
            'daily_aggregates': 0,
            'database_size_mb': 0.0
        })
        
        generator = ReportGenerator(test_store)
        output_path = temp_dir / "duration_test.txt"
        
        # 24 hours = 86400 seconds
        start_time = 1234567800
        end_time = 1234654200  # 24 hours later
        
        generator.generate_summary_report(str(output_path), start_time, end_time)
        
        content = output_path.read_text()
        assert "Duration: 24.0 hours" in content
    
    @pytest.mark.unit
    def test_error_handling(self, test_store):
        """Test error handling in report generation"""
        # Make file writing fail
        with patch('builtins.open', side_effect=IOError("Permission denied")):
            generator = ReportGenerator(test_store)
            
            test_store.get_system_metrics = Mock(return_value=[])
            test_store.get_network_metrics = Mock(return_value=[])
            test_store.get_log_events = Mock(return_value=[])
            test_store.get_stats = Mock(return_value={})
            
            with pytest.raises(IOError):
                generator.generate_summary_report("/tmp/report.txt", 0, 100)
    
    @pytest.mark.unit
    def test_empty_metrics_handling(self, test_store):
        """Test handling of metrics with None values"""
        test_store.get_system_metrics = Mock(return_value=[
            {'cpu_percent': None, 'memory_percent': 50.0, 'disk_percent': None},
            {'cpu_percent': 45.0, 'memory_percent': None, 'disk_percent': 60.0}
        ])
        test_store.get_network_metrics = Mock(return_value=[])
        test_store.get_log_events = Mock(return_value=[])
        
        generator = ReportGenerator(test_store)
        stats = generator._compute_statistics(0, 100)
        
        # Should handle None values gracefully
        assert stats['system']['avg_cpu'] == 45.0  # Only one valid value
        assert stats['system']['max_cpu'] == 45.0
        assert stats['system']['avg_memory'] == 50.0  # Only one valid value
        assert stats['system']['max_memory'] == 50.0
        assert stats['system']['avg_disk'] == 60.0  # Only one valid value