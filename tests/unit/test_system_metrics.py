"""
Unit tests for logly.collectors.system_metrics module
Tests system metrics collection from /proc filesystem
"""

import pytest
from unittest.mock import Mock, patch, mock_open
from pathlib import Path
import os

from logly.collectors.system_metrics import SystemMetricsCollector
from logly.storage.models import SystemMetric


class TestSystemMetricsCollector:
    """Test suite for SystemMetricsCollector class"""

    @pytest.mark.unit
    def test_init(self):
        """Test SystemMetricsCollector initialization"""
        config = {"enabled": True, "metrics": ["cpu_percent", "memory_percent"]}

        collector = SystemMetricsCollector(config)

        assert collector.config == config
        assert collector.enabled
        assert collector.metrics_to_collect == ["cpu_percent", "memory_percent"]
        assert collector._last_cpu_stats is None
        assert collector._last_disk_io is None

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.cpu_count", return_value=4)
    def test_get_cpu_stats(self, mock_cpu_count, mock_file):
        """Test _get_cpu_stats method"""
        # Mock /proc/stat content
        mock_file.return_value.readline.return_value = (
            "cpu  100 200 300 400 500 600 700 0 0 0\n"
        )

        config = {"metrics": ["cpu_percent", "cpu_count"]}
        collector = SystemMetricsCollector(config)

        # First call - no previous stats
        cpu_percent, cpu_count = collector._get_cpu_stats()

        assert cpu_percent is None  # No previous stats to compare
        assert cpu_count == 4
        assert collector._last_cpu_stats == (400, 2800)  # idle=400, total=2800

        # Second call - calculate percentage
        mock_file.return_value.readline.return_value = (
            "cpu  150 250 350 500 550 650 750 0 0 0\n"
        )
        cpu_percent, cpu_count = collector._get_cpu_stats()

        # CPU usage = (total_diff - idle_diff) / total_diff
        # total_diff = 3200 - 2800 = 400
        # idle_diff = 500 - 400 = 100
        # cpu_percent = (400 - 100) / 400 * 100 = 75.0
        assert cpu_percent is not None
        assert cpu_count == 4

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_get_cpu_stats_error_handling(self, mock_file, caplog):
        """Test error handling in _get_cpu_stats"""
        mock_file.side_effect = IOError("File not found")

        config = {"metrics": ["cpu_percent"]}
        collector = SystemMetricsCollector(config)

        cpu_percent, cpu_count = collector._get_cpu_stats()

        assert cpu_percent is None
        assert cpu_count == 0
        assert "Error reading CPU stats" in caplog.text

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_get_memory_stats(self, mock_file):
        """Test _get_memory_stats method"""
        # Mock /proc/meminfo content
        meminfo_content = """MemTotal:        8388608 kB
MemFree:         2097152 kB
MemAvailable:    4194304 kB
Buffers:          524288 kB
Cached:          1048576 kB
"""
        mock_file.return_value.read.return_value = meminfo_content
        mock_file.return_value.__iter__.return_value = meminfo_content.splitlines()
        mock_file.return_value.readlines.return_value = meminfo_content.splitlines()

        config = {"metrics": ["memory_total", "memory_available", "memory_percent"]}
        collector = SystemMetricsCollector(config)

        mem_stats = collector._get_memory_stats()

        assert mem_stats["total"] == 8388608 * 1024  # Convert KB to bytes
        assert mem_stats["available"] == 4194304 * 1024
        assert mem_stats["percent"] == pytest.approx(50.0, 0.1)

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_get_memory_stats_no_memavailable(self, mock_file):
        """Test _get_memory_stats when MemAvailable is not present"""
        # Older kernels don't have MemAvailable
        meminfo_content = """MemTotal:        8388608 kB
MemFree:         2097152 kB
Buffers:          524288 kB
Cached:          1048576 kB
"""
        mock_file.return_value.read.return_value = meminfo_content
        mock_file.return_value.__iter__.return_value = meminfo_content.splitlines()
        mock_file.return_value.readlines.return_value = meminfo_content.splitlines()

        config = {"metrics": ["memory_total", "memory_available"]}
        collector = SystemMetricsCollector(config)

        mem_stats = collector._get_memory_stats()

        # Should estimate available as free + buffers + cached
        expected_available = (2097152 + 524288 + 1048576) * 1024
        assert mem_stats["available"] == expected_available

    @pytest.mark.unit
    @patch("os.statvfs")
    def test_get_disk_stats(self, mock_statvfs):
        """Test _get_disk_stats method"""
        # Mock statvfs result
        mock_stat = Mock()
        mock_stat.f_blocks = 100000  # Total blocks
        mock_stat.f_frsize = 4096  # Block size
        mock_stat.f_bavail = 50000  # Available blocks
        mock_statvfs.return_value = mock_stat

        config = {"metrics": ["disk_usage"]}
        collector = SystemMetricsCollector(config)

        disk_stats = collector._get_disk_stats("/")

        total = 100000 * 4096
        free = 50000 * 4096
        used = total - free

        assert disk_stats["total"] == total
        assert disk_stats["used"] == used
        assert disk_stats["percent"] == 50.0

    @pytest.mark.unit
    @patch("os.statvfs")
    def test_get_disk_stats_error_handling(self, mock_statvfs, caplog):
        """Test error handling in _get_disk_stats"""
        mock_statvfs.side_effect = OSError("Permission denied")

        config = {"metrics": ["disk_usage"]}
        collector = SystemMetricsCollector(config)

        disk_stats = collector._get_disk_stats("/")

        assert disk_stats["total"] == 0
        assert disk_stats["used"] == 0
        assert disk_stats["percent"] == 0.0
        assert "Error reading disk stats" in caplog.text

    @pytest.mark.unit
    @patch("builtins.open", new_callable=mock_open)
    def test_get_disk_io_stats(self, mock_file):
        """Test _get_disk_io_stats method"""
        # Mock /proc/diskstats content
        diskstats_content = """   8       0 sda 1000 0 8000 100 2000 0 16000 200 0 100 300
   8       1 sda1 100 0 800 10 200 0 1600 20 0 10 30
   8      16 sdb 500 0 4000 50 1000 0 8000 100 0 50 150"""

        mock_file.return_value.read.return_value = diskstats_content
        mock_file.return_value.__iter__.return_value = diskstats_content.splitlines()
        mock_file.return_value.readlines.return_value = diskstats_content.splitlines()

        config = {"metrics": ["disk_io"]}
        collector = SystemMetricsCollector(config)

        io_stats = collector._get_disk_io_stats()

        # Should sum up whole disks (sda, sdb) but not partitions (sda1)
        # read_sectors = 8000 + 4000 = 12000
        # write_sectors = 16000 + 8000 = 24000
        assert io_stats["read_bytes"] == 12000 * 512
        assert io_stats["write_bytes"] == 24000 * 512

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_get_load_average(self, mock_file):
        """Test _get_load_average method"""
        mock_file.return_value.readline.return_value = "1.50 2.00 1.80 2/150 1234\n"

        config = {"metrics": ["load_average"]}
        collector = SystemMetricsCollector(config)

        load = collector._get_load_average()

        assert load == (1.50, 2.00, 1.80)

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("builtins.open")
    def test_get_load_average_error_handling(self, mock_file, caplog):
        """Test error handling in _get_load_average"""
        mock_file.side_effect = IOError("File not found")

        config = {"metrics": ["load_average"]}
        collector = SystemMetricsCollector(config)

        load = collector._get_load_average()

        assert load == (0.0, 0.0, 0.0)
        assert "Error reading load average" in caplog.text

    @pytest.mark.unit
    @patch.object(SystemMetricsCollector, "_get_cpu_stats")
    @patch.object(SystemMetricsCollector, "_get_memory_stats")
    @patch.object(SystemMetricsCollector, "_get_disk_stats")
    @patch.object(SystemMetricsCollector, "_get_disk_io_stats")
    @patch.object(SystemMetricsCollector, "_get_load_average")
    @patch("time.time", return_value=1234567890)
    def test_collect(
        self, mock_time, mock_load, mock_disk_io, mock_disk, mock_memory, mock_cpu
    ):
        """Test collect method"""
        # Setup mocks
        mock_cpu.return_value = (45.5, 4)
        mock_memory.return_value = {
            "total": 8589934592,
            "available": 4294967296,
            "percent": 50.0,
        }
        mock_disk.return_value = {
            "total": 107374182400,
            "used": 53687091200,
            "percent": 50.0,
        }
        mock_disk_io.return_value = {"read_bytes": 1000000, "write_bytes": 2000000}
        mock_load.return_value = (1.5, 2.0, 1.8)

        config = {
            "metrics": [
                "cpu_percent",
                "cpu_count",
                "memory_total",
                "memory_available",
                "memory_percent",
                "disk_usage",
                "disk_io",
                "load_average",
            ]
        }

        collector = SystemMetricsCollector(config)
        metric = collector.collect()

        assert isinstance(metric, SystemMetric)
        assert metric.timestamp == 1234567890
        assert metric.cpu_percent == 45.5
        assert metric.cpu_count == 4
        assert metric.memory_total == 8589934592
        assert metric.memory_available == 4294967296
        assert metric.memory_percent == 50.0
        assert metric.disk_total == 107374182400
        assert metric.disk_used == 53687091200
        assert metric.disk_percent == 50.0
        assert metric.disk_read_bytes == 1000000
        assert metric.disk_write_bytes == 2000000
        assert metric.load_1min == 1.5
        assert metric.load_5min == 2.0
        assert metric.load_15min == 1.8

    @pytest.mark.unit
    def test_collect_partial_metrics(self):
        """Test collecting only specific metrics"""
        config = {"metrics": ["cpu_percent", "memory_percent"]}

        collector = SystemMetricsCollector(config)

        with patch.object(collector, "_get_cpu_stats", return_value=(45.5, 4)):
            with patch.object(
                collector,
                "_get_memory_stats",
                return_value={"total": 8000, "available": 4000, "percent": 50.0},
            ):
                metric = collector.collect()

                assert metric.cpu_percent == 45.5
                assert metric.memory_percent == 50.0
                assert metric.disk_total is None  # Not collected

    @pytest.mark.unit
    @patch("logly.collectors.system_metrics.IS_LINUX", True)
    @patch("logly.collectors.system_metrics.IS_MACOS", False)
    @patch("logly.collectors.system_metrics.Path")
    def test_validate(self, mock_path):
        """Test validate method"""
        # Mock Path.exists() for /proc/stat and /proc/meminfo
        mock_path.return_value.exists.return_value = True

        config = {"metrics": []}
        collector = SystemMetricsCollector(config)

        assert collector.validate()

        # Test when files don't exist
        mock_path.return_value.exists.return_value = False
        assert not collector.validate()
