"""
Unit tests for logly.collectors.network_monitor module
Tests network metrics collection from /proc/net
"""

import pytest
from unittest.mock import patch, mock_open

from logly.collectors.network_monitor import NetworkMonitor
from logly.storage.models import NetworkMetric


class TestNetworkMonitor:
    """Test suite for NetworkMonitor class"""

    @pytest.mark.unit
    def test_init(self):
        """Test NetworkMonitor initialization"""
        config = {
            "enabled": True,
            "metrics": ["bytes_sent", "bytes_recv", "connections"],
        }

        monitor = NetworkMonitor(config)

        assert monitor.config == config
        assert monitor.enabled
        assert monitor.metrics_to_collect == ["bytes_sent", "bytes_recv", "connections"]
        assert monitor._last_net_io is None

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_get_network_io_stats(self, mock_file):
        """Test _get_network_io_stats method"""
        # Mock /proc/net/dev content
        net_dev_content = """Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000000  1000    0    0    0     0          0         0  1000000  1000    0    0    0     0       0          0
  eth0: 2000000  2000    5    3    0     0          0         0  1500000  1500    2    1    0     0       0          0
  eth1:  500000   500    1    0    0     0          0         0   250000   250    0    0    0     0       0          0
"""
        mock_file.return_value.read.return_value = net_dev_content
        mock_file.return_value.__iter__.return_value = net_dev_content.splitlines()
        mock_file.return_value.readlines.return_value = net_dev_content.splitlines()

        config = {"metrics": ["bytes_sent", "bytes_recv"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_network_io_stats()

        # Should sum all interfaces except loopback
        assert stats["bytes_recv"] == 2000000 + 500000  # eth0 + eth1
        assert stats["bytes_sent"] == 1500000 + 250000
        assert stats["packets_recv"] == 2000 + 500
        assert stats["packets_sent"] == 1500 + 250
        assert stats["errors_in"] == 5 + 1
        assert stats["errors_out"] == 2 + 0
        assert stats["drops_in"] == 3 + 0
        assert stats["drops_out"] == 1 + 0

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_get_network_io_stats_skip_loopback(self, mock_file):
        """Test that loopback interface is skipped"""
        net_dev_content = """Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 9999999  9999    0    0    0     0          0         0  9999999  9999    0    0    0     0       0          0
  eth0: 1000000  1000    0    0    0     0          0         0  1000000  1000    0    0    0     0       0          0
"""
        mock_file.return_value.read.return_value = net_dev_content
        mock_file.return_value.__iter__.return_value = net_dev_content.splitlines()
        mock_file.return_value.readlines.return_value = net_dev_content.splitlines()

        config = {"metrics": ["bytes_sent", "bytes_recv"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_network_io_stats()

        # Should only count eth0, not lo
        assert stats["bytes_recv"] == 1000000
        assert stats["bytes_sent"] == 1000000

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open")
    def test_get_network_io_stats_error_handling(self, mock_file, caplog):
        """Test error handling in _get_network_io_stats"""
        mock_file.side_effect = IOError("File not found")

        config = {"metrics": ["bytes_sent", "bytes_recv"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_network_io_stats()

        assert stats["bytes_recv"] == 0
        assert stats["bytes_sent"] == 0
        assert stats["packets_recv"] == 0
        assert stats["packets_sent"] == 0
        assert "Error reading network I/O stats" in caplog.text

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    @patch("logly.collectors.network_monitor.Path")
    def test_get_connection_stats(self, mock_path, mock_file):
        """Test _get_connection_stats method"""
        # Mock /proc/net/tcp content
        tcp_content = """  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:0050 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0
   1: 0100007F:0051 0100007F:9999 01 00000000:00000000 00:00000000 00000000     0        0 12346 1 0000000000000000 100 0 0 10 0
   2: 00000000:0080 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12347 1 0000000000000000 100 0 0 10 0
   3: C0A80101:8080 C0A80102:1234 06 00000000:00000000 00:00000000 00000000     0        0 12348 1 0000000000000000 100 0 0 10 0
"""

        # Mock path existence checks - only TCP exists, not TCP6
        def path_exists():
            # Access the path through the mock's parent
            path_str = str(mock_path.call_args[0][0]) if mock_path.call_args else ""
            return "/tcp" in path_str and "/tcp6" not in path_str

        mock_path.return_value.exists.side_effect = path_exists

        # Setup file reading
        mock_file.return_value.read.return_value = tcp_content
        mock_file.return_value.__iter__.return_value = tcp_content.splitlines()
        mock_file.return_value.readlines.return_value = tcp_content.splitlines()

        config = {"metrics": ["connections"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_connection_stats()

        # State codes: 01=ESTABLISHED, 0A=LISTEN, 06=TIME_WAIT
        assert stats["established"] == 1  # Line 1
        assert stats["listen"] == 2  # Lines 0 and 2
        assert stats["time_wait"] == 1  # Line 3
        assert stats["other"] == 0

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    @patch("logly.collectors.network_monitor.Path")
    def test_get_connection_stats_ipv6(self, mock_path, mock_file):
        """Test _get_connection_stats with IPv6"""
        # Mock /proc/net/tcp6 content
        tcp6_content = """  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000000000001:0050 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0
   1: 00000000000000000000000000000001:0051 00000000000000000000000000000001:9999 01 00000000:00000000 00:00000000 00000000     0        0 12346 1 0000000000000000 100 0 0 10 0
"""

        # Mock path existence - both tcp and tcp6 exist
        def path_exists():
            path_str = str(mock_path.call_args[0][0]) if mock_path.call_args else ""
            return "/tcp" in path_str

        mock_path.return_value.exists.side_effect = path_exists

        # Setup different content for tcp and tcp6
        def open_side_effect(path, mode="r"):
            if "tcp6" in str(path):
                m = mock_open(read_data=tcp6_content)()
                m.__iter__.return_value = tcp6_content.splitlines()
                m.readlines.return_value = tcp6_content.splitlines()
                return m
            else:
                # Regular TCP with no entries
                tcp_content = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
                m = mock_open(read_data=tcp_content)()
                m.__iter__.return_value = tcp_content.splitlines()
                m.readlines.return_value = tcp_content.splitlines()
                return m

        mock_file.side_effect = open_side_effect

        config = {"metrics": ["connections"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_connection_stats()

        # IPv6: 0A=LISTEN, 01=ESTABLISHED
        assert stats["listen"] == 1  # Line 0 of tcp6
        assert stats["established"] == 1  # Line 1 of tcp6

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("logly.collectors.network_monitor.Path")
    def test_get_connection_stats_no_files(self, mock_path):
        """Test _get_connection_stats when /proc/net/tcp doesn't exist"""
        mock_path.return_value.exists.return_value = False

        config = {"metrics": ["connections"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_connection_stats()

        assert stats["established"] == 0
        assert stats["listen"] == 0
        assert stats["time_wait"] == 0
        assert stats["other"] == 0

    @pytest.mark.unit
    @patch.object(NetworkMonitor, "_get_network_io_stats")
    @patch.object(NetworkMonitor, "_get_connection_stats")
    @patch("time.time", return_value=1234567890)
    def test_collect(self, mock_time, mock_conn_stats, mock_io_stats):
        """Test collect method"""
        # Setup mocks
        mock_io_stats.return_value = {
            "bytes_sent": 1000000,
            "bytes_recv": 2000000,
            "packets_sent": 1000,
            "packets_recv": 2000,
            "errors_in": 5,
            "errors_out": 2,
            "drops_in": 3,
            "drops_out": 1,
        }

        mock_conn_stats.return_value = {
            "established": 10,
            "listen": 5,
            "time_wait": 2,
            "other": 0,
        }

        config = {
            "metrics": [
                "bytes_sent",
                "bytes_recv",
                "packets_sent",
                "packets_recv",
                "connections",
                "listening_ports",
            ]
        }

        monitor = NetworkMonitor(config)
        metric = monitor.collect()

        assert isinstance(metric, NetworkMetric)
        assert metric.timestamp == 1234567890
        assert metric.bytes_sent == 1000000
        assert metric.bytes_recv == 2000000
        assert metric.packets_sent == 1000
        assert metric.packets_recv == 2000
        assert metric.errors_in == 5
        assert metric.errors_out == 2
        assert metric.drops_in == 3
        assert metric.drops_out == 1
        assert metric.connections_established == 10
        assert metric.connections_listen == 5
        assert metric.connections_time_wait == 2

    @pytest.mark.unit
    def test_collect_partial_metrics(self):
        """Test collecting only specific metrics"""
        config = {"metrics": ["bytes_sent", "bytes_recv"]}

        monitor = NetworkMonitor(config)

        with patch.object(
            monitor,
            "_get_network_io_stats",
            return_value={
                "bytes_sent": 1000,
                "bytes_recv": 2000,
                "packets_sent": 10,
                "packets_recv": 20,
                "errors_in": 0,
                "errors_out": 0,
                "drops_in": 0,
                "drops_out": 0,
            },
        ):
            metric = monitor.collect()

            assert metric.bytes_sent == 1000
            assert metric.bytes_recv == 2000
            assert metric.connections_established is None  # Not collected

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("logly.collectors.network_monitor.Path")
    def test_validate(self, mock_path):
        """Test validate method"""
        # Mock Path.exists() for /proc/net/dev
        mock_path.return_value.exists.return_value = True

        config = {"metrics": []}
        monitor = NetworkMonitor(config)

        assert monitor.validate()

        # Test when file doesn't exist
        mock_path.return_value.exists.return_value = False
        assert not monitor.validate()

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    def test_malformed_net_dev(self, mock_file):
        """Test handling of malformed /proc/net/dev content"""
        # Malformed content with missing fields
        net_dev_content = """Inter-|   Receive
 face |bytes    packets
  eth0: 1000
  eth1: malformed line
"""
        mock_file.return_value.read.return_value = net_dev_content
        mock_file.return_value.__iter__.return_value = net_dev_content.splitlines()
        mock_file.return_value.readlines.return_value = net_dev_content.splitlines()

        config = {"metrics": ["bytes_sent", "bytes_recv"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_network_io_stats()

        # Should handle gracefully and return zeros
        assert stats["bytes_recv"] == 0
        assert stats["bytes_sent"] == 0

    @pytest.mark.unit
    @patch("logly.collectors.network_monitor.IS_LINUX", True)
    @patch("logly.collectors.network_monitor.IS_MACOS", False)
    @patch("builtins.open", new_callable=mock_open)
    @patch("logly.collectors.network_monitor.Path")
    def test_malformed_tcp_stats(self, mock_path, mock_file):
        """Test handling of malformed /proc/net/tcp content"""
        # Malformed TCP content
        tcp_content = """  sl  local_address rem_address   st
   0: invalid line
   1: 0100007F:0050 00000000:0000 XX
"""

        mock_path.return_value.exists.return_value = True
        mock_file.return_value.read.return_value = tcp_content
        mock_file.return_value.__iter__.return_value = tcp_content.splitlines()

        config = {"metrics": ["connections"]}
        monitor = NetworkMonitor(config)

        stats = monitor._get_connection_stats()

        # Should handle gracefully
        assert stats["established"] == 0
        assert stats["listen"] == 0
        assert stats["other"] == 0  # Unknown state 'XX' goes to other
