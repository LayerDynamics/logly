"""
Pytest configuration and fixtures for Logly tests
Provides shared test fixtures and utilities for all test types
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import pytest
import time

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ============================================================================
# TEST MODE CONFIGURATION
# ============================================================================


@pytest.fixture(autouse=True, scope="session")
def enable_test_mode():
    """
    Automatically enable test mode for all tests
    This allows tests to use custom database paths instead of hardcoded paths
    """
    os.environ["LOGLY_TEST_MODE"] = "1"
    yield
    # Clean up after all tests
    if "LOGLY_TEST_MODE" in os.environ:
        del os.environ["LOGLY_TEST_MODE"]


# ============================================================================
# TEMPORARY DIRECTORY FIXTURES
# ============================================================================


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    temp_dir = tempfile.mkdtemp(prefix="logly_test_")
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_db_path(temp_dir):
    """Create a temporary database path"""
    db_path = temp_dir / "test.db"
    yield str(db_path)


@pytest.fixture
def temp_log_dir(temp_dir):
    """Create a temporary log directory"""
    log_dir = temp_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    yield log_dir


@pytest.fixture
def temp_config_file(temp_dir):
    """Create a temporary config file"""
    config_path = temp_dir / "test_config.yaml"
    config_content = """
database:
  path: "/tmp/test_logly.db"
  retention_days: 7

collection:
  system_metrics: 60
  network_metrics: 60
  log_parsing: 300

system:
  enabled: true
  metrics:
    - cpu_percent
    - memory_percent

network:
  enabled: true
  metrics:
    - bytes_sent
    - bytes_recv

logs:
  enabled: true
  sources:
    test_source:
      path: "/tmp/test.log"
      enabled: true
"""
    config_path.write_text(config_content)
    yield str(config_path)


# ============================================================================
# MOCK DATA FIXTURES
# ============================================================================


@pytest.fixture
def mock_system_metric():
    """Create a mock SystemMetric object"""
    from logly.storage.models import SystemMetric

    return SystemMetric(
        timestamp=int(time.time()),
        cpu_percent=45.5,
        cpu_count=4,
        memory_total=8589934592,  # 8GB
        memory_available=4294967296,  # 4GB
        memory_percent=50.0,
        disk_total=107374182400,  # 100GB
        disk_used=53687091200,  # 50GB
        disk_percent=50.0,
        disk_read_bytes=1000000,
        disk_write_bytes=2000000,
        load_1min=1.5,
        load_5min=2.0,
        load_15min=1.8,
    )


@pytest.fixture
def mock_network_metric():
    """Create a mock NetworkMetric object"""
    from logly.storage.models import NetworkMetric

    return NetworkMetric(
        timestamp=int(time.time()),
        bytes_sent=1000000,
        bytes_recv=2000000,
        packets_sent=1000,
        packets_recv=2000,
        errors_in=0,
        errors_out=0,
        drops_in=0,
        drops_out=0,
        connections_established=10,
        connections_listen=5,
        connections_time_wait=2,
    )


@pytest.fixture
def mock_log_event():
    """Create a mock LogEvent object"""
    from logly.storage.models import LogEvent

    return LogEvent(
        timestamp=int(time.time()),
        source="test_source",
        message="Test log message",
        level="INFO",
        ip_address="192.168.1.100",
        user="testuser",
        service="test_service",
        action="test_action",
        metadata={"key": "value"},
    )


@pytest.fixture
def mock_log_events():
    """Create multiple mock LogEvent objects"""
    from logly.storage.models import LogEvent

    base_time = int(time.time())
    events = []

    # Create various types of log events
    events.append(
        LogEvent(
            timestamp=base_time - 300,
            source="fail2ban",
            message="[sshd] Ban 192.168.1.100",
            level="WARNING",
            ip_address="192.168.1.100",
            service="sshd",
            action="ban",
        )
    )

    events.append(
        LogEvent(
            timestamp=base_time - 200,
            source="auth",
            message="Failed password for testuser from 192.168.1.101",
            level="WARNING",
            ip_address="192.168.1.101",
            user="testuser",
            service="ssh",
            action="failed_login",
        )
    )

    events.append(
        LogEvent(
            timestamp=base_time - 100,
            source="syslog",
            message="Error: Connection timeout",
            level="ERROR",
            service="nginx",
        )
    )

    return events


# ============================================================================
# MOCK FILE SYSTEM FIXTURES
# ============================================================================


@pytest.fixture
def mock_proc_files(temp_dir):
    """Create mock /proc filesystem structure"""
    proc_dir = temp_dir / "proc"
    proc_dir.mkdir(exist_ok=True)

    # Mock /proc/stat
    stat_file = proc_dir / "stat"
    stat_file.write_text("cpu  100 200 300 400 500 600 700 0 0 0\n")

    # Mock /proc/meminfo
    meminfo_file = proc_dir / "meminfo"
    meminfo_file.write_text("""MemTotal:        8388608 kB
MemFree:         4194304 kB
MemAvailable:    4194304 kB
Buffers:          524288 kB
Cached:          1048576 kB
""")

    # Mock /proc/loadavg
    loadavg_file = proc_dir / "loadavg"
    loadavg_file.write_text("1.50 2.00 1.80 2/150 1234\n")

    # Mock /proc/net/dev
    net_dev_dir = proc_dir / "net"
    net_dev_dir.mkdir(exist_ok=True)
    net_dev_file = net_dev_dir / "dev"
    net_dev_file.write_text("""Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000000  1000    0    0    0     0          0         0  1000000  1000    0    0    0     0       0          0
  eth0: 2000000  2000    5    3    0     0          0         0  1000000  1500    2    1    0     0       0          0
""")

    # Mock /proc/net/tcp
    tcp_file = net_dev_dir / "tcp"
    tcp_file.write_text("""  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:0050 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12345 1 0000000000000000 100 0 0 10 0
   1: 0100007F:0051 0100007F:9999 01 00000000:00000000 00:00000000 00000000     0        0 12346 1 0000000000000000 100 0 0 10 0
""")

    # Mock /proc/diskstats
    diskstats_file = proc_dir / "diskstats"
    diskstats_file.write_text(
        "   8       0 sda 1000 0 8000 100 2000 0 16000 200 0 100 300\n"
    )

    yield proc_dir


@pytest.fixture
def mock_log_file(temp_dir):
    """Create a mock log file with sample content"""
    log_file = temp_dir / "test.log"
    log_content = """2025-01-15 10:00:00 server sshd[1234]: Failed password for invalid user admin from 192.168.1.100
2025-01-15 10:01:00 server fail2ban[5678]: [sshd] Ban 192.168.1.100
2025-01-15 10:02:00 server nginx[9012]: Error: Connection timeout
2025-01-15 10:03:00 server sshd[1234]: Accepted publickey for user1 from 192.168.1.101
"""
    log_file.write_text(log_content)
    yield str(log_file)


# ============================================================================
# DATABASE FIXTURES
# ============================================================================


@pytest.fixture
def test_store(temp_db_path):
    """Create a test SQLiteStore instance with mocked path validation"""
    from logly.storage.sqlite_store import SQLiteStore

    # Mock the path validation to accept our temp path
    with patch("logly.storage.sqlite_store.validate_db_path", return_value=True):
        with patch(
            "logly.storage.sqlite_store.get_db_path", return_value=Path(temp_db_path)
        ):
            store = SQLiteStore(temp_db_path)
            yield store


@pytest.fixture
def populated_store(
    test_store, mock_system_metric, mock_network_metric, mock_log_event
):
    """Create a store with some test data"""
    test_store.insert_system_metric(mock_system_metric)
    test_store.insert_network_metric(mock_network_metric)
    test_store.insert_log_event(mock_log_event)
    yield test_store


# ============================================================================
# CONFIGURATION FIXTURES
# ============================================================================


@pytest.fixture
def mock_config():
    """Create a mock Config object"""
    config_dict = {
        "database": {"path": "/tmp/test.db", "retention_days": 7},
        "collection": {"system_metrics": 60, "network_metrics": 60, "log_parsing": 300},
        "system": {"enabled": True, "metrics": ["cpu_percent", "memory_percent"]},
        "network": {"enabled": True, "metrics": ["bytes_sent", "bytes_recv"]},
        "logs": {
            "enabled": True,
            "sources": {"test": {"path": "/tmp/test.log", "enabled": True}},
        },
        "aggregation": {
            "enabled": True,
            "intervals": ["hourly", "daily"],
            "keep_raw_data_days": 7,
        },
        "export": {"default_format": "csv", "timestamp_format": "%Y-%m-%d %H:%M:%S"},
        "tracing": {
            "enabled": True,
            "trace_processes": True,
            "trace_network": True,
            "trace_ips": True,
            "trace_errors": True,
        },
        "logging": {"level": "INFO"},
    }

    mock_config = Mock()
    mock_config.config = config_dict
    mock_config.get = lambda key, default=None: config_dict.get(key, default)
    mock_config.get_database_config = lambda: config_dict["database"]
    mock_config.get_collection_config = lambda: config_dict["collection"]
    mock_config.get_system_config = lambda: config_dict["system"]
    mock_config.get_network_config = lambda: config_dict["network"]
    mock_config.get_logs_config = lambda: config_dict["logs"]
    mock_config.get_aggregation_config = lambda: config_dict["aggregation"]
    mock_config.get_export_config = lambda: config_dict["export"]
    mock_config.get_logging_config = lambda: config_dict["logging"]

    yield mock_config


# ============================================================================
# MOCK PATCH HELPERS
# ============================================================================


@pytest.fixture
def mock_time():
    """Mock time.time() to return a fixed timestamp"""
    fixed_time = 1234567890
    with patch("time.time", return_value=fixed_time):
        yield fixed_time


@pytest.fixture
def mock_datetime():
    """Mock datetime.now() for consistent timestamps"""
    from datetime import datetime

    fixed_datetime = datetime(2025, 1, 15, 12, 0, 0)
    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_datetime
        mock_dt.fromtimestamp = datetime.fromtimestamp
        yield fixed_datetime


# ============================================================================
# CLEANUP FIXTURES
# ============================================================================


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Automatically clean up after each test"""
    yield
    # Clean up any temp files created during tests
    import gc

    gc.collect()


# ============================================================================
# TEST MARKERS
# ============================================================================


def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests that test individual components"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests that test component interactions"
    )
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests that test complete workflows"
    )
    config.addinivalue_line("markers", "slow: Tests that take a long time to run")
    config.addinivalue_line(
        "markers", "requires_root: Tests that require root privileges"
    )
