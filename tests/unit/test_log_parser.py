"""
Unit tests for logly.collectors.log_parser module
Tests log parsing from various sources using regex patterns
"""

import pytest
from unittest.mock import Mock, patch, mock_open

from logly.collectors.log_parser import LogParser


class TestLogParser:
    """Test suite for LogParser class"""

    @pytest.mark.unit
    def test_init(self):
        """Test LogParser initialization"""
        config = {
            "enabled": True,
            "sources": {
                "fail2ban": {"path": "/var/log/fail2ban.log", "enabled": True},
                "syslog": {"path": "/var/log/syslog", "enabled": False},
            },
        }

        parser = LogParser(config)

        assert parser.config == config
        assert parser.enabled
        assert parser.log_sources == config["sources"]
        assert parser._file_positions == {}
        assert parser.PATTERNS is not None

    @pytest.mark.unit
    def test_patterns_compiled(self):
        """Test that regex patterns are properly compiled"""
        config = {"sources": {}}
        parser = LogParser(config)

        # Check that patterns are compiled regex objects
        assert hasattr(parser.PATTERNS["fail2ban_ban"], "search")
        assert hasattr(parser.PATTERNS["auth_failed"], "search")
        assert hasattr(parser.PATTERNS["syslog_error"], "search")

    @pytest.mark.unit
    def test_collect(self, temp_dir):
        """Test collect method"""
        # Create a real file for the test
        log_file = temp_dir / "test.log"
        log_content = """2025-01-15 10:00:00 server fail2ban[1234]: [sshd] Ban 192.168.1.100
2025-01-15 10:01:00 server sshd[5678]: Failed password for testuser from 192.168.1.101"""
        log_file.write_text(log_content)

        config = {"sources": {"test_log": {"path": str(log_file), "enabled": True}}}

        parser = LogParser(config)
        events = parser.collect()

        # Should parse events from log file
        assert len(events) > 0

    @pytest.mark.unit
    @patch("logly.collectors.log_parser.Path")
    def test_collect_with_disabled_source(self, mock_path):
        """Test collect with disabled source"""
        config = {
            "sources": {
                "test_log": {
                    "path": "/tmp/test.log",
                    "enabled": False,  # Disabled
                }
            }
        }

        parser = LogParser(config)
        events = parser.collect()

        assert events == []

    @pytest.mark.unit
    @patch("logly.collectors.log_parser.Path")
    def test_collect_with_nonexistent_file(self, mock_path):
        """Test collect with non-existent log file"""
        mock_path.return_value.exists.return_value = False

        config = {
            "sources": {"test_log": {"path": "/tmp/nonexistent.log", "enabled": True}}
        }

        parser = LogParser(config)
        events = parser.collect()

        assert events == []

    @pytest.mark.unit
    def test_parse_fail2ban_ban(self):
        """Test parsing fail2ban ban message"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "2025-01-15 10:00:00 server fail2ban[1234]: [sshd] Ban 192.168.1.100"
        event = parser._parse_fail2ban(line)

        assert event is not None
        assert event.source == "fail2ban"
        assert event.level == "WARNING"
        assert event.ip_address == "192.168.1.100"
        assert event.service == "sshd"
        assert event.action == "ban"
        assert event.metadata is not None
        assert event.metadata["jail"] == "sshd"

    @pytest.mark.unit
    def test_parse_fail2ban_unban(self):
        """Test parsing fail2ban unban message"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "2025-01-15 10:00:00 server fail2ban[1234]: [nginx] Unban 10.0.0.5"
        event = parser._parse_fail2ban(line)

        assert event is not None
        assert event.level == "INFO"
        assert event.ip_address == "10.0.0.5"
        assert event.service == "nginx"
        assert event.action == "unban"

    @pytest.mark.unit
    def test_parse_fail2ban_found(self):
        """Test parsing fail2ban found message"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "2025-01-15 10:00:00 server fail2ban[1234]: [sshd] Found 192.168.1.100"
        event = parser._parse_fail2ban(line)

        assert event is not None
        assert event.level == "INFO"
        assert event.ip_address == "192.168.1.100"
        assert event.action == "found"

    @pytest.mark.unit
    def test_parse_auth_failed_login(self):
        """Test parsing auth.log failed login"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "Jan 15 10:00:00 server sshd[1234]: Failed password for invalid user admin from 192.168.1.100"
        event = parser._parse_auth_log(line)

        assert event is not None
        assert event.source == "auth"
        assert event.level == "WARNING"
        assert event.ip_address == "192.168.1.100"
        assert event.user == "admin"
        assert event.service == "ssh"
        assert event.action == "failed_login"

    @pytest.mark.unit
    def test_parse_auth_accepted(self):
        """Test parsing auth.log accepted authentication"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "Jan 15 10:00:00 server sshd[1234]: Accepted publickey for testuser from 192.168.1.101"
        event = parser._parse_auth_log(line)

        assert event is not None
        assert event.level == "INFO"
        assert event.ip_address == "192.168.1.101"
        assert event.user == "testuser"
        assert event.action == "successful_login"
        assert event.metadata is not None
        assert event.metadata["method"] == "publickey"

    @pytest.mark.unit
    def test_parse_syslog_error(self):
        """Test parsing syslog error message"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "Jan 15 10:00:00 server nginx[1234]: Error: Connection timeout"
        event = parser._parse_syslog(line)

        assert event is not None
        assert event.source == "syslog"
        assert event.level == "ERROR"
        assert "Connection timeout" in event.message

    @pytest.mark.unit
    def test_parse_syslog_warning(self):
        """Test parsing syslog warning message"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "Jan 15 10:00:00 server kernel: WARNING: High memory usage detected"
        event = parser._parse_syslog(line)

        assert event is not None
        assert event.level == "WARNING"

    @pytest.mark.unit
    def test_parse_django_log(self):
        """Test parsing Django log message"""
        config = {"sources": {}}
        parser = LogParser(config)

        # Django log with level
        line = "[ERROR] Database connection failed: timeout"
        event = parser._parse_django_log(line)

        assert event is not None
        assert event.source == "django"
        assert event.level == "ERROR"
        assert event.message == "Database connection failed: timeout"
        assert event.service == "django"

    @pytest.mark.unit
    def test_parse_django_log_no_level(self):
        """Test parsing Django log without level marker"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = "Request processed successfully"
        event = parser._parse_django_log(line)

        assert event is not None
        assert event.level == "INFO"
        assert event.message == line

    @pytest.mark.unit
    def test_parse_nginx_log_success(self):
        """Test parsing nginx access log with 200 status"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = '192.168.1.100 - - [15/Jan/2025:10:00:00 +0000] "GET /index.html HTTP/1.1" 200 1024'
        event = parser._parse_nginx_log(line)

        assert event is not None
        assert event.source == "nginx"
        assert event.level == "INFO"
        assert event.ip_address == "192.168.1.100"
        assert event.action == "http_request"
        assert event.metadata is not None
        assert event.metadata["status"] == 200
        assert event.metadata["request"] == "GET /index.html HTTP/1.1"

    @pytest.mark.unit
    def test_parse_nginx_log_error(self):
        """Test parsing nginx access log with 500 status"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = '10.0.0.1 - - [15/Jan/2025:10:00:00 +0000] "POST /api/data HTTP/1.1" 500 256'
        event = parser._parse_nginx_log(line)

        assert event is not None
        assert event.level == "ERROR"
        assert event.metadata is not None
        assert event.metadata["status"] == 500

    @pytest.mark.unit
    def test_parse_nginx_log_warning(self):
        """Test parsing nginx access log with 404 status"""
        config = {"sources": {}}
        parser = LogParser(config)

        line = '192.168.1.100 - - [15/Jan/2025:10:00:00 +0000] "GET /missing.html HTTP/1.1" 404 512'
        event = parser._parse_nginx_log(line)

        assert event is not None
        assert event.level == "WARNING"
        assert event.metadata is not None
        assert event.metadata["status"] == 404

    @pytest.mark.unit
    def test_parse_generic(self):
        """Test generic log parsing"""
        config = {"sources": {}}
        parser = LogParser(config)

        # Test error detection
        line = "Something went wrong: critical error occurred"
        event = parser._parse_generic("custom", line)

        assert event is not None
        assert event.source == "custom"
        assert event.level == "CRITICAL"
        assert event.message == line

    @pytest.mark.unit
    def test_parse_generic_level_detection(self):
        """Test level detection in generic parser"""
        config = {"sources": {}}
        parser = LogParser(config)

        test_cases = [
            ("FATAL: System crash", "CRITICAL"),
            ("ERROR: Connection failed", "ERROR"),
            ("Warning: Low disk space", "WARNING"),
            ("Info: Process started", "INFO"),
            ("Normal log message", "INFO"),
        ]

        for line, expected_level in test_cases:
            event = parser._parse_generic("test", line)
            assert event is not None
            assert event.level == expected_level

    @pytest.mark.unit
    @patch("builtins.open", new_callable=mock_open)
    @patch("logly.collectors.log_parser.Path")
    def test_file_position_tracking(self, mock_path, mock_file):
        """Test that file positions are tracked correctly"""
        mock_path.return_value.exists.return_value = True
        mock_path.return_value.stat.return_value.st_size = 1000

        # First read
        mock_file.return_value.tell.return_value = 500
        mock_file.return_value.__iter__.return_value = ["line1", "line2"]

        config = {"sources": {"test": {"path": "/tmp/test.log", "enabled": True}}}

        parser = LogParser(config)
        parser.collect()

        # Check position was saved
        assert parser._file_positions["/tmp/test.log"] == 500

    @pytest.mark.unit
    @patch("builtins.open", new_callable=mock_open)
    @patch("logly.collectors.log_parser.Path")
    def test_file_rotation_detection(self, mock_path, mock_file):
        """Test detection of log file rotation"""
        mock_path.return_value.exists.return_value = True

        config = {"sources": {"test": {"path": "/tmp/test.log", "enabled": True}}}

        parser = LogParser(config)

        # Set initial position
        parser._file_positions["/tmp/test.log"] = 1000

        # Mock smaller file size (indicates rotation)
        mock_path.return_value.stat.return_value.st_size = 100

        mock_file.return_value.tell.return_value = 50
        mock_file.return_value.__iter__.return_value = []

        parser.collect()

        # Should seek to 0 (start of new file)
        mock_file.return_value.seek.assert_called_with(0)

    @pytest.mark.unit
    @patch("logly.collectors.log_parser.Path")
    def test_validate(self, mock_path):
        """Test validate method"""
        # At least one source exists
        mock_path.return_value.exists.side_effect = [
            False,
            True,
        ]  # First doesn't exist, second does

        config = {
            "sources": {
                "source1": {"path": "/tmp/log1.log", "enabled": True},
                "source2": {"path": "/tmp/log2.log", "enabled": True},
            }
        }

        parser = LogParser(config)
        assert parser.validate()

    @pytest.mark.unit
    @patch("logly.collectors.log_parser.Path")
    def test_validate_no_sources(self, mock_path):
        """Test validate with no accessible sources"""
        mock_path.return_value.exists.return_value = False

        config = {"sources": {"source1": {"path": "/tmp/log1.log", "enabled": True}}}

        parser = LogParser(config)
        assert not parser.validate()

    @pytest.mark.unit
    def test_parse_line_routing(self):
        """Test that _parse_line routes to correct parser"""
        config = {"sources": {}}
        parser = LogParser(config)

        # Mock individual parsers
        parser._parse_fail2ban = Mock(return_value="fail2ban_event")
        parser._parse_auth_log = Mock(return_value="auth_event")
        parser._parse_syslog = Mock(return_value="syslog_event")
        parser._parse_django_log = Mock(return_value="django_event")
        parser._parse_nginx_log = Mock(return_value="nginx_event")
        parser._parse_generic = Mock(return_value="generic_event")

        # Test routing
        assert parser._parse_line("fail2ban", "test") == "fail2ban_event"
        assert parser._parse_line("auth", "test") == "auth_event"
        assert parser._parse_line("syslog", "test") == "syslog_event"
        assert parser._parse_line("django", "test") == "django_event"
        assert parser._parse_line("nginx", "test") == "nginx_event"
        assert parser._parse_line("unknown", "test") == "generic_event"
