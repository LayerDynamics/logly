"""
Unit tests for IssueDetector class
"""
import pytest
import time
from unittest.mock import Mock, MagicMock
from logly.query.issue_detector import IssueDetector
from logly.query.models import (
    BruteForceAlert, IPThreat, HighUsagePeriod, DiskAlert,
    ErrorSpike, RecurringError, CriticalError, NetworkIssue
)


@pytest.fixture
def mock_store():
    """Create a mock SQLiteStore"""
    return Mock()


@pytest.fixture
def detector(mock_store):
    """Create IssueDetector with mock store"""
    config = {
        "high_cpu_percent": 85,
        "high_memory_percent": 90,
        "disk_space_critical": 90,
        "error_spike_multiplier": 3.0,
        "failed_login_threshold": 5,
        "threat_score_high": 70,
        "network_error_rate": 5.0,
        "sustained_duration_min": 300,
    }
    return IssueDetector(mock_store, config)


class TestSecurityDetection:
    """Tests for security issue detection"""

    def test_find_brute_force_attempts_no_events(self, detector, mock_store):
        """Test brute force detection with no events"""
        mock_store.get_log_events.return_value = []

        alerts = detector.find_brute_force_attempts(hours=24)

        assert len(alerts) == 0

    def test_find_brute_force_attempts_below_threshold(self, detector, mock_store):
        """Test brute force detection with attempts below threshold"""
        current_time = int(time.time())
        mock_store.get_log_events.return_value = [
            {
                "timestamp": current_time - 3600,
                "action": "failed_login",
                "ip_address": "1.2.3.4",
                "user": "admin"
            },
            {
                "timestamp": current_time - 3500,
                "action": "failed_login",
                "ip_address": "1.2.3.4",
                "user": "admin"
            }
        ]

        alerts = detector.find_brute_force_attempts(hours=24)

        assert len(alerts) == 0  # Only 2 attempts, threshold is 5

    def test_find_brute_force_attempts_above_threshold(self, detector, mock_store):
        """Test brute force detection with attempts above threshold"""
        current_time = int(time.time())
        mock_store.get_log_events.return_value = [
            {
                "timestamp": current_time - i * 100,
                "action": "failed_login",
                "ip_address": "1.2.3.4",
                "user": "admin"
            }
            for i in range(6)
        ]

        alerts = detector.find_brute_force_attempts(hours=24, threshold=5)

        assert len(alerts) == 1
        assert isinstance(alerts[0], BruteForceAlert)
        assert alerts[0].ip_address == "1.2.3.4"
        assert alerts[0].attempt_count == 6
        assert alerts[0].severity >= 50

    def test_find_brute_force_multiple_ips(self, detector, mock_store):
        """Test brute force detection with multiple IPs"""
        current_time = int(time.time())
        events = []

        # IP 1: 6 attempts
        for i in range(6):
            events.append({
                "timestamp": current_time - i * 100,
                "action": "failed_login",
                "ip_address": "1.2.3.4",
                "user": "admin"
            })

        # IP 2: 10 attempts
        for i in range(10):
            events.append({
                "timestamp": current_time - i * 100,
                "action": "failed_login",
                "ip_address": "5.6.7.8",
                "user": "root"
            })

        mock_store.get_log_events.return_value = events

        alerts = detector.find_brute_force_attempts(hours=24)

        assert len(alerts) == 2
        # Should be sorted by severity (more attempts = higher severity)
        assert alerts[0].attempt_count >= alerts[1].attempt_count

    def test_find_suspicious_ips_empty(self, detector, mock_store):
        """Test suspicious IP detection with no threats"""
        mock_store.get_high_threat_ips.return_value = []

        threats = detector.find_suspicious_ips()

        assert len(threats) == 0

    def test_find_suspicious_ips_found(self, detector, mock_store):
        """Test suspicious IP detection with threats"""
        mock_store.get_high_threat_ips.return_value = [
            {"ip_address": "1.2.3.4", "threat_score": 85}
        ]
        mock_store.get_ip_reputation.return_value = {
            "ip_address": "1.2.3.4",
            "threat_score": 85,
            "failed_login_count": 10,
            "ban_count": 2,
            "first_seen": int(time.time()) - 86400,
            "last_seen": int(time.time())
        }

        threats = detector.find_suspicious_ips(threat_threshold=70)

        assert len(threats) == 1
        assert isinstance(threats[0], IPThreat)
        assert threats[0].ip_address == "1.2.3.4"
        assert threats[0].threat_score == 85
        assert threats[0].failed_login_count == 10

    def test_find_banned_ips(self, detector, mock_store):
        """Test banned IP detection"""
        current_time = int(time.time())
        mock_store.get_log_events.return_value = [
            {
                "timestamp": current_time - 3600,
                "action": "ban",
                "ip_address": "1.2.3.4",
                "message": "Banned for excessive failed logins"
            }
        ]

        bans = detector.find_banned_ips(hours=24)

        assert len(bans) == 1
        assert bans[0].ip_address == "1.2.3.4"
        assert bans[0].severity == 70


class TestPerformanceDetection:
    """Tests for performance issue detection"""

    def test_find_high_cpu_no_data(self, detector, mock_store):
        """Test CPU detection with no data"""
        mock_store.get_system_metrics.return_value = []

        issues = detector.find_high_cpu_periods(hours=24)

        assert len(issues) == 0

    def test_find_high_cpu_below_threshold(self, detector, mock_store):
        """Test CPU detection below threshold"""
        current_time = int(time.time())
        mock_store.get_system_metrics.return_value = [
            {
                "timestamp": current_time - i * 60,
                "cpu_percent": 50.0
            }
            for i in range(10)
        ]

        issues = detector.find_high_cpu_periods(threshold=85, hours=24)

        assert len(issues) == 0

    def test_find_high_cpu_above_threshold_short_duration(self, detector, mock_store):
        """Test CPU detection above threshold but short duration"""
        current_time = int(time.time())
        mock_store.get_system_metrics.return_value = [
            {
                "timestamp": current_time - i * 60,
                "cpu_percent": 90.0
            }
            for i in range(3)  # Only 3 minutes
        ]

        issues = detector.find_high_cpu_periods(threshold=85, duration=300, hours=24)

        assert len(issues) == 0  # Duration < 300 seconds

    def test_find_high_cpu_sustained(self, detector, mock_store):
        """Test CPU detection with sustained high usage"""
        current_time = int(time.time())
        mock_store.get_system_metrics.return_value = [
            {
                "timestamp": current_time - i * 60,
                "cpu_percent": 92.0
            }
            for i in range(10)  # 10 minutes = 600 seconds
        ]

        issues = detector.find_high_cpu_periods(threshold=85, duration=300, hours=24)

        assert len(issues) == 1
        assert isinstance(issues[0], HighUsagePeriod)
        assert issues[0].resource_type == "cpu"
        assert issues[0].peak_value == 92.0
        assert issues[0].sustained_duration >= 540  # 9 * 60

    def test_find_high_memory_periods(self, detector, mock_store):
        """Test memory detection with high usage"""
        current_time = int(time.time())
        mock_store.get_system_metrics.return_value = [
            {
                "timestamp": current_time - i * 60,
                "memory_percent": 95.0
            }
            for i in range(10)
        ]

        issues = detector.find_high_memory_periods(threshold=90, duration=300, hours=24)

        assert len(issues) == 1
        assert issues[0].resource_type == "memory"
        assert issues[0].peak_value == 95.0

    def test_find_disk_space_issues_ok(self, detector, mock_store):
        """Test disk space detection when OK"""
        mock_store.get_system_metrics.return_value = [
            {
                "timestamp": int(time.time()),
                "disk_percent": 50.0,
                "disk_total": 1000000000,
                "disk_used": 500000000
            }
        ]

        issues = detector.find_disk_space_issues(threshold=90)

        assert len(issues) == 0

    def test_find_disk_space_issues_critical(self, detector, mock_store):
        """Test disk space detection when critical"""
        mock_store.get_system_metrics.return_value = [
            {
                "timestamp": int(time.time()),
                "disk_percent": 95.0,
                "disk_total": 1000000000,
                "disk_used": 950000000
            }
        ]

        issues = detector.find_disk_space_issues(threshold=90)

        assert len(issues) == 1
        assert isinstance(issues[0], DiskAlert)
        assert issues[0].usage_percent == 95.0
        assert issues[0].severity >= 70


class TestErrorDetection:
    """Tests for error issue detection"""

    def test_find_error_spikes_insufficient_data(self, detector, mock_store):
        """Test error spike detection with insufficient data"""
        mock_store.get_log_events.return_value = [
            {"timestamp": int(time.time()), "source": "app", "level": "ERROR"}
        ]

        spikes = detector.find_error_spikes(hours=24)

        assert len(spikes) == 0  # Need at least 10 events

    def test_find_error_spikes_no_spike(self, detector, mock_store):
        """Test error spike detection with consistent rate"""
        current_time = int(time.time())
        hour = 3600

        # Generate 5 errors per hour for 5 hours
        events = []
        for h in range(5):
            for i in range(5):
                events.append({
                    "timestamp": current_time - h * hour - i * 100,
                    "source": "app",
                    "level": "ERROR",
                    "message": "Test error"
                })

        mock_store.get_log_events.return_value = events

        spikes = detector.find_error_spikes(hours=24, spike_multiplier=3.0)

        assert len(spikes) == 0  # No spike, consistent rate

    def test_find_error_spikes_detected(self, detector, mock_store):
        """Test error spike detection with actual spike"""
        current_time = int(time.time())
        current_hour_bucket = current_time // 3600
        hour = 3600

        events = []

        # Normal rate: 5 errors/hour for hours 1-3 (going back in time)
        for h in range(1, 4):
            # Create timestamps at the start of each hour bucket
            hour_bucket = current_hour_bucket - h
            base_time = hour_bucket * 3600 + 1800  # Middle of that hour
            for i in range(5):
                events.append({
                    "timestamp": base_time + i * 60,  # Spread within the hour
                    "source": "app",
                    "level": "ERROR",
                    "message": "Test error"
                })

        # Spike: 20 errors in current hour bucket
        # Create timestamps in the current hour bucket
        base_time = current_hour_bucket * 3600 + 1800  # Middle of current hour
        for i in range(20):
            events.append({
                "timestamp": base_time + i * 30,  # Spread across 10 minutes
                "source": "app",
                "level": "ERROR",
                "message": "Test error"
            })

        mock_store.get_log_events.return_value = events

        spikes = detector.find_error_spikes(hours=24, spike_multiplier=3.0)

        assert len(spikes) >= 1
        assert isinstance(spikes[0], ErrorSpike)
        assert spikes[0].spike_factor >= 3.0

    def test_find_recurring_errors(self, detector, mock_store):
        """Test recurring error detection"""
        current_time = int(time.time())

        # Generate 10 errors of the same type
        events = [
            {
                "timestamp": current_time - i * 300,
                "source": "app",
                "action": "database_error",
                "level": "ERROR",
                "message": "Connection timeout"
            }
            for i in range(10)
        ]

        mock_store.get_log_events.return_value = events

        recurring = detector.find_recurring_errors(hours=24, min_occurrences=5)

        assert len(recurring) >= 1
        assert isinstance(recurring[0], RecurringError)
        assert recurring[0].occurrence_count >= 5

    def test_find_critical_errors(self, detector, mock_store):
        """Test critical error detection"""
        current_time = int(time.time())

        mock_store.get_traces.return_value = [
            {
                "timestamp": current_time - 3600,
                "severity": 90,
                "source": "app",
                "message": "Critical system failure",
                "error_category": "system",
                "stacktrace": "Error: Critical failure\n  at app.py:123",
                "impact": "Service unavailable"
            }
        ]

        critical = detector.find_critical_errors(hours=24)

        assert len(critical) == 1
        assert isinstance(critical[0], CriticalError)
        assert critical[0].severity >= 80

    def test_find_database_issues(self, detector, mock_store):
        """Test database issue detection"""
        mock_store.get_error_patterns.return_value = {
            "by_category": [
                {
                    "error_category": "database",
                    "count": 15
                }
            ],
            "by_type": [
                {
                    "error_type": "connection_timeout",
                    "count": 5
                }
            ]
        }

        issues = detector.find_database_issues(hours=24)

        assert len(issues) >= 1
        assert issues[0].error_category == "database"
        assert issues[0].occurrence_count == 15


class TestNetworkDetection:
    """Tests for network issue detection"""

    def test_find_connection_anomalies_insufficient_data(self, detector, mock_store):
        """Test connection anomaly detection with insufficient data"""
        mock_store.get_network_metrics.return_value = [
            {"timestamp": int(time.time()), "connections_established": 100}
        ]

        anomalies = detector.find_connection_anomalies(hours=24)

        assert len(anomalies) == 0  # Need at least 10 data points

    def test_find_connection_anomalies_normal(self, detector, mock_store):
        """Test connection anomaly detection with normal data"""
        current_time = int(time.time())

        # Generate consistent connection counts around 100
        mock_store.get_network_metrics.return_value = [
            {
                "timestamp": current_time - i * 60,
                "connections_established": 100 + (i % 5)
            }
            for i in range(20)
        ]

        anomalies = detector.find_connection_anomalies(hours=24)

        assert len(anomalies) == 0  # No anomalies, data is consistent

    def test_find_connection_anomalies_detected(self, detector, mock_store):
        """Test connection anomaly detection with anomaly"""
        current_time = int(time.time())

        metrics = []

        # Normal: ~100 connections
        for i in range(19):
            metrics.append({
                "timestamp": current_time - i * 60,
                "connections_established": 100
            })

        # Anomaly: 500 connections (5x normal)
        metrics.append({
            "timestamp": current_time,
            "connections_established": 500
        })

        mock_store.get_network_metrics.return_value = metrics

        anomalies = detector.find_connection_anomalies(hours=24)

        assert len(anomalies) >= 1

    def test_find_network_errors_ok(self, detector, mock_store):
        """Test network error detection with acceptable rates"""
        mock_store.get_network_metrics.return_value = [
            {
                "timestamp": int(time.time()),
                "errors_in": 1,
                "errors_out": 1,
                "drops_in": 1,
                "drops_out": 1,
                "packets_recv": 10000,
                "packets_sent": 10000,
                "connections_established": 100
            }
        ]

        issues = detector.find_network_errors(hours=24, threshold=5.0)

        assert len(issues) == 0  # Error rate < 5%

    def test_find_network_errors_high_rate(self, detector, mock_store):
        """Test network error detection with high error rate"""
        mock_store.get_network_metrics.return_value = [
            {
                "timestamp": int(time.time()),
                "errors_in": 300,
                "errors_out": 300,
                "drops_in": 200,
                "drops_out": 200,
                "packets_recv": 10000,
                "packets_sent": 10000,
                "connections_established": 100
            }
        ]

        issues = detector.find_network_errors(hours=24, threshold=5.0)

        assert len(issues) == 1
        assert isinstance(issues[0], NetworkIssue)
        assert issues[0].error_rate >= 5.0


class TestConfiguration:
    """Tests for configuration handling"""

    def test_default_thresholds(self, mock_store):
        """Test that default thresholds are used when not configured"""
        detector = IssueDetector(mock_store, {})

        assert detector.thresholds["high_cpu_percent"] == 85
        assert detector.thresholds["high_memory_percent"] == 90
        assert detector.thresholds["failed_login_threshold"] == 5

    def test_custom_thresholds(self, mock_store):
        """Test that custom thresholds override defaults"""
        config = {
            "high_cpu_percent": 95,
            "failed_login_threshold": 10
        }
        detector = IssueDetector(mock_store, config)

        assert detector.thresholds["high_cpu_percent"] == 95
        assert detector.thresholds["failed_login_threshold"] == 10
        # Default should still be present
        assert detector.thresholds["high_memory_percent"] == 90
