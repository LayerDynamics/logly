"""
Issue detection engine for identifying problems in system data.
"""

import time
from typing import List, Optional, Dict, Any
from collections import defaultdict
from logly.storage.sqlite_store import SQLiteStore
from logly.query.models import (
    BruteForceAlert,
    IPThreat,
    HighUsagePeriod,
    DiskAlert,
    ErrorSpike,
    RecurringError,
    CriticalError,
    ConnectionAnomaly,
    NetworkIssue,
    SecurityIssue,
    PerformanceIssue,
    ErrorIssue,
)


class IssueDetector:
    """Detects issues and problems in system data."""

    def __init__(self, store: SQLiteStore, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the issue detector.

        Args:
            store: SQLiteStore instance for data access
            config: Configuration dictionary with thresholds
        """
        self.store = store
        self.config = config or {}

        # Default thresholds (can be overridden by config)
        self.thresholds = {
            "high_cpu_percent": self.config.get("high_cpu_percent", 85),
            "high_memory_percent": self.config.get("high_memory_percent", 90),
            "disk_space_critical": self.config.get("disk_space_critical", 90),
            "error_spike_multiplier": self.config.get("error_spike_multiplier", 3.0),
            "failed_login_threshold": self.config.get("failed_login_threshold", 5),
            "threat_score_high": self.config.get("threat_score_high", 70),
            "network_error_rate": self.config.get("network_error_rate", 5.0),
            "sustained_duration_min": self.config.get(
                "sustained_duration_min", 300
            ),  # 5 minutes
        }

    # ============================================================================
    # Combined Issue Detection
    # ============================================================================

    def detect_all_issues(
        self, start_time: int, end_time: int
    ) -> List[Dict[str, Any]]:
        """
        Detect all types of issues within the specified time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            List of issue dictionaries with type, severity, and details
        """
        # Calculate hours from time range
        hours = max(1, (end_time - start_time) // 3600)

        all_issues = []

        # Security issues - pass explicit time range
        brute_force = self.find_brute_force_attempts(start_time=start_time, end_time=end_time)
        for alert in brute_force:
            all_issues.append({
                "type": "brute_force",
                "severity": alert.severity,
                "title": alert.title,
                "description": alert.description,
                "ip_address": alert.ip_address,
                "details": alert.__dict__,
            })

        suspicious_ips = self.find_suspicious_ips()
        for threat in suspicious_ips:
            all_issues.append({
                "type": "suspicious_ip",
                "severity": threat.severity,
                "title": threat.title,
                "description": threat.description,
                "ip_address": threat.ip_address,
                "details": threat.__dict__,
            })

        banned_ips = self.find_banned_ips(start_time=start_time, end_time=end_time)
        for issue in banned_ips:
            all_issues.append({
                "type": "banned_ip",
                "severity": issue.severity,
                "title": issue.title,
                "description": issue.description,
                "details": issue.__dict__,
            })

        # Performance issues
        high_cpu = self.find_high_cpu_periods(hours=hours, start_time=start_time, end_time=end_time)
        for period in high_cpu:
            all_issues.append({
                "type": "high_cpu",
                "severity": period.severity,
                "title": period.title,
                "description": period.description,
                "details": period.__dict__,
            })

        high_memory = self.find_high_memory_periods(hours=hours, start_time=start_time, end_time=end_time)
        for period in high_memory:
            all_issues.append({
                "type": "high_memory",
                "severity": period.severity,
                "title": period.title,
                "description": period.description,
                "details": period.__dict__,
            })

        disk_issues = self.find_disk_space_issues(hours=hours)
        for alert in disk_issues:
            all_issues.append({
                "type": "disk_space",
                "severity": alert.severity,
                "title": alert.title,
                "description": alert.description,
                "details": alert.__dict__,
            })

        # Error issues
        error_spikes = self.find_error_spikes(hours=hours)
        for spike in error_spikes:
            all_issues.append({
                "type": "error_spike",
                "severity": spike.severity,
                "title": spike.title,
                "description": spike.description,
                "details": spike.__dict__,
            })

        recurring_errors = self.find_recurring_errors(hours=hours)
        for error in recurring_errors:
            all_issues.append({
                "type": "recurring_error",
                "severity": error.severity,
                "title": error.title,
                "description": error.description,
                "details": error.__dict__,
            })

        critical_errors = self.find_critical_errors(hours=hours)
        for error in critical_errors:
            all_issues.append({
                "type": "critical_error",
                "severity": error.severity,
                "title": error.title,
                "description": error.description,
                "details": error.__dict__,
            })

        # Network issues
        connection_anomalies = self.find_connection_anomalies(hours=hours)
        for anomaly in connection_anomalies:
            all_issues.append({
                "type": "connection_anomaly",
                "severity": anomaly.severity,
                "title": anomaly.title,
                "description": anomaly.description,
                "details": anomaly.__dict__,
            })

        network_errors = self.find_network_errors(hours=hours)
        for issue in network_errors:
            all_issues.append({
                "type": "network_error",
                "severity": issue.severity,
                "title": issue.title,
                "description": issue.description,
                "details": issue.__dict__,
            })

        # Sort by severity (highest first)
        all_issues.sort(key=lambda x: x["severity"], reverse=True)

        return all_issues

    # ============================================================================
    # Security Issue Detection
    # ============================================================================

    def find_brute_force_attempts(
        self, hours: int = 24, threshold: Optional[int] = None, start_time: Optional[int] = None, end_time: Optional[int] = None
    ) -> List[BruteForceAlert]:
        """
        Find brute force attack attempts.

        Args:
            hours: Time window to analyze (default: 24, used if start_time/end_time not provided)
            threshold: Minimum failed attempts to consider (default: from config)
            start_time: Optional explicit start timestamp
            end_time: Optional explicit end timestamp

        Returns:
            List of BruteForceAlert objects
        """
        # Ensure threshold is set to a concrete int value
        threshold_value: int = threshold if threshold is not None else self.thresholds["failed_login_threshold"]

        # Use explicit time range if provided, otherwise calculate from hours
        if start_time is None or end_time is None:
            start_time = int(time.time()) - (hours * 3600)
            end_time = int(time.time())

        # Get failed login events
        events = self.store.get_log_events(
            start_time, end_time, source=None, level=None, limit=10000
        )

        # Group by IP address
        ip_attempts: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "users": set(), "first_seen": None, "last_seen": None}
        )

        for event in events:
            if event.get("action") == "failed_login" and event.get("ip_address"):
                ip = event["ip_address"]
                timestamp = event["timestamp"]
                user = event.get("user", "unknown")

                ip_attempts[ip]["count"] = ip_attempts[ip]["count"] + 1
                users_set = ip_attempts[ip]["users"]
                if isinstance(users_set, set):
                    users_set.add(user)

                first_seen = ip_attempts[ip]["first_seen"]
                if first_seen is None:
                    ip_attempts[ip]["first_seen"] = timestamp
                elif isinstance(first_seen, int):
                    ip_attempts[ip]["first_seen"] = min(first_seen, timestamp)

                last_seen = ip_attempts[ip]["last_seen"]
                if last_seen is None:
                    ip_attempts[ip]["last_seen"] = timestamp
                elif isinstance(last_seen, int):
                    ip_attempts[ip]["last_seen"] = max(last_seen, timestamp)

        # Create alerts for IPs exceeding threshold
        alerts = []
        for ip, data in ip_attempts.items():
            count = data["count"]
            first_seen = data["first_seen"]
            last_seen = data["last_seen"]
            users = data["users"]

            # Ensure all required fields are present and have correct types
            if not isinstance(count, int) or not isinstance(first_seen, int) or not isinstance(last_seen, int):
                continue
            if not isinstance(users, set):
                continue

            if count >= threshold_value:
                time_span = last_seen - first_seen
                users_list = list(users)

                # Calculate severity based on attempt count and time span
                severity = min(100, 50 + (count - threshold_value) * 5)
                if time_span < 300:  # Less than 5 minutes
                    severity = min(100, severity + 20)

                alert = BruteForceAlert(
                    severity=severity,
                    title=f"Brute Force Attack from {ip}",
                    description=f"{count} failed login attempts from {ip} targeting {len(users_list)} user(s)",
                    first_seen=first_seen,
                    last_seen=last_seen,
                    occurrence_count=count,
                    affected_resources=users_list,
                    recommendations=[
                        f"Consider blocking IP {ip}",
                        "Enable rate limiting on authentication endpoints",
                        "Review authentication logs for compromised accounts",
                    ],
                    ip_address=ip,
                    target_user=users_list[0]
                    if len(users_list) == 1
                    else f"{len(users_list)} users",
                    attempt_count=count,
                    time_span=time_span,
                    unique_users=len(users_list),
                )
                alerts.append(alert)

        return sorted(alerts, key=lambda x: x.severity, reverse=True)

    def find_suspicious_ips(
        self, threat_threshold: Optional[int] = None
    ) -> List[IPThreat]:
        """
        Find IPs with high threat scores.

        Args:
            threat_threshold: Minimum threat score (default: from config)

        Returns:
            List of IPThreat objects
        """
        # Ensure threat_threshold is set to a concrete int value
        threshold_value: int = threat_threshold if threat_threshold is not None else self.thresholds["threat_score_high"]

        # Get high-threat IPs from reputation table
        high_threat_ips = self.store.get_high_threat_ips(threshold_value)

        threats = []
        for ip_data in high_threat_ips:
            ip = ip_data["ip_address"]
            threat_score = ip_data["threat_score"]

            # Get full IP reputation data
            rep = self.store.get_ip_reputation(ip)
            if not rep:
                continue

            threat = IPThreat(
                severity=threat_score,
                title=f"High-Threat IP: {ip}",
                description=f"IP {ip} has a threat score of {threat_score}/100",
                first_seen=rep["first_seen"],
                last_seen=rep["last_seen"],
                occurrence_count=rep["failed_login_count"] + rep["ban_count"],
                affected_resources=[],
                recommendations=[
                    f"Review all activity from {ip}",
                    "Consider adding to IP blacklist",
                    "Check for successful authentications from this IP",
                ],
                ip_address=ip,
                threat_score=threat_score,
                failed_login_count=rep["failed_login_count"],
                ban_count=rep["ban_count"],
                first_activity=rep["first_seen"],
                last_activity=rep["last_seen"],
            )

            threats.append(threat)

        return sorted(threats, key=lambda x: x.threat_score, reverse=True)

    def find_unauthorized_access_attempts(self, hours: int = 24) -> List[SecurityIssue]:
        """
        Find unauthorized access attempts.

        Args:
            hours: Time window to analyze

        Returns:
            List of SecurityIssue objects
        """
        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        # Get failed authentication events
        events = self.store.get_log_events(
            start_time, end_time, source="auth", level=None, limit=10000
        )

        # Filter for permission denied, access denied, etc.
        unauthorized = []
        for event in events:
            message = event.get("message", "").lower()

            if any(
                keyword in message
                for keyword in ["denied", "unauthorized", "forbidden", "not permitted"]
            ):
                issue = SecurityIssue(
                    severity=60,
                    title="Unauthorized Access Attempt",
                    description=event.get("message", "Unknown"),
                    first_seen=event["timestamp"],
                    last_seen=event["timestamp"],
                    occurrence_count=1,
                    affected_resources=[event.get("user", "unknown")],
                    recommendations=[
                        "Review access controls",
                        "Audit user permissions",
                    ],
                    ip_address=event.get("ip_address"),
                    target_user=event.get("user"),
                )
                unauthorized.append(issue)

        return unauthorized

    def find_banned_ips(self, hours: int = 24, start_time: Optional[int] = None, end_time: Optional[int] = None) -> List[SecurityIssue]:
        """
        Find recently banned IPs.

        Args:
            hours: Time window to analyze (used if start_time/end_time not provided)
            start_time: Optional explicit start timestamp
            end_time: Optional explicit end timestamp

        Returns:
            List of SecurityIssue objects with ban information
        """
        # Use explicit time range if provided, otherwise calculate from hours
        if start_time is None or end_time is None:
            start_time = int(time.time()) - (hours * 3600)
            end_time = int(time.time())

        # Get ban events
        events = self.store.get_log_events(
            start_time, end_time, source=None, level=None, limit=10000
        )

        bans = []
        for event in events:
            if event.get("action") == "ban" and event.get("ip_address"):
                issue = SecurityIssue(
                    severity=70,
                    title=f"IP Banned: {event['ip_address']}",
                    description=event.get(
                        "message", f"IP {event['ip_address']} was banned"
                    ),
                    first_seen=event["timestamp"],
                    last_seen=event["timestamp"],
                    occurrence_count=1,
                    affected_resources=[],
                    recommendations=[
                        "Review ban reason",
                        "Ensure ban is legitimate",
                        "Monitor for ban evasion attempts",
                    ],
                    ip_address=event["ip_address"],
                    attack_type="banned",
                )
                bans.append(issue)

        return bans

    # ============================================================================
    # Performance Issue Detection
    # ============================================================================

    def find_high_cpu_periods(
        self,
        threshold: Optional[float] = None,
        duration: Optional[int] = None,
        hours: int = 24,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[HighUsagePeriod]:
        """
        Find periods of sustained high CPU usage.

        Args:
            threshold: CPU percentage threshold (default: from config)
            duration: Minimum sustained duration in seconds (default: from config)
            hours: Time window to analyze (used if start_time/end_time not provided)
            start_time: Optional explicit start timestamp
            end_time: Optional explicit end timestamp

        Returns:
            List of HighUsagePeriod objects
        """
        # Ensure parameters are set to concrete values
        threshold_value: float = threshold if threshold is not None else self.thresholds["high_cpu_percent"]
        duration_value: int = duration if duration is not None else self.thresholds["sustained_duration_min"]

        return self._find_high_resource_periods(
            metric_field="cpu_percent",
            threshold=threshold_value,
            duration=duration_value,
            hours=hours,
            resource_type="cpu",
            start_time=start_time,
            end_time=end_time,
        )

    def find_high_memory_periods(
        self,
        threshold: Optional[float] = None,
        duration: Optional[int] = None,
        hours: int = 24,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[HighUsagePeriod]:
        """
        Find periods of sustained high memory usage.

        Args:
            threshold: Memory percentage threshold (default: from config)
            duration: Minimum sustained duration in seconds (default: from config)
            hours: Time window to analyze (used if start_time/end_time not provided)
            start_time: Optional explicit start timestamp
            end_time: Optional explicit end timestamp

        Returns:
            List of HighUsagePeriod objects
        """
        # Ensure parameters are set to concrete values
        threshold_value: float = threshold if threshold is not None else self.thresholds["high_memory_percent"]
        duration_value: int = duration if duration is not None else self.thresholds["sustained_duration_min"]

        return self._find_high_resource_periods(
            metric_field="memory_percent",
            threshold=threshold_value,
            duration=duration_value,
            hours=hours,
            resource_type="memory",
            start_time=start_time,
            end_time=end_time,
        )

    def _find_high_resource_periods(
        self,
        metric_field: str,
        threshold: float,
        duration: int,
        hours: int,
        resource_type: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[HighUsagePeriod]:
        """
        Generic method to find periods of high resource usage.

        Args:
            metric_field: Field name in system_metrics table
            threshold: Percentage threshold
            duration: Minimum sustained duration
            hours: Time window to analyze (used if start_time/end_time not provided)
            resource_type: Type of resource (cpu, memory, disk)
            start_time: Optional explicit start timestamp
            end_time: Optional explicit end timestamp

        Returns:
            List of HighUsagePeriod objects
        """
        # Use explicit time range if provided, otherwise calculate from hours
        if start_time is None or end_time is None:
            start_time = int(time.time()) - (hours * 3600)
            end_time = int(time.time())

        metrics = self.store.get_system_metrics(start_time, end_time, limit=10000)

        if not metrics:
            return []

        # Calculate average sampling interval to adapt duration requirement
        sorted_metrics = sorted(metrics, key=lambda x: x["timestamp"])
        min_samples = 3  # Minimum number of samples for a sustained period

        # Find periods where metric exceeds threshold
        periods = []
        current_period = None

        for metric in sorted_metrics:
            value = metric.get(metric_field)

            # Skip if value is None (metric not collected)
            if value is None:
                continue

            if value >= threshold:
                if current_period is None:
                    # Start new period
                    current_period = {
                        "start": metric["timestamp"],
                        "end": metric["timestamp"],
                        "peak": value,
                        "values": [value],
                    }
                else:
                    # Continue current period
                    current_period["end"] = metric["timestamp"]
                    current_period["peak"] = max(current_period["peak"], value)
                    current_period["values"].append(value)
            else:
                if current_period is not None:
                    # End current period
                    period_duration = current_period["end"] - current_period["start"]
                    # Accept if duration requirement is met (or disabled) AND we have minimum number of samples
                    if (duration == 0 or period_duration >= duration) and len(current_period["values"]) >= min_samples:
                        periods.append(current_period)
                    current_period = None

        # Don't forget the last period
        if current_period is not None:
            period_duration = current_period["end"] - current_period["start"]
            # Accept if duration requirement is met (or disabled) AND we have minimum number of samples
            if (duration == 0 or period_duration >= duration) and len(current_period["values"]) >= min_samples:
                periods.append(current_period)

        # Create HighUsagePeriod objects
        issues = []
        for period in periods:
            avg_value = sum(period["values"]) / len(period["values"])
            sustained_duration = period["end"] - period["start"]

            # Calculate severity based on how much threshold was exceeded
            excess = period["peak"] - threshold
            severity = min(100, 60 + int(excess))

            issue = HighUsagePeriod(
                severity=severity,
                title=f"High {resource_type.upper()} Usage",
                description=f"{resource_type.upper()} usage sustained above {threshold}% for {sustained_duration} seconds",
                first_seen=period["start"],
                last_seen=period["end"],
                occurrence_count=len(period["values"]),
                affected_resources=[],
                recommendations=[
                    f"Investigate {resource_type} usage patterns",
                    f"Identify processes consuming high {resource_type}",
                    f"Consider scaling resources if {resource_type} usage remains high",
                ],
                metric_name=metric_field,
                current_value=period["values"][-1] if period["values"] else 0,
                threshold=threshold,
                peak_value=period["peak"],
                avg_value=avg_value,
                resource_type=resource_type,
                sustained_duration=sustained_duration,
            )
            issues.append(issue)

        return sorted(issues, key=lambda x: x.severity, reverse=True)

    def find_disk_space_issues(
        self, threshold: Optional[float] = None, hours: int = 1
    ) -> List[DiskAlert]:
        """
        Find disk space issues.

        Args:
            threshold: Disk usage percentage threshold (default: from config)
            hours: Time window (defaults to most recent hour)

        Returns:
            List of DiskAlert objects
        """
        # Ensure threshold is set to a concrete float value
        threshold_value: float = threshold if threshold is not None else self.thresholds["disk_space_critical"]

        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        metrics = self.store.get_system_metrics(start_time, end_time, limit=1)

        if not metrics:
            return []

        # Get most recent metric
        metric = metrics[0]
        disk_percent = metric.get("disk_percent")

        # Skip if disk_percent is None or not available
        if disk_percent is None:
            return []

        if disk_percent >= threshold_value:
            # Calculate severity
            excess = disk_percent - threshold_value
            severity = min(100, 70 + int(excess * 3))

            alert = DiskAlert(
                severity=severity,
                title="Low Disk Space",
                description=f"Disk usage at {disk_percent:.1f}% (threshold: {threshold_value}%)",
                first_seen=metric["timestamp"],
                last_seen=metric["timestamp"],
                occurrence_count=1,
                affected_resources=[],
                recommendations=[
                    "Clean up old log files",
                    "Remove temporary files",
                    "Archive or delete old data",
                    "Consider expanding disk capacity",
                ],
                disk_total=metric.get("disk_total", 0),
                disk_used=metric.get("disk_used", 0),
                disk_available=metric.get("disk_total", 0) - metric.get("disk_used", 0),
                usage_percent=disk_percent,
                threshold=threshold_value,
            )

            return [alert]

        return []

    def find_resource_exhaustion(
        self, metric_type: str, threshold: float, hours: int = 24
    ) -> List[PerformanceIssue]:
        """
        Generic method to find resource exhaustion issues.

        Args:
            metric_type: Type of metric (cpu, memory, disk)
            threshold: Threshold value
            hours: Time window to analyze

        Returns:
            List of PerformanceIssue objects (HighUsagePeriod or DiskAlert subclasses)
        """
        if metric_type == "cpu":
            # Return as List[PerformanceIssue] (HighUsagePeriod is a subclass)
            return list(self.find_high_cpu_periods(threshold=threshold, hours=hours))
        elif metric_type == "memory":
            # Return as List[PerformanceIssue] (HighUsagePeriod is a subclass)
            return list(self.find_high_memory_periods(threshold=threshold, hours=hours))
        elif metric_type == "disk":
            # Return as List[PerformanceIssue] (DiskAlert is a subclass)
            return list(self.find_disk_space_issues(threshold=threshold, hours=hours))
        else:
            return []

    # ============================================================================
    # Error Issue Detection
    # ============================================================================

    def find_error_spikes(
        self, hours: int = 24, spike_multiplier: Optional[float] = None
    ) -> List[ErrorSpike]:
        """
        Find spikes in error rates.

        Args:
            hours: Time window to analyze
            spike_multiplier: Multiplier for baseline to detect spikes (default: from config)

        Returns:
            List of ErrorSpike objects
        """
        # Ensure spike_multiplier is set to a concrete float value
        multiplier_value: float = spike_multiplier if spike_multiplier is not None else self.thresholds["error_spike_multiplier"]

        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        # Get error events
        events = self.store.get_log_events(
            start_time, end_time, source=None, level="ERROR", limit=10000
        )

        if len(events) < 10:  # Not enough data
            return []

        # Group errors by hour and by error type
        error_patterns = defaultdict(lambda: defaultdict(int))

        for event in events:
            hour = event["timestamp"] // 3600
            error_type = event.get("source", "unknown")
            error_patterns[error_type][hour] += 1

        spikes = []
        for error_type, hourly_counts in error_patterns.items():
            if len(hourly_counts) < 2:
                continue

            # Sort by hour to get chronological order
            sorted_hours = sorted(hourly_counts.keys())
            counts = [hourly_counts[h] for h in sorted_hours]
            avg_count = sum(counts[:-1]) / len(counts[:-1]) if len(counts) > 1 else 0
            latest_count = counts[-1]

            if avg_count > 0 and latest_count >= avg_count * multiplier_value:
                spike_factor = latest_count / avg_count if avg_count > 0 else 0

                # Find a sample message
                sample = next((e for e in events if e.get("source") == error_type), {})

                severity = min(100, 60 + int((spike_factor - multiplier_value) * 10))

                spike = ErrorSpike(
                    severity=severity,
                    title=f"Error Spike: {error_type}",
                    description=f"Error rate increased {spike_factor:.1f}x from baseline",
                    first_seen=min(hourly_counts.keys()) * 3600,
                    last_seen=max(hourly_counts.keys()) * 3600,
                    occurrence_count=latest_count,
                    affected_resources=[],
                    recommendations=[
                        f"Investigate recent changes to {error_type}",
                        "Review error logs for root cause",
                        "Check for resource constraints",
                    ],
                    error_type=error_type,
                    source=error_type,
                    sample_message=sample.get("message", ""),
                    baseline_count=int(avg_count),
                    spike_count=latest_count,
                    spike_factor=spike_factor,
                )
                spikes.append(spike)

        return sorted(spikes, key=lambda x: x.spike_factor, reverse=True)

    def find_recurring_errors(
        self, hours: int = 24, min_occurrences: int = 5
    ) -> List[RecurringError]:
        """
        Find errors that occur repeatedly.

        Args:
            hours: Time window to analyze
            min_occurrences: Minimum number of occurrences

        Returns:
            List of RecurringError objects
        """
        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        events = self.store.get_log_events(
            start_time, end_time, source=None, level="ERROR", limit=10000
        )

        # Group by error pattern (simplified - just by source and level)
        error_counts: Dict[str, int] = {}
        error_samples = {}

        for event in events:
            # Create a simple pattern signature
            pattern = f"{event.get('source', 'unknown')}:{event.get('action', 'error')}"
            error_counts[pattern] = error_counts.get(pattern, 0) + 1

            if pattern not in error_samples:
                error_samples[pattern] = event

        recurring = []
        for pattern, count in error_counts.items():
            if count >= min_occurrences:
                sample = error_samples[pattern]
                rate = count / hours

                severity = min(100, 50 + int(count / min_occurrences) * 5)

                error = RecurringError(
                    severity=severity,
                    title=f"Recurring Error: {pattern}",
                    description=f"Error occurred {count} times in {hours} hours",
                    first_seen=start_time,
                    last_seen=end_time,
                    occurrence_count=count,
                    affected_resources=[],
                    recommendations=[
                        "Identify and fix root cause",
                        "Add monitoring for this error pattern",
                        "Consider implementing retry logic or error handling",
                    ],
                    error_type=sample.get("source", "unknown"),
                    source=sample.get("source", "unknown"),
                    sample_message=sample.get("message", ""),
                    pattern_signature=pattern,
                    occurrences_per_hour=rate,
                )
                recurring.append(error)

        return sorted(recurring, key=lambda x: x.occurrence_count, reverse=True)

    def find_critical_errors(self, hours: int = 24) -> List[CriticalError]:
        """
        Find critical errors requiring immediate attention.

        Args:
            hours: Time window to analyze

        Returns:
            List of CriticalError objects
        """
        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        # Get traces with high severity
        traces = self.store.get_traces(
            start_time, end_time, source=None, min_severity=80, limit=100
        )

        critical = []
        for trace in traces:
            error = CriticalError(
                severity=trace.get("severity", 90),
                title=f"Critical Error: {trace.get('source', 'Unknown')}",
                description=trace.get("message", "Critical error detected"),
                first_seen=trace["timestamp"],
                last_seen=trace["timestamp"],
                occurrence_count=1,
                affected_resources=[],
                recommendations=[
                    "Immediate investigation required",
                    "Check system stability",
                    "Review error context and stacktrace",
                ],
                error_type=trace.get("error_category", "unknown"),
                error_category=trace.get("error_category", "unknown"),
                source=trace.get("source", "unknown"),
                sample_message=trace.get("message", ""),
                stacktrace=trace.get("stacktrace"),
                impact=trace.get("impact", "Unknown"),
            )
            critical.append(error)

        return sorted(critical, key=lambda x: x.severity, reverse=True)

    def find_database_issues(self, hours: int = 24) -> List[ErrorIssue]:
        """
        Find database-related issues.

        Args:
            hours: Time window to analyze

        Returns:
            List of ErrorIssue objects
        """
        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        # Get error patterns from database
        patterns_data = self.store.get_error_patterns(start_time, end_time)

        db_issues = []
        # Check patterns by category for database errors
        for category_pattern in patterns_data.get("by_category", []):
            if category_pattern.get("error_category") == "database":
                count = category_pattern.get("count", 0)
                issue = ErrorIssue(
                    severity=70,
                    title="Database Errors",
                    description=f"Database errors detected: {count} occurrences",
                    first_seen=start_time,
                    last_seen=end_time,
                    occurrence_count=count,
                    affected_resources=[],
                    recommendations=[
                        "Check database connectivity",
                        "Review database logs",
                        "Verify database configuration",
                        "Check for deadlocks or connection pool exhaustion",
                    ],
                    error_type="database",
                    error_category="database",
                    source="database",
                )
                db_issues.append(issue)

        # Also check for specific database error types
        for type_pattern in patterns_data.get("by_type", []):
            error_type = type_pattern.get("error_type", "")
            if error_type and "database" in error_type.lower():
                count = type_pattern.get("count", 0)
                issue = ErrorIssue(
                    severity=70,
                    title=f"Database Error: {error_type}",
                    description=f"Database errors detected: {count} occurrences",
                    first_seen=start_time,
                    last_seen=end_time,
                    occurrence_count=count,
                    affected_resources=[],
                    recommendations=[
                        "Check database connectivity",
                        "Review database logs",
                        "Verify database configuration",
                        "Check for deadlocks or connection pool exhaustion",
                    ],
                    error_type=error_type,
                    error_category="database",
                    source="database",
                )
                db_issues.append(issue)

        return db_issues

    # ============================================================================
    # Network Issue Detection
    # ============================================================================

    def find_connection_anomalies(self, hours: int = 24) -> List[ConnectionAnomaly]:
        """
        Find anomalous network connection behavior.

        Args:
            hours: Time window to analyze

        Returns:
            List of ConnectionAnomaly objects
        """
        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        metrics = self.store.get_network_metrics(start_time, end_time, limit=10000)

        if len(metrics) < 10:
            return []

        # Calculate baseline for connection counts
        connection_counts = [m.get("connections_established", 0) for m in metrics]
        avg_connections = sum(connection_counts) / len(connection_counts)
        std_dev = (
            sum((x - avg_connections) ** 2 for x in connection_counts)
            / len(connection_counts)
        ) ** 0.5

        anomalies = []
        for metric in metrics:
            connections = metric.get("connections_established", 0)
            deviation = abs(connections - avg_connections)

            # Anomaly if > 3 standard deviations
            if std_dev > 0 and deviation > 3 * std_dev:
                deviation_percent = (
                    (deviation / avg_connections * 100) if avg_connections > 0 else 0
                )

                anomaly = ConnectionAnomaly(
                    severity=min(100, 60 + int(deviation_percent / 10)),
                    title="Connection Count Anomaly",
                    description=f"Unusual connection count: {connections} (expected ~{avg_connections:.0f})",
                    first_seen=metric["timestamp"],
                    last_seen=metric["timestamp"],
                    occurrence_count=1,
                    affected_resources=[],
                    recommendations=[
                        "Investigate sudden connection changes",
                        "Check for connection leaks",
                        "Review network activity logs",
                    ],
                    connection_count=connections,
                    anomaly_type="connection_count",
                    expected_value=avg_connections,
                    actual_value=connections,
                    deviation_percent=deviation_percent,
                )
                anomalies.append(anomaly)

        return anomalies

    def find_network_errors(
        self, hours: int = 24, threshold: Optional[float] = None
    ) -> List[NetworkIssue]:
        """
        Find periods with high network errors.

        Args:
            hours: Time window to analyze
            threshold: Error rate threshold (default: from config)

        Returns:
            List of NetworkIssue objects
        """
        if threshold is None:
            threshold = self.thresholds["network_error_rate"]
        start_time = int(time.time()) - (hours * 3600)
        end_time = int(time.time())

        metrics = self.store.get_network_metrics(start_time, end_time, limit=10000)

        issues = []
        for metric in metrics:
            errors_in = metric.get("errors_in", 0)
            errors_out = metric.get("errors_out", 0)
            drops_in = metric.get("drops_in", 0)
            drops_out = metric.get("drops_out", 0)
            packets_recv = metric.get("packets_recv", 0)
            packets_sent = metric.get("packets_sent", 0)

            total_errors = errors_in + errors_out + drops_in + drops_out
            total_packets = packets_recv + packets_sent

            if total_packets > 0:
                error_rate = (total_errors / total_packets) * 100

                if error_rate >= threshold:
                    issue = NetworkIssue(
                        severity=min(100, 60 + int(error_rate)),
                        title="High Network Error Rate",
                        description=f"Network error rate at {error_rate:.2f}%",
                        first_seen=metric["timestamp"],
                        last_seen=metric["timestamp"],
                        occurrence_count=1,
                        affected_resources=[],
                        recommendations=[
                            "Check network hardware",
                            "Review network configuration",
                            "Investigate packet loss causes",
                        ],
                        connection_count=metric.get("connections_established", 0),
                        error_count=total_errors,
                        drop_count=drops_in + drops_out,
                        error_rate=error_rate,
                    )
                    issues.append(issue)

        return issues
