"""
Analysis engine for trend detection and system health assessment.
"""

import time
from typing import List, Dict, Any, Optional
from collections import defaultdict
from logly.storage.sqlite_store import SQLiteStore
from logly.query.models import (
    HealthReport,
    TrendReport,
    SecurityReport,
    ErrorTrendReport,
)
from logly.query.issue_detector import IssueDetector


class AnalysisEngine:
    """Analyzes trends and patterns in system data."""

    def __init__(self, store: SQLiteStore, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the analysis engine.

        Args:
            store: SQLiteStore instance for data access
            config: Configuration dictionary
        """
        self.store = store
        self.config = config or {}
        self.detector = IssueDetector(store, config)

    # ============================================================================
    # System Health Analysis
    # ============================================================================

    def analyze_system_health(self, hours: int = 24) -> HealthReport:
        """
        Analyze overall system health.

        Args:
            hours: Time window to analyze (default: 24)

        Returns:
            HealthReport object with comprehensive health assessment
        """
        timestamp = int(time.time())

        # Detect all types of issues
        security_issues = self._detect_security_issues(hours)
        performance_issues = self._detect_performance_issues(hours)
        error_issues = self._detect_error_issues(hours)
        network_issues = self._detect_network_issues(hours)

        # Combine all issues
        all_issues = (
            security_issues + performance_issues + error_issues + network_issues
        )

        # Count by severity
        critical_count = sum(1 for issue in all_issues if issue.severity >= 81)
        high_count = sum(1 for issue in all_issues if 61 <= issue.severity <= 80)
        medium_count = sum(1 for issue in all_issues if 31 <= issue.severity <= 60)
        low_count = sum(1 for issue in all_issues if issue.severity <= 30)

        # Calculate component scores (100 = perfect, 0 = critical)
        security_score = self._calculate_component_score(security_issues)
        performance_score = self._calculate_component_score(performance_issues)
        error_score = self._calculate_component_score(error_issues)
        network_score = self._calculate_component_score(network_issues)

        # Calculate overall health score (weighted average)
        health_score = int(
            (security_score * 0.30)
            + (performance_score * 0.25)
            + (error_score * 0.25)
            + (network_score * 0.20)
        )

        # Determine status
        if health_score >= 80:
            status = "healthy"
        elif health_score >= 50:
            status = "degraded"
        else:
            status = "critical"

        # Get top 5 issues by severity
        top_issues = sorted(all_issues, key=lambda x: x.severity, reverse=True)[:5]

        # Generate recommendations
        recommendations = self._generate_health_recommendations(
            all_issues, health_score, status
        )

        return HealthReport(
            timestamp=timestamp,
            health_score=health_score,
            status=status,
            time_window=hours,
            security_score=security_score,
            performance_score=performance_score,
            error_score=error_score,
            network_score=network_score,
            total_issues=len(all_issues),
            critical_issues=critical_count,
            high_issues=high_count,
            medium_issues=medium_count,
            low_issues=low_count,
            top_issues=top_issues,
            recommendations=recommendations,
        )

    def _detect_security_issues(self, hours: int) -> List:
        """Detect all security issues."""
        issues = []
        issues.extend(self.detector.find_brute_force_attempts(hours=hours))
        issues.extend(self.detector.find_suspicious_ips())
        issues.extend(self.detector.find_unauthorized_access_attempts(hours=hours))
        issues.extend(self.detector.find_banned_ips(hours=hours))
        return issues

    def _detect_performance_issues(self, hours: int) -> List:
        """Detect all performance issues."""
        issues = []
        issues.extend(self.detector.find_high_cpu_periods(hours=hours))
        issues.extend(self.detector.find_high_memory_periods(hours=hours))
        issues.extend(self.detector.find_disk_space_issues(hours=1))
        return issues

    def _detect_error_issues(self, hours: int) -> List:
        """Detect all error-related issues."""
        issues = []
        issues.extend(self.detector.find_error_spikes(hours=hours))
        issues.extend(self.detector.find_recurring_errors(hours=hours))
        issues.extend(self.detector.find_critical_errors(hours=hours))
        issues.extend(self.detector.find_database_issues(hours=hours))
        return issues

    def _detect_network_issues(self, hours: int) -> List:
        """Detect all network-related issues."""
        issues = []
        issues.extend(self.detector.find_connection_anomalies(hours=hours))
        issues.extend(self.detector.find_network_errors(hours=hours))
        return issues

    def _calculate_component_score(self, issues: List) -> int:
        """
        Calculate a component health score from issues.

        Args:
            issues: List of Issue objects

        Returns:
            Score from 0-100 (100 = perfect health)
        """
        if not issues:
            return 100

        # Weight by severity
        total_impact = sum(issue.severity for issue in issues)

        # Normalize: each critical issue (100) reduces score by 20 points
        # So 5 critical issues would bring score to 0
        score = max(0, 100 - total_impact // 5)

        return score

    def _generate_health_recommendations(
        self, issues: List, health_score: int, status: str
    ) -> List[str]:
        """Generate actionable health recommendations."""
        recommendations = []

        if status == "critical":
            recommendations.append(
                "⚠️  URGENT: System health is critical - immediate action required"
            )

        # Count by type
        security_count = sum(1 for i in issues if i.issue_type == "security")
        performance_count = sum(1 for i in issues if i.issue_type == "performance")
        error_count = sum(1 for i in issues if i.issue_type == "error")

        if security_count > 0:
            recommendations.append(
                f"Address {security_count} security issue(s) - review authentication and access controls"
            )

        if performance_count > 0:
            recommendations.append(
                f"Investigate {performance_count} performance issue(s) - check resource utilization"
            )

        if error_count > 5:
            recommendations.append(
                f"High error rate detected ({error_count} issues) - review application logs"
            )

        if health_score < 80:
            recommendations.append(
                "Schedule maintenance window to address system issues"
            )

        if not recommendations:
            recommendations.append("System is healthy - continue monitoring")

        return recommendations

    # ============================================================================
    # Security Analysis
    # ============================================================================

    def analyze_security_posture(self, hours: int = 24) -> SecurityReport:
        """
        Analyze security posture and threats.

        Args:
            hours: Time window to analyze (default: 24)

        Returns:
            SecurityReport object with security assessment
        """
        timestamp = int(time.time())
        start_time = timestamp - (hours * 3600)

        # Get all security metrics
        high_threat_ips = self.store.get_high_threat_ips(threshold=70)

        # Get failed login events
        events = self.store.get_log_events(
            start_time, timestamp, source=None, level=None, limit=10000
        )

        failed_logins = [e for e in events if e.get("action") == "failed_login"]
        bans = [e for e in events if e.get("action") == "ban"]

        # Detect security issues
        brute_force = self.detector.find_brute_force_attempts(hours=hours)
        suspicious_ips = self.detector.find_suspicious_ips()

        # Build top threat IPs list
        top_threat_ips = []
        for ip_data in sorted(
            high_threat_ips, key=lambda x: x.get("threat_score", 0), reverse=True
        )[:5]:
            top_threat_ips.append(
                {
                    "ip_address": ip_data["ip_address"],
                    "threat_score": ip_data["threat_score"],
                    "failed_logins": ip_data.get("failed_login_count", 0),
                    "bans": ip_data.get("ban_count", 0),
                }
            )

        # Calculate risk score (0-100, lower is better)
        risk_score = min(
            100,
            (
                len(high_threat_ips) * 10
                + len(brute_force) * 15
                + min(len(failed_logins) // 10, 30)
            ),
        )

        # Determine security posture
        if risk_score < 20:
            posture = "good"
        elif risk_score < 50:
            posture = "fair"
        elif risk_score < 80:
            posture = "poor"
        else:
            posture = "critical"

        # Generate recommendations
        recommendations = []
        if len(brute_force) > 0:
            recommendations.append(
                "Implement rate limiting to prevent brute force attacks"
            )
        if len(high_threat_ips) > 5:
            recommendations.append("Review and update IP blacklist")
        if len(failed_logins) > 50:
            recommendations.append("Investigate spike in failed login attempts")
        if posture in ["poor", "critical"]:
            recommendations.append("Enable two-factor authentication")
            recommendations.append("Review firewall rules and access controls")

        if not recommendations:
            recommendations.append(
                "Security posture is good - maintain current monitoring"
            )

        return SecurityReport(
            timestamp=timestamp,
            time_window=hours,
            total_threats=len(brute_force) + len(suspicious_ips),
            high_threat_ips=len(high_threat_ips),
            failed_login_attempts=len(failed_logins),
            successful_bans=len(bans),
            top_threat_ips=top_threat_ips,
            recent_attacks=list(brute_force + suspicious_ips),  # type: ignore[arg-type]
            security_posture=posture,
            risk_score=risk_score,
            recommendations=recommendations,
        )

    # ============================================================================
    # Error Trend Analysis
    # ============================================================================

    def analyze_error_trends(self, days: int = 7) -> ErrorTrendReport:
        """
        Analyze error trends over time.

        Args:
            days: Number of days to analyze (default: 7)

        Returns:
            ErrorTrendReport object with trend analysis
        """
        timestamp = int(time.time())
        start_time = timestamp - (days * 86400)

        # Get all error events
        events = self.store.get_log_events(
            start_time, timestamp, source=None, level="ERROR", limit=10000
        )

        # Get error patterns
        patterns = self.store.get_error_patterns(start_time, timestamp)

        # Calculate metrics
        total_errors = len(events)
        error_rate = total_errors / (days * 24) if days > 0 else 0

        # Count unique error types
        unique_types = len(set(e.get("source", "unknown") for e in events))

        # Group by category
        errors_by_category = defaultdict(int)
        errors_by_source = defaultdict(int)

        for event in events:
            errors_by_source[event.get("source", "unknown")] += 1

        for pattern in patterns:
            category = pattern.get("error_category", "unknown") if isinstance(pattern, dict) else "unknown"
            count = pattern.get("error_count", 1) if isinstance(pattern, dict) else 1
            errors_by_category[category] += count

        # Use detector to find top errors
        recurring = self.detector.find_recurring_errors(
            hours=days * 24, min_occurrences=3
        )
        spikes = self.detector.find_error_spikes(hours=days * 24)
        critical = self.detector.find_critical_errors(hours=days * 24)

        top_errors = (recurring + spikes + critical)[:10]

        # Determine trend
        # Compare first half vs second half of time period
        midpoint = start_time + ((timestamp - start_time) // 2)
        first_half = [e for e in events if e["timestamp"] < midpoint]
        second_half = [e for e in events if e["timestamp"] >= midpoint]

        if len(first_half) == 0:
            trend = "stable"
        else:
            ratio = len(second_half) / len(first_half)
            if ratio > 1.2:
                trend = "worsening"
            elif ratio < 0.8:
                trend = "improving"
            else:
                trend = "stable"

        # Generate recommendations
        recommendations = []
        if trend == "worsening":
            recommendations.append(
                "Error rate is increasing - investigate recent changes"
            )
        if total_errors > 100:
            recommendations.append("High error volume - prioritize error resolution")
        if len(recurring) > 0:
            recommendations.append(f"Fix {len(recurring)} recurring error pattern(s)")
        if len(critical) > 0:
            recommendations.append(
                f"Address {len(critical)} critical error(s) immediately"
            )

        # Specific category recommendations
        if errors_by_category.get("database", 0) > 10:
            recommendations.append(
                "Database errors detected - check connection pool and queries"
            )
        if errors_by_category.get("resource", 0) > 10:
            recommendations.append(
                "Resource errors detected - review memory and disk usage"
            )

        if not recommendations:
            recommendations.append("Error rate is acceptable - continue monitoring")

        return ErrorTrendReport(
            timestamp=timestamp,
            time_period=days,
            total_errors=total_errors,
            error_rate=error_rate,
            unique_error_types=unique_types,
            errors_by_category=dict(errors_by_category),
            errors_by_source=dict(errors_by_source),
            errors_by_severity={},  # Could be enhanced
            top_errors=list(top_errors),  # type: ignore[arg-type]
            trend=trend,
            recommendations=recommendations,
        )

    # ============================================================================
    # Resource Usage Trend Analysis
    # ============================================================================

    def get_resource_usage_trends(self, days: int = 7) -> Dict[str, TrendReport]:
        """
        Analyze resource usage trends.

        Args:
            days: Number of days to analyze (default: 7)

        Returns:
            Dictionary mapping metric names to TrendReport objects
        """
        timestamp = int(time.time())
        start_time = timestamp - (days * 86400)

        # Get system metrics
        metrics = self.store.get_system_metrics(start_time, timestamp, limit=10000)

        if not metrics:
            return {}

        # Analyze key metrics
        trends = {}

        for metric_name in ["cpu_percent", "memory_percent", "disk_percent"]:
            trend = self._analyze_metric_trend(metrics, metric_name, days)
            trends[metric_name] = trend

        return trends

    def _analyze_metric_trend(
        self, metrics: List[Dict[str, Any]], metric_name: str, days: int
    ) -> TrendReport:
        """Analyze trend for a specific metric."""
        values = [m.get(metric_name, 0) for m in metrics]

        if not values:
            return TrendReport(metric_name=metric_name, time_period=days, data_points=0)

        # Calculate statistics
        min_val = min(values)
        max_val = max(values)
        avg_val = sum(values) / len(values)

        # Calculate median
        sorted_values = sorted(values)
        n = len(sorted_values)
        if n % 2 == 0:
            median_val = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        else:
            median_val = sorted_values[n // 2]

        # Calculate standard deviation
        variance = sum((x - avg_val) ** 2 for x in values) / len(values)
        std_dev = variance**0.5

        # Determine trend direction using simple linear regression
        trend_direction, trend_strength = self._calculate_trend(values)

        # Find anomalies (values > 2 std deviations from mean)
        anomalies = []
        for i, value in enumerate(values):
            if abs(value - avg_val) > 2 * std_dev:
                anomalies.append(
                    {
                        "index": i,
                        "value": value,
                        "deviation": abs(value - avg_val) / std_dev
                        if std_dev > 0
                        else 0,
                        "timestamp": metrics[i]["timestamp"],
                    }
                )

        return TrendReport(
            metric_name=metric_name,
            time_period=days,
            data_points=len(values),
            min_value=min_val,
            max_value=max_val,
            avg_value=avg_val,
            median_value=median_val,
            std_deviation=std_dev,
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            anomaly_count=len(anomalies),
            anomalies=anomalies[:10],  # Limit to top 10
        )

    def _calculate_trend(self, values: List[float]) -> tuple:
        """
        Calculate trend direction and strength using simple linear regression.

        Returns:
            Tuple of (direction, strength) where direction is "increasing", "decreasing", or "stable"
            and strength is a value between 0 and 1
        """
        if len(values) < 2:
            return "stable", 0.0

        n = len(values)
        x_values = list(range(n))

        # Calculate slope using least squares
        x_mean = sum(x_values) / n
        y_mean = sum(values) / n

        numerator = sum((x_values[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x_values[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return "stable", 0.0

        slope = numerator / denominator

        # Calculate correlation coefficient (R) for trend strength
        y_variance = sum((v - y_mean) ** 2 for v in values)
        if y_variance == 0:
            return "stable", 0.0

        predicted = [slope * x + (y_mean - slope * x_mean) for x in x_values]
        ss_res = sum((values[i] - predicted[i]) ** 2 for i in range(n))
        ss_tot = sum((values[i] - y_mean) ** 2 for i in range(n))

        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        strength = abs(r_squared) ** 0.5  # Convert to correlation coefficient

        # Determine direction
        if abs(slope) < 0.01:  # Threshold for "stable"
            direction = "stable"
        elif slope > 0:
            direction = "increasing"
        else:
            direction = "decreasing"

        return direction, min(1.0, strength)

    # ============================================================================
    # Top Error Sources
    # ============================================================================

    def get_top_error_sources(
        self, hours: int = 24, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get the top error sources by count.

        Args:
            hours: Time window to analyze
            limit: Maximum number of sources to return

        Returns:
            List of dictionaries with source name and error count
        """
        timestamp = int(time.time())
        start_time = timestamp - (hours * 3600)

        events = self.store.get_log_events(
            start_time, timestamp, source=None, level="ERROR", limit=10000
        )

        # Count by source
        source_counts = defaultdict(int)
        for event in events:
            source = event.get("source", "unknown")
            source_counts[source] += 1

        # Sort by count
        sorted_sources = sorted(
            source_counts.items(), key=lambda x: x[1], reverse=True
        )[:limit]

        return [
            {"source": source, "error_count": count} for source, count in sorted_sources
        ]

    # ============================================================================
    # Simplified Analysis Methods
    # ============================================================================

    def analyze_performance(
        self, start_time: int, end_time: int
    ) -> Dict[str, Any]:
        """
        Analyze performance metrics for the specified time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            Dictionary containing performance analysis
        """
        hours = max(1, (end_time - start_time) // 3600)

        # Get system metrics
        metrics = self.store.get_system_metrics(start_time, end_time, limit=10000)

        if not metrics:
            return {
                "status": "no_data",
                "message": "No performance metrics available",
            }

        # Calculate statistics
        cpu_values = [m.get("cpu_percent", 0) for m in metrics if m.get("cpu_percent")]
        memory_values = [m.get("memory_percent", 0) for m in metrics if m.get("memory_percent")]
        disk_values = [m.get("disk_percent", 0) for m in metrics if m.get("disk_percent")]

        performance_analysis = {
            "time_range": {"start": start_time, "end": end_time, "hours": hours},
            "cpu": {
                "avg": sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                "max": max(cpu_values) if cpu_values else 0,
                "min": min(cpu_values) if cpu_values else 0,
            },
            "memory": {
                "avg": sum(memory_values) / len(memory_values) if memory_values else 0,
                "max": max(memory_values) if memory_values else 0,
                "min": min(memory_values) if memory_values else 0,
            },
            "disk": {
                "avg": sum(disk_values) / len(disk_values) if disk_values else 0,
                "max": max(disk_values) if disk_values else 0,
                "min": min(disk_values) if disk_values else 0,
            },
        }

        # Detect performance issues
        issues = []
        issues.extend(self.detector.find_high_cpu_periods(hours=hours))
        issues.extend(self.detector.find_high_memory_periods(hours=hours))
        issues.extend(self.detector.find_disk_space_issues(hours=hours))

        performance_analysis["issues"] = [
            {
                "type": type(issue).__name__,
                "severity": issue.severity,
                "title": issue.title,
                "description": issue.description,
            }
            for issue in issues
        ]

        performance_analysis["issue_count"] = len(issues)
        performance_analysis["status"] = "critical" if any(i.severity > 80 for i in issues) else "warning" if issues else "healthy"

        return performance_analysis

    def analyze_security(
        self, start_time: int, end_time: int
    ) -> Dict[str, Any]:
        """
        Analyze security events for the specified time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            Dictionary containing security analysis
        """
        hours = max(1, (end_time - start_time) // 3600)

        # Get security-related events
        events = self.store.get_log_events(start_time, end_time, limit=10000)

        # Count different types of security events
        failed_logins = sum(1 for e in events if e.get("action") == "failed_login")
        bans = sum(1 for e in events if e.get("action") == "ban")
        errors = sum(1 for e in events if e.get("level") == "ERROR")

        # Get unique IPs involved
        ip_addresses = set(e.get("ip_address") for e in events if e.get("ip_address"))

        security_analysis = {
            "time_range": {"start": start_time, "end": end_time, "hours": hours},
            "event_counts": {
                "total": len(events),
                "failed_logins": failed_logins,
                "bans": bans,
                "errors": errors,
            },
            "unique_ips": list(ip_addresses),
            "unique_ip_count": len(ip_addresses),
        }

        # Detect security issues
        issues = []
        issues.extend(self.detector.find_brute_force_attempts(hours=hours))
        issues.extend(self.detector.find_suspicious_ips())
        issues.extend(self.detector.find_banned_ips(hours=hours))

        security_analysis["issues"] = [
            {
                "type": type(issue).__name__,
                "severity": issue.severity,
                "title": issue.title,
                "description": issue.description,
            }
            for issue in issues
        ]

        security_analysis["issue_count"] = len(issues)
        security_analysis["status"] = "critical" if any(i.severity > 80 for i in issues) else "warning" if issues else "healthy"

        return security_analysis
