"""
Data models for query results and issue detection.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum


class IssueType(Enum):
    """Types of issues that can be detected."""

    SECURITY = "security"
    PERFORMANCE = "performance"
    ERROR = "error"
    NETWORK = "network"


class IssueSeverity(Enum):
    """Severity levels for issues."""

    LOW = (0, 30)
    MEDIUM = (31, 60)
    HIGH = (61, 80)
    CRITICAL = (81, 100)

    def __init__(self, min_score, max_score):
        self.min_score = min_score
        self.max_score = max_score

    @classmethod
    def from_score(cls, score: int):
        """Get severity level from numeric score."""
        for severity in cls:
            if severity.min_score <= score <= severity.max_score:
                return severity
        return cls.LOW


@dataclass
class Issue:
    """Base class for all detected issues."""

    severity: int  # 0-100
    title: str
    description: str
    first_seen: int  # Unix timestamp
    last_seen: int  # Unix timestamp
    occurrence_count: int
    issue_type: str = ""  # Set by subclasses in __post_init__
    affected_resources: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def severity_level(self) -> IssueSeverity:
        """Get the severity level enum."""
        return IssueSeverity.from_score(self.severity)

    @property
    def duration_seconds(self) -> int:
        """Get the duration of the issue in seconds."""
        return self.last_seen - self.first_seen

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "issue_type": self.issue_type,
            "severity": self.severity,
            "severity_level": self.severity_level.name,
            "title": self.title,
            "description": self.description,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "occurrence_count": self.occurrence_count,
            "duration_seconds": self.duration_seconds,
            "affected_resources": self.affected_resources,
            "recommendations": self.recommendations,
            "metadata": self.metadata,
        }


@dataclass
class SecurityIssue(Issue):
    """Security-related issue."""

    ip_address: Optional[str] = None
    target_user: Optional[str] = None
    attack_type: Optional[str] = None
    threat_score: Optional[int] = None

    def __post_init__(self):
        self.issue_type = IssueType.SECURITY.value


@dataclass
class BruteForceAlert(SecurityIssue):
    """Brute force attack detection."""

    attempt_count: int = 0
    time_span: int = 0  # seconds
    unique_users: int = 1

    def __post_init__(self):
        super().__post_init__()
        self.attack_type = "brute_force"


@dataclass
class IPThreat(SecurityIssue):
    """High-threat IP address."""

    failed_login_count: int = 0
    ban_count: int = 0
    first_activity: Optional[int] = None
    last_activity: Optional[int] = None

    def __post_init__(self):
        super().__post_init__()
        self.attack_type = "high_threat_ip"


@dataclass
class PerformanceIssue(Issue):
    """Performance-related issue."""

    metric_name: str = ""
    current_value: float = 0.0
    threshold: float = 0.0
    peak_value: float = 0.0
    avg_value: float = 0.0

    def __post_init__(self):
        self.issue_type = IssueType.PERFORMANCE.value


@dataclass
class HighUsagePeriod(PerformanceIssue):
    """Period of high resource usage."""

    resource_type: str = ""  # cpu, memory, disk
    sustained_duration: int = 0  # seconds

    def __post_init__(self):
        super().__post_init__()


@dataclass
class DiskAlert(PerformanceIssue):
    """Disk space issue."""

    disk_total: int = 0
    disk_used: int = 0
    disk_available: int = 0
    usage_percent: float = 0.0

    def __post_init__(self):
        super().__post_init__()
        self.metric_name = "disk_space"
        self.resource_type = "disk"


@dataclass
class ErrorIssue(Issue):
    """Error-related issue."""

    error_type: str = ""
    error_category: str = ""
    source: str = ""
    sample_message: str = ""
    stacktrace: Optional[str] = None

    def __post_init__(self):
        self.issue_type = IssueType.ERROR.value


@dataclass
class ErrorSpike(ErrorIssue):
    """Spike in error rate."""

    baseline_count: int = 0
    spike_count: int = 0
    spike_factor: float = 0.0

    def __post_init__(self):
        super().__post_init__()


@dataclass
class RecurringError(ErrorIssue):
    """Error that occurs repeatedly."""

    pattern_signature: str = ""
    occurrences_per_hour: float = 0.0

    def __post_init__(self):
        super().__post_init__()


@dataclass
class CriticalError(ErrorIssue):
    """Critical error requiring immediate attention."""

    impact: str = ""
    process_info: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        super().__post_init__()
        self.severity = max(self.severity, 81)  # Critical errors are at least 81


@dataclass
class NetworkIssue(Issue):
    """Network-related issue."""

    connection_count: int = 0
    error_count: int = 0
    drop_count: int = 0
    error_rate: float = 0.0

    def __post_init__(self):
        self.issue_type = IssueType.NETWORK.value


@dataclass
class ConnectionAnomaly(NetworkIssue):
    """Anomalous network connection behavior."""

    anomaly_type: str = ""
    expected_value: float = 0.0
    actual_value: float = 0.0
    deviation_percent: float = 0.0

    def __post_init__(self):
        super().__post_init__()


@dataclass
class HealthReport:
    """Overall system health report."""

    timestamp: int
    health_score: int  # 0-100
    status: str  # healthy, degraded, critical
    time_window: int  # hours analyzed

    # Component scores
    security_score: int = 100
    performance_score: int = 100
    error_score: int = 100
    network_score: int = 100

    # Issue counts
    total_issues: int = 0
    critical_issues: int = 0
    high_issues: int = 0
    medium_issues: int = 0
    low_issues: int = 0

    # Top issues
    top_issues: List[Issue] = field(default_factory=list)

    # Recommendations
    recommendations: List[str] = field(default_factory=list)

    @property
    def status_from_score(self) -> str:
        """Determine status from health score."""
        if self.health_score >= 80:
            return "healthy"
        elif self.health_score >= 50:
            return "degraded"
        else:
            return "critical"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "health_score": self.health_score,
            "status": self.status or self.status_from_score,
            "time_window": self.time_window,
            "component_scores": {
                "security": self.security_score,
                "performance": self.performance_score,
                "error": self.error_score,
                "network": self.network_score,
            },
            "issue_counts": {
                "total": self.total_issues,
                "critical": self.critical_issues,
                "high": self.high_issues,
                "medium": self.medium_issues,
                "low": self.low_issues,
            },
            "top_issues": [issue.to_dict() for issue in self.top_issues],
            "recommendations": self.recommendations,
        }


@dataclass
class TrendReport:
    """Trend analysis report."""

    metric_name: str
    time_period: int  # days
    data_points: int

    # Statistical values
    min_value: float = 0.0
    max_value: float = 0.0
    avg_value: float = 0.0
    median_value: float = 0.0
    std_deviation: float = 0.0

    # Trend indicators
    trend_direction: str = "stable"  # increasing, decreasing, stable
    trend_strength: float = 0.0  # 0-1

    # Anomalies
    anomaly_count: int = 0
    anomalies: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "metric_name": self.metric_name,
            "time_period": self.time_period,
            "data_points": self.data_points,
            "statistics": {
                "min": self.min_value,
                "max": self.max_value,
                "avg": self.avg_value,
                "median": self.median_value,
                "std_deviation": self.std_deviation,
            },
            "trend": {
                "direction": self.trend_direction,
                "strength": self.trend_strength,
            },
            "anomalies": {"count": self.anomaly_count, "details": self.anomalies},
        }


@dataclass
class SecurityReport:
    """Security posture analysis."""

    timestamp: int
    time_window: int  # hours

    # Threat metrics
    total_threats: int = 0
    high_threat_ips: int = 0
    failed_login_attempts: int = 0
    successful_bans: int = 0

    # Top threats
    top_threat_ips: List[Dict[str, Any]] = field(default_factory=list)
    recent_attacks: List[SecurityIssue] = field(default_factory=list)

    # Overall posture
    security_posture: str = "good"  # good, fair, poor, critical
    risk_score: int = 0  # 0-100, lower is better

    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "time_window": self.time_window,
            "threat_metrics": {
                "total_threats": self.total_threats,
                "high_threat_ips": self.high_threat_ips,
                "failed_login_attempts": self.failed_login_attempts,
                "successful_bans": self.successful_bans,
            },
            "top_threat_ips": self.top_threat_ips,
            "recent_attacks": [attack.to_dict() for attack in self.recent_attacks],
            "security_posture": self.security_posture,
            "risk_score": self.risk_score,
            "recommendations": self.recommendations,
        }


@dataclass
class ErrorTrendReport:
    """Error trend analysis."""

    timestamp: int
    time_period: int  # days

    # Error metrics
    total_errors: int = 0
    error_rate: float = 0.0  # errors per hour
    unique_error_types: int = 0

    # Breakdown by category
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_source: Dict[str, int] = field(default_factory=dict)
    errors_by_severity: Dict[str, int] = field(default_factory=dict)

    # Top errors
    top_errors: List[ErrorIssue] = field(default_factory=list)

    # Trend indicators
    trend: str = "stable"  # improving, worsening, stable

    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "time_period": self.time_period,
            "error_metrics": {
                "total_errors": self.total_errors,
                "error_rate": self.error_rate,
                "unique_error_types": self.unique_error_types,
            },
            "breakdown": {
                "by_category": self.errors_by_category,
                "by_source": self.errors_by_source,
                "by_severity": self.errors_by_severity,
            },
            "top_errors": [error.to_dict() for error in self.top_errors],
            "trend": self.trend,
            "recommendations": self.recommendations,
        }
