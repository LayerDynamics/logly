"""
Query module for finding issues and problems in logly data.

This module provides high-level interfaces for:
- Detecting security, performance, error, and network issues
- Building complex queries with a fluent interface
- Analyzing trends and system health
- Generating reports and recommendations

Example usage:
    from logly.query import QueryBuilder, IssueDetector, AnalysisEngine
    from logly.storage.sqlite_store import SQLiteStore
    from logly.config import Config

    # Initialize
    config = Config()
    store = SQLiteStore(config.get_database_config()['path'])

    # Use QueryBuilder for fluent queries
    query = QueryBuilder(store)
    recent_errors = query.events().in_last_hours(24).errors_only().all()
    high_threat_ips = query.ips().high_threat().sort_by_threat()

    # Use IssueDetector to find problems
    detector = IssueDetector(store, config.get('query.thresholds', {}))
    brute_force = detector.find_brute_force_attempts(hours=24)
    high_cpu = detector.find_high_cpu_periods(hours=24)

    # Use AnalysisEngine for comprehensive analysis
    engine = AnalysisEngine(store, config.get('query', {}))
    health = engine.analyze_system_health(hours=24)
    security = engine.analyze_security_posture(hours=24)
"""

from logly.query.models import (
    # Issue types
    Issue,
    SecurityIssue,
    BruteForceAlert,
    IPThreat,
    PerformanceIssue,
    HighUsagePeriod,
    DiskAlert,
    ErrorIssue,
    ErrorSpike,
    RecurringError,
    CriticalError,
    NetworkIssue,
    ConnectionAnomaly,
    # Reports
    HealthReport,
    SecurityReport,
    ErrorTrendReport,
    TrendReport,
    # Enums
    IssueType,
    IssueSeverity,
)

from logly.query.issue_detector import IssueDetector
from logly.query.query_builder import QueryBuilder
from logly.query.analysis_engine import AnalysisEngine

__all__ = [
    # Core classes
    "QueryBuilder",
    "IssueDetector",
    "AnalysisEngine",
    # Issue models
    "Issue",
    "SecurityIssue",
    "BruteForceAlert",
    "IPThreat",
    "PerformanceIssue",
    "HighUsagePeriod",
    "DiskAlert",
    "ErrorIssue",
    "ErrorSpike",
    "RecurringError",
    "CriticalError",
    "NetworkIssue",
    "ConnectionAnomaly",
    # Report models
    "HealthReport",
    "SecurityReport",
    "ErrorTrendReport",
    "TrendReport",
    # Enums
    "IssueType",
    "IssueSeverity",
]
