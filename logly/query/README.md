# Query

## What It Does

The query module provides a high-level interface for querying stored data, detecting issues, and analyzing system health. It includes a fluent query builder API for data retrieval, an issue detection engine for identifying problems, and an analysis engine for trend assessment and reporting.

**Core capabilities:**

1. **Query Building** - Fluent API for constructing queries across events, metrics, traces, errors, and IP reputation data
2. **Issue Detection** - Automated detection of security threats, performance problems, errors, and network anomalies
3. **Health Analysis** - Comprehensive system health assessment with scoring and recommendations
4. **Trend Analysis** - Statistical analysis of metrics over time with anomaly detection
5. **Reporting** - Structured reports for security posture, error trends, and resource usage

**Key features:**

- Chainable query methods for intuitive data exploration
- Configurable thresholds for issue detection
- Multi-dimensional health scoring (security, performance, errors, network)
- Statistical trend analysis with linear regression
- Actionable recommendations for detected issues
- Severity classification (low, medium, high, critical)

### Detailed Breakdown

*models*:
Defines dataclasses for query results, issues, and reports with enums and serialization methods. Key models include:

- **IssueType** - Enum for issue categories (SECURITY, PERFORMANCE, ERROR, NETWORK).
- **IssueSeverity** - Enum mapping severity ranges (LOW: 0-30, MEDIUM: 31-60, HIGH: 61-80, CRITICAL: 81-100) with `from_score()` factory.
- **Issue** - Base issue class with type, severity (0-100), title, description, timestamps (first_seen, last_seen), occurrence_count, affected_resources, recommendations, and metadata. Properties: `severity_level` (returns enum), `duration_seconds`. Method: `to_dict()`.
- **SecurityIssue** - Security issue with optional ip_address, target_user, attack_type, threat_score. Subtypes: `BruteForceAlert` (attempt_count, time_span, unique_users), `IPThreat` (failed_login_count, ban_count, first/last_activity).
- **PerformanceIssue** - Performance issue with metric_name, current_value, threshold, peak_value, avg_value. Subtypes: `HighUsagePeriod` (resource_type, sustained_duration), `DiskAlert` (disk_total/used/available, usage_percent).
- **ErrorIssue** - Error issue with error_type, error_category, source, sample_message, optional stacktrace. Subtypes: `ErrorSpike` (baseline_count, spike_count, spike_factor), `RecurringError` (pattern_signature, occurrences_per_hour), `CriticalError` (impact, process_info, auto-sets severity >= 81).
- **NetworkIssue** - Network issue with connection_count, error_count, drop_count, error_rate. Subtypes: `ConnectionAnomaly` (anomaly_type, expected_value, actual_value, deviation_percent).
- **HealthReport** - Overall system health with timestamp, health_score (0-100), status (healthy/degraded/critical), time_window, component scores (security/performance/error/network), issue counts by severity, top_issues list, recommendations. Property: `status_from_score`. Method: `to_dict()`.
- **TrendReport** - Metric trend analysis with metric_name, time_period (days), data_points, statistics (min/max/avg/median/std_deviation), trend indicators (direction: increasing/decreasing/stable, strength: 0-1), anomaly_count, anomalies list. Method: `to_dict()`.
- **SecurityReport** - Security posture with timestamp, time_window, threat metrics (total_threats, high_threat_ips, failed_login_attempts, successful_bans), top_threat_ips list, recent_attacks list, security_posture (good/fair/poor/critical), risk_score (0-100, lower is better), recommendations. Method: `to_dict()`.
- **ErrorTrendReport** - Error trend analysis with timestamp, time_period (days), error metrics (total_errors, error_rate per hour, unique_error_types), breakdowns (by_category, by_source, by_severity), top_errors list, trend (improving/worsening/stable), recommendations. Method: `to_dict()`.

All issue types inherit from base classes and auto-set their issue_type via `__post_init__()`. All report types include `to_dict()` for JSON serialization.

*query_builder*:
Fluent query builder with chainable methods for constructing database queries. Core classes:

- **BaseQuery** - Base class with common time filtering: `in_last_hours(hours)`, `in_last_days(days)`, `between(start_time, end_time)`, `limit(count)`. Private method `_get_time_range()` defaults to last 24 hours if not specified.
- **EventQuery** - Queries log events with filters: `with_level(level)` (INFO/WARNING/ERROR), `by_source(source)` (fail2ban/syslog/auth), shortcuts `errors_only()`, `warnings_only()`. Execution: `all()` returns list, `count()` returns int, `first()` returns single event or None.
- **MetricQuery** - Queries system/network metrics with type selection: `system()`, `network()`. Execution: `all()` returns list, `latest()` returns most recent, aggregates: `avg(field)`, `max(field)`, `min(field)`.
- **TraceQuery** - Queries event traces with filters: `by_source(source)`, `with_severity(min_severity)`, shortcuts `critical_only()` (80+), `high_severity()` (60+). Execution: `all()`, `count()`.
- **ErrorQuery** - Queries error patterns with category filter: `by_category(category)`, shortcuts `database_errors()`, `resource_errors()`, `network_errors()`. Execution: `all()`, `count()`, `by_type()` (groups by error type).
- **IPQuery** - Queries IP reputation with filters: `with_threat_above(threshold)`, `high_threat()` (70+), `for_ip(ip_address)` (specific IP lookup). Execution: `all()`, `count()`, sorting: `sort_by_threat()`, `sort_by_activity()`.
- **QueryBuilder** - Main entry point with factory methods: `events()` returns EventQuery, `metrics()` returns MetricQuery, `traces()` returns TraceQuery, `errors()` returns ErrorQuery, `ips()` returns IPQuery. Convenience methods: `recent_errors(hours)` (ERROR level events), `system_health_snapshot()` (latest system+network metrics), `threat_summary(hours)` (high-threat IPs and failed logins with counts and top 5 list).

Example usage: `query.events().in_last_hours(24).with_level("ERROR").by_source("django").all()` or `query.metrics().system().in_last_days(7).avg("cpu_percent")`.

*issue_detector*:
Automated issue detection engine that analyzes data against configurable thresholds. Core functionality:

- **Initialization** - Takes SQLiteStore and optional config dict. Default thresholds: high_cpu_percent (85), high_memory_percent (90), disk_space_critical (90), error_spike_multiplier (3.0), failed_login_threshold (5), threat_score_high (70), network_error_rate (5.0%), sustained_duration_min (300 seconds).
- **Security Detection** - `find_brute_force_attempts(hours, threshold)` groups failed logins by IP, flags IPs exceeding threshold with calculated severity (higher for rapid attempts), returns BruteForceAlert list. `find_suspicious_ips(threat_threshold)` queries high-threat IPs from reputation table, returns IPThreat list with threat scores. `find_unauthorized_access_attempts(hours)` searches for denied/unauthorized keywords in auth logs. `find_banned_ips(hours)` retrieves recent IP bans.
- **Performance Detection** - `find_high_cpu_periods(threshold, duration, hours)` finds sustained high CPU usage above threshold for minimum duration using `_find_high_resource_periods()` helper. `find_high_memory_periods(threshold, duration, hours)` same for memory. `find_disk_space_issues(threshold, hours)` checks most recent disk usage. `find_resource_exhaustion(metric_type, threshold, hours)` generic dispatcher. Helper `_find_high_resource_periods()` detects continuous periods exceeding threshold, calculates peak/avg values, returns HighUsagePeriod list.
- **Error Detection** - `find_error_spikes(hours, spike_multiplier)` groups errors by hour and type, detects when latest hour exceeds baseline by multiplier, returns ErrorSpike list with spike_factor. `find_recurring_errors(hours, min_occurrences)` groups errors by pattern signature, flags patterns exceeding minimum count, calculates rate per hour, returns RecurringError list. `find_critical_errors(hours)` queries traces with severity >= 80, returns CriticalError list. `find_database_issues(hours)` filters error patterns for database category.
- **Network Detection** - `find_connection_anomalies(hours)` calculates baseline connection counts with standard deviation, flags outliers > 3 std deviations, returns ConnectionAnomaly list with deviation_percent. `find_network_errors(hours, threshold)` calculates error rate (errors/packets), flags when exceeding threshold, returns NetworkIssue list.

All detection methods calculate severity dynamically based on how much thresholds are exceeded and return issues sorted by severity or relevant metric.

*analysis_engine*:
High-level analysis engine for health assessment and trend detection. Core functionality:

- **System Health** - `analyze_system_health(hours)` runs all issue detectors (security, performance, error, network), counts issues by severity level, calculates component scores (100 = perfect, weighted by issue severity), computes overall health_score as weighted average (security 30%, performance 25%, error 25%, network 20%), determines status (healthy >= 80, degraded >= 50, critical < 50), selects top 5 issues, generates actionable recommendations, returns HealthReport. Private helpers: `_detect_security_issues()`, `_detect_performance_issues()`, `_detect_error_issues()`, `_detect_network_issues()` aggregate all detector results. `_calculate_component_score(issues)` reduces score by total impact (each critical issue worth 20 points). `_generate_health_recommendations(issues, health_score, status)` creates priority-ordered recommendations based on issue counts and severity.
- **Security Analysis** - `analyze_security_posture(hours)` gathers security metrics (high-threat IPs, failed logins, bans), runs brute force and suspicious IP detection, builds top 5 threat IP list, calculates risk_score (0-100, lower better) based on threat counts and activity volume, determines security_posture (good < 20, fair < 50, poor < 80, critical >= 80), generates targeted recommendations (rate limiting, IP blacklist, 2FA, firewall review), returns SecurityReport.
- **Error Trends** - `analyze_error_trends(days)` retrieves all ERROR level events and patterns, calculates total_errors and error_rate (per hour), counts unique_error_types, groups by category/source/severity, runs recurring/spike/critical error detection for top 10 list, compares first half vs second half to determine trend (worsening > 1.2x, improving < 0.8x, stable otherwise), generates recommendations based on trend direction and error volume/categories, returns ErrorTrendReport.
- **Resource Trends** - `get_resource_usage_trends(days)` retrieves system metrics, analyzes cpu_percent/memory_percent/disk_percent using `_analyze_metric_trend()` helper, returns dict mapping metric names to TrendReport. Helper `_analyze_metric_trend(metrics, metric_name, days)` calculates statistics (min/max/avg/median/std_deviation), determines trend direction and strength via `_calculate_trend()` using linear regression, finds anomalies (> 2 std deviations from mean), returns TrendReport. `_calculate_trend(values)` performs least squares regression to calculate slope, computes R-squared for strength (0-1), determines direction (increasing/decreasing/stable based on slope threshold).
- **Utility Methods** - `get_top_error_sources(hours, limit)` counts errors by source, returns sorted list of top N sources with error counts.

The analysis engine integrates issue detection with statistical analysis to provide comprehensive health, security, and trend reporting with actionable insights.
