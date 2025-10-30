# Logly

A lightweight, minimal-dependency log aggregation and system monitoring tool designed for AWS EC2 instances running Django/PostgreSQL applications.

## Overview

Logly solves the problem of AWS EC2 instances only retaining logs for 24 hours by continuously collecting, storing, and aggregating:

- System metrics (CPU, memory, disk usage, load average)
- Network activity (traffic stats, connection counts)
- Log events (fail2ban, syslog, auth.log, custom application logs)

All data is stored in an optimized SQLite database with time-series indexing for fast queries and easy export.

## Features

- **Minimal Dependencies**: Uses Python stdlib where possible (only requires PyYAML)
- **Zero-Config Storage**: SQLite database with automatic schema initialization
- **Efficient Collection**: Parses `/proc` filesystem directly, no external tools needed
- **Time-Series Optimization**: Indexed database with hourly/daily aggregates
- **Flexible Exports**: CSV, JSON, and human-readable summary reports
- **Systemd Integration**: Runs as a background service
- **Log Parsing**: Intelligent parsing for fail2ban, syslog, auth.log, nginx, Django logs
- **Automatic Cleanup**: Configurable data retention policies
- **🆕 Issue Detection**: Proactive detection of security threats, performance problems, and error patterns
- **🆕 Query System**: Fluent interface for querying metrics, logs, and system health
- **🆕 Health Monitoring**: Comprehensive health scoring (0-100) with actionable recommendations
- **🆕 Threat Intelligence**: IP reputation tracking and brute force attack detection

## Installation

### From Source

```bash
# Clone or copy the repository
cd logly

# Install dependencies
pip install -r requirements.txt

# Install logly
pip install -e .
```

### System Installation

```bash
# Install to system Python
sudo pip install .

# Copy configuration
sudo mkdir -p /etc/logly
sudo cp config/logly.yaml /etc/logly/

# Copy systemd service
sudo cp systemd/logly.service /etc/systemd/system/

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable logly
sudo systemctl start logly
```

## Configuration

Configuration file: `/etc/logly/logly.yaml` (or specify with `-c` flag)

### Key Settings

```yaml
database:
  path: "/var/lib/logly/logly.db"
  retention_days: 90

collection:
  system_metrics: 60      # Collect every 60 seconds
  network_metrics: 60
  log_parsing: 300        # Parse logs every 5 minutes

logs:
  sources:
    fail2ban:
      path: "/var/log/fail2ban.log"
      enabled: true
    django:
      path: "/var/log/django/django.log"
      enabled: true
```

See [config/logly.yaml](config/logly.yaml) for full configuration options.

## Usage

### Start the Daemon

```bash
# Using systemd (recommended)
sudo systemctl start logly

# Or run directly
sudo logly start

# With custom config
sudo logly start --config /path/to/config.yaml
```

### Check Status

```bash
logly status
```

Output:

```ascii
============================================================
LOGLY STATUS
============================================================
Database Path:           /var/lib/logly/logly.db
Database Size:           15.43 MB
System Metrics:          12,450 records
Network Metrics:         12,450 records
Log Events:              3,892 records
Hourly Aggregates:       168 records
Daily Aggregates:        7 records
============================================================
```

### Export Data

Export system metrics to CSV:

```bash
logly export system /tmp/metrics.csv --days 7
```

Export network metrics to JSON:

```bash
logly export network /tmp/network.json --hours 24 --format json
```

Export logs with filters:

```bash
logly export logs /tmp/auth-errors.csv --source auth --level ERROR --days 1
```

### Generate Reports

```bash
# Generate summary report
logly report /tmp/summary.txt --days 7

# Generate and print to console
logly report /tmp/summary.txt --hours 24 --print
```

Example report:

```ascii
======================================================================
LOGLY SUMMARY REPORT
======================================================================

Report Period: 2025-10-23 00:00:00 to 2025-10-30 00:00:00
Duration: 168.0 hours

----------------------------------------------------------------------
SYSTEM METRICS
----------------------------------------------------------------------
  CPU Usage (avg):        23.4%
  CPU Usage (max):        87.2%
  Memory Usage (avg):     45.6%
  Memory Usage (max):     78.3%
  Disk Usage (avg):       62.1%

----------------------------------------------------------------------
NETWORK METRICS
----------------------------------------------------------------------
  Bytes Sent (total):     2.34 GB
  Bytes Received (total): 5.67 GB
  Packets Sent:           1,234,567
  Packets Received:       2,345,678

----------------------------------------------------------------------
LOG EVENTS
----------------------------------------------------------------------
  Total Events:           3,892
  Failed Logins:          45
  Banned IPs:             12
  Errors:                 156
  Warnings:               892
```

### Query for Issues and Problems

Logly includes a powerful query system that proactively detects and reports issues:

#### System Health Check

Get an overall health assessment with component scores and actionable recommendations:

```bash
logly query health --hours 24
```

Output:

```ascii
======================================================================
SYSTEM HEALTH REPORT
======================================================================
Time Window:       24 hours
Health Score:      87/100
Status:            HEALTHY

Component Scores:
----------------------------------------------------------------------
  Security:        95/100
  Performance:     82/100
  Errors:          90/100
  Network:         98/100

Issues Detected:
----------------------------------------------------------------------
  Critical:        0
  High:            1
  Medium:          2
  Low:             3
  Total:           6

Top Issues:
----------------------------------------------------------------------
  1. [HIGH] High CPU Usage
     CPU usage sustained above 85% for 600 seconds
  2. [MEDIUM] Recurring Error: app:database_error
     Error occurred 12 times in 24 hours

Recommendations:
----------------------------------------------------------------------
  • Investigate 2 performance issue(s) - check resource utilization
  • Fix 1 recurring error pattern(s)
======================================================================
```

#### Security Analysis

Detect brute force attacks, suspicious IPs, and unauthorized access attempts:

```bash
logly query security --hours 48
```

Features:
- Brute force attack detection (5+ failed logins from same IP)
- High-threat IP identification (threat score ≥ 70)
- Failed login tracking
- Ban event monitoring

Output includes threat scores, top attacking IPs, and security recommendations.

#### Performance Issue Detection

Find resource exhaustion, high CPU/memory usage, and disk space problems:

```bash
logly query performance --hours 72
```

Detects:
- Sustained high CPU usage (>85% for 5+ minutes)
- High memory pressure (>90%)
- Low disk space (<10% free)
- Resource exhaustion patterns

#### Error Analysis

Analyze error trends, detect spikes, and find recurring problems:

```bash
logly query errors --hours 168  # Last week
```

Features:
- Error spike detection (3x baseline rate)
- Recurring error identification
- Critical error alerting
- Error trend analysis (improving/worsening/stable)
- Top error sources ranking

#### IP Threat Analysis

Query IP reputation data and threat intelligence:

```bash
# Find all high-threat IPs
logly query ips --threshold 70

# Export to JSON for further analysis
logly query ips --threshold 80 -o threats.json
```

Shows:
- IP threat scores (0-100)
- Failed login counts per IP
- Ban history
- Activity timestamps

#### Export Query Results

All query commands support JSON export:

```bash
logly query health --hours 24 -o health_report.json
logly query security --hours 48 -o security_analysis.json
logly query errors --hours 168 -o error_trends.json
```

### Programmatic Usage

Use the query module in your Python code:

```python
from logly.query import IssueDetector, AnalysisEngine, QueryBuilder
from logly.storage.sqlite_store import SQLiteStore
from logly.core.config import Config

# Initialize
config = Config()
store = SQLiteStore(config.get_database_config()['path'])

# Use QueryBuilder for fluent queries
query = QueryBuilder(store)

# Get recent errors
recent_errors = query.events()\
    .in_last_hours(24)\
    .errors_only()\
    .by_source("django")\
    .all()

# Get high-threat IPs sorted by threat score
threats = query.ips()\
    .high_threat()\
    .sort_by_threat()

# Calculate average CPU usage
avg_cpu = query.metrics()\
    .system()\
    .in_last_days(7)\
    .avg("cpu_percent")

# Use IssueDetector to find problems
detector = IssueDetector(store, config.get('query.thresholds', {}))
brute_force = detector.find_brute_force_attempts(hours=24)
high_cpu = detector.find_high_cpu_periods(hours=24)

# Use AnalysisEngine for comprehensive reports
engine = AnalysisEngine(store, config.get('query', {}))
health = engine.analyze_system_health(hours=24)
security = engine.analyze_security_posture(hours=48)
print(f"System Health Score: {health.health_score}/100")
print(f"Security Risk Score: {security.risk_score}/100")
```

### One-Time Collection (Testing)

```bash
# Run collection once without starting daemon
logly collect
```

## Architecture

```ascii
logly/
├── logly/
│   ├── __init__.py
│   ├── __main__.py              # Entry point (python -m logly)
│   ├── cli.py                   # CLI interface
│   ├── collectors/
│   │   ├── base_collector.py   # Abstract collector interface
│   │   ├── system_metrics.py   # CPU, memory, disk from /proc
│   │   ├── network_monitor.py  # Network stats from /proc/net
│   │   └── log_parser.py       # Regex-based log parsing
│   ├── core/
│   │   ├── config.py           # YAML configuration management
│   │   ├── scheduler.py        # Periodic collection scheduling
│   │   └── aggregator.py       # Time-series rollup computation
│   ├── storage/
│   │   ├── sqlite_store.py     # SQLite operations
│   │   ├── models.py           # Data models
│   │   └── schema.sql          # Database schema
│   ├── exporters/
│   │   ├── csv_exporter.py     # CSV export
│   │   ├── json_exporter.py    # JSON export
│   │   └── report_generator.py # Summary reports
│   ├── query/                   # 🆕 Query and issue detection
│   │   ├── __init__.py         # Public API
│   │   ├── models.py           # Issue and report data models
│   │   ├── issue_detector.py   # Detects security/perf/error issues
│   │   ├── query_builder.py    # Fluent query interface
│   │   └── analysis_engine.py  # Trend analysis and health scoring
│   └── tracers/                 # Event tracing and causality
│       ├── event_tracer.py     # Enhanced event tracing
│       ├── ip_reputation.py    # IP threat scoring
│       └── error_tracer.py     # Deep error analysis
├── config/
│   └── logly.yaml              # Default configuration
├── systemd/
│   └── logly.service           # Systemd service file
└── tests/                       # Unit tests
```

## How It Works

### 1. Data Collection

**System Metrics** (every 60 seconds):

- Parses `/proc/stat` for CPU usage
- Parses `/proc/meminfo` for memory stats
- Uses `os.statvfs()` for disk usage
- Parses `/proc/diskstats` for I/O stats
- Reads `/proc/loadavg` for load average

**Network Metrics** (every 60 seconds):

- Parses `/proc/net/dev` for traffic stats
- Parses `/proc/net/tcp` and `/proc/net/tcp6` for connection states

**Log Parsing** (every 5 minutes):

- Tracks file positions to read only new lines
- Applies regex patterns to extract structured data
- Handles log rotation automatically

### 2. Storage

- **SQLite Database** with optimized schema
- **Indexed Timestamps** for fast time-range queries
- **Composite Indexes** for common query patterns
- **Automatic Schema Initialization** on first run

### 3. Aggregation

- **Hourly Rollups**: Computed at the top of each hour
- **Daily Rollups**: Computed at midnight
- **Automatic Cleanup**: Removes data older than retention period

### 4. Export

- **CSV**: For Excel, pandas analysis
- **JSON**: For programmatic access
- **Summary Reports**: Human-readable text format

## Database Schema

### Tables

- `system_metrics` - Raw CPU, memory, disk metrics
- `network_metrics` - Raw network traffic and connections
- `log_events` - Parsed log entries with structured fields
- `hourly_aggregates` - Pre-computed hourly statistics
- `daily_aggregates` - Pre-computed daily statistics
- `metadata` - System information and versioning

### Indexes

- `idx_system_metrics_timestamp` - Fast time-range queries
- `idx_log_events_source_timestamp` - Fast log filtering
- And more (see [schema.sql](logly/storage/schema.sql))

## Performance

- **Minimal CPU Impact**: Collection runs in background with configurable intervals
- **Low Memory Footprint**: ~20MB RSS during normal operation
- **Fast Queries**: Indexed time-series data with sub-second query times
- **Efficient Storage**: SQLite with automatic VACUUM and cleanup

## Troubleshooting

### Logs

```bash
# View systemd logs
sudo journalctl -u logly -f

# Check log file (if configured)
tail -f /var/log/logly/logly.log
```

### Permissions

Logly needs to run as root to access system logs. Ensure:

- Read access to `/var/log/` files
- Write access to database path
- Read access to `/proc/` filesystem

### Database Issues

```bash
# Check database integrity
sqlite3 /var/lib/logly/logly.db "PRAGMA integrity_check;"

# View schema
sqlite3 /var/lib/logly/logly.db ".schema"

# Manual cleanup
sqlite3 /var/lib/logly/logly.db "VACUUM;"
```

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/

# With coverage
pytest --cov=logly tests/
```

### Project Structure

See [Architecture](#architecture) section above.

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## FAQ

**Q: Why SQLite instead of a time-series database?**
A: SQLite provides zero-config setup, excellent performance for this use case, and easy backup/migration. For most single-server deployments, it's more than sufficient.

**Q: Can I use this on non-AWS servers?**
A: Yes! Logly works on any Linux system with `/proc` filesystem (Ubuntu, Debian, RHEL, etc.).

**Q: How much disk space does it use?**
A: With default settings (90-day retention), expect ~50-100MB per month depending on log volume.

**Q: Can I add custom log sources?**
A: Yes! Add new sources to the `logs.sources` section in config and Logly will parse them.

**Q: Does it support multiple servers?**
A: Currently Logly is designed for single-server deployments. For multi-server setups, run one instance per server and aggregate externally.

## Roadmap

- [ ] Web UI for viewing metrics
- [ ] Alerting system (email, Slack, webhooks)
- [ ] PostgreSQL storage backend option
- [ ] Plugin system for custom collectors
- [ ] Docker container image
- [ ] Multi-server aggregation

## Support

For issues, questions, or feature requests, please open an issue on GitHub.
