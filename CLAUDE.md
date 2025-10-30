# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Logly is a lightweight log aggregation and system monitoring tool for Linux servers, particularly AWS EC2 instances. It continuously collects system metrics, network activity, and log events into a SQLite database with time-series optimization. The tool provides proactive issue detection, health monitoring, and threat intelligence.

**Core Purpose**: Solve the problem of EC2 instances only retaining logs for 24 hours by providing persistent, queryable log and metrics storage.

## Development Commands

### Installation & Setup

This project uses conda for environment management:

```bash
# Create and activate conda environment
conda env create -f environment.yml
conda activate logly

# Or manually
conda create -n logly python>=3.8
conda activate logly
pip install -r requirements/requirements.txt
pip install -r requirements/requirements-dev.txt

# Install logly in editable mode
pip install -e .

# Alternative: Use the control script
./logly.sh install --dev
```

### Running Tests

```bash
# Ensure conda environment is activated
conda activate logly

# Run all tests
pytest tests/

# Run with coverage
pytest --cov=logly tests/

# Run specific test types
pytest tests/unit/          # Unit tests only
pytest tests/integration/   # Integration tests only
pytest tests/e2e/           # End-to-end tests only

# Run specific test file
pytest tests/unit/test_sqlite_store.py

# Run single test
pytest tests/unit/test_sqlite_store.py::test_insert_system_metric

# Using the control script
./logly.sh test
```

### Running the Application

```bash
# Start the daemon
logly start

# Start with custom config
logly start --config /path/to/config.yaml

# Run one-time collection (testing)
logly collect

# Check status
logly status

# View logs
logly logs

# Using the control script
./logly.sh start
./logly.sh status
./logly.sh logs
```

### Query and Analysis

```bash
# System health check
logly query health --hours 24

# Security analysis
logly query security --hours 48

# Performance issues
logly query performance --hours 72

# Error analysis
logly query errors --hours 168

# IP threat analysis
logly query ips --threshold 70
```

## Architecture Overview

### High-Level Structure

Logly follows a modular architecture with clear separation of concerns:

1. **Collection Layer** ([logly/collectors/](logly/collectors/))
   - `BaseCollector`: Abstract interface all collectors implement
   - `SystemMetrics`: Parses `/proc` filesystem directly (CPU, memory, disk, load)
   - `NetworkMonitor`: Parses `/proc/net/` for traffic and connection stats
   - `LogParser`: Regex-based parsing for fail2ban, syslog, auth.log, nginx, Django logs
   - `TracerCollector`: Collects enriched event tracing data

2. **Core Engine** ([logly/core/](logly/core/))
   - `Config`: YAML configuration with hardcoded path defaults
   - `Scheduler`: Manages periodic collection tasks with configurable intervals
   - `Aggregator`: Computes hourly/daily rollups for time-series data

3. **Storage Layer** ([logly/storage/](logly/storage/))
   - `SQLiteStore`: Main storage interface with time-series optimized schema
   - `models.py`: Data models (SystemMetric, NetworkMetric, LogEvent)
   - Schema: Indexed tables for fast time-range queries
   - **Important**: Database paths are HARDCODED via `utils/paths.py` - uses `logly/db/` directory

4. **Query System** ([logly/query/](logly/query/)) - Proactive issue detection
   - `QueryBuilder`: Fluent interface for complex queries
   - `IssueDetector`: Detects security, performance, error, network issues
   - `AnalysisEngine`: Trend analysis, health scoring, comprehensive reports
   - `models.py`: Issue and report data models with severity levels

5. **Tracers** ([logly/tracers/](logly/tracers/)) - Event correlation and intelligence
   - `EventTracer`: Enhanced event tracing with causality tracking
   - `ProcessTracer`: Process lifecycle and resource tracking
   - `NetworkTracer`: Network connection correlation
   - `IPTracer`: IP reputation and threat scoring
   - `ErrorTracer`: Deep error pattern analysis

6. **Export Layer** ([logly/exporters/](logly/exporters/))
   - `CSVExporter`: Export metrics/logs to CSV
   - `JSONExporter`: Export to JSON format
   - `ReportGenerator`: Human-readable summary reports

7. **CLI** ([logly/cli.py](logly/cli.py))
   - Commands: start, collect, status, export, report, query
   - Entry point: `logly` command via setuptools

### Key Design Patterns

**Collector Pattern**: All collectors inherit from `BaseCollector` and implement `collect()` method. The scheduler orchestrates collection intervals.

**Repository Pattern**: `SQLiteStore` provides a clean interface abstracting database operations. Context managers ensure proper connection handling.

**Fluent Builder**: `QueryBuilder` provides chainable methods for building complex queries without SQL.

**Strategy Pattern**: Exporters implement different serialization strategies (CSV, JSON, text reports).

### Critical Implementation Details

**Hardcoded Paths**: Database and log paths are hardcoded via [logly/utils/paths.py](logly/utils/paths.py):
- Database: `logly/db/logly.db`
- Logs: `logly/logs/`
- Config: `config/logly.yaml`

**Test Mode**: Tests use `LOGLY_TEST_MODE=1` environment variable to bypass hardcoded paths and use temporary directories. See [tests/conftest.py](tests/conftest.py) for automatic test mode setup.

**Concurrency**: SQLite connections use `timeout=30` and retry logic to handle concurrent access. See `SQLiteStore._connection()` for implementation.

**Collection Flow**:
1. Scheduler starts background threads for each collector type
2. Collectors read from `/proc` or parse log files
3. Data inserted into SQLite with timestamp indexing
4. Aggregator runs hourly/daily to compute rollups
5. Old data cleaned up based on retention policy

**Query Flow**:
1. User runs `logly query <command>`
2. CLI instantiates QueryBuilder/IssueDetector/AnalysisEngine
3. Issue detectors analyze data using thresholds and patterns
4. Results formatted as reports with severity levels
5. Optional JSON export for programmatic access

## Configuration

Config file: [config/logly.yaml](config/logly.yaml)

Key sections:
- `database`: Retention settings (path is hardcoded, cannot be changed)
- `collection`: Intervals for metrics/network/logs (in seconds)
- `system`/`network`/`logs`: Enable/disable collectors and specify metrics
- `aggregation`: Hourly/daily rollup settings
- `query`: Issue detection thresholds

## Testing Philosophy

Tests are organized into three tiers:
- **Unit** ([tests/unit/](tests/unit/)): Test individual components in isolation
- **Integration** ([tests/integration/](tests/integration/)): Test component interactions
- **E2E** ([tests/e2e/](tests/e2e/)): Test complete workflows and disaster recovery

All tests automatically enable `LOGLY_TEST_MODE=1` via conftest.py autouse fixture. This allows tests to use temporary database paths instead of hardcoded paths.

**Fixtures** ([tests/conftest.py](tests/conftest.py)):
- `test_store`: SQLiteStore with mocked path validation
- `populated_store`: Store with sample data
- `mock_config`: Mock Config object
- `mock_proc_files`: Mock /proc filesystem
- `temp_db_path`, `temp_dir`, `temp_log_dir`: Temporary directories

## Common Development Workflows

### Adding a New Collector

1. Create collector class inheriting from `BaseCollector` in [logly/collectors/](logly/collectors/)
2. Implement `collect()` method returning list of data models
3. Add collector to scheduler in [logly/core/scheduler.py](logly/core/scheduler.py)
4. Add configuration section to [config/logly.yaml](config/logly.yaml)
5. Add unit tests in [tests/unit/](tests/unit/)

### Adding a New Issue Detector

1. Define issue model in [logly/query/models.py](logly/query/models.py)
2. Add detection method to `IssueDetector` in [logly/query/issue_detector.py](logly/query/issue_detector.py)
3. Integrate into `AnalysisEngine` reports in [logly/query/analysis_engine.py](logly/query/analysis_engine.py)
4. Add CLI command in [logly/cli.py](logly/cli.py)
5. Add unit tests in [tests/unit/test_issue_detector.py](tests/unit/test_issue_detector.py)

### Adding a New Exporter

1. Create exporter class in [logly/exporters/](logly/exporters/)
2. Implement export methods for each data type (metrics, logs, reports)
3. Add to CLI export command in [logly/cli.py](logly/cli.py)
4. Add unit tests in [tests/unit/](tests/unit/)

### Debugging Database Issues

1. Check database path is correct: `logly/db/logly.db`
2. Verify schema: `sqlite3 logly/db/logly.db ".schema"`
3. Check data: `sqlite3 logly/db/logly.db "SELECT COUNT(*) FROM system_metrics;"`
4. Run integrity check: `sqlite3 logly/db/logly.db "PRAGMA integrity_check;"`
5. Enable debug logging in config: `logging.level: DEBUG`

### Working with Hardcoded Paths

**Production code**: Use helpers from [logly/utils/paths.py](logly/utils/paths.py):
- `get_db_path()`: Returns hardcoded database path
- `get_logs_dir()`: Returns hardcoded logs directory
- `get_project_root()`: Returns project root directory
- `validate_db_path(path)`: Validates path matches expected location

**Test code**: Use `LOGLY_TEST_MODE=1` environment variable (automatically set by conftest.py) to bypass path validation and use temporary directories.

## File Organization

```
logly/
├── logly/                      # Main package
│   ├── cli.py                 # CLI entry point
│   ├── collectors/            # Data collection implementations
│   ├── core/                  # Configuration, scheduling, aggregation
│   ├── exporters/             # CSV, JSON, report exporters
│   ├── query/                 # Issue detection and analysis
│   ├── storage/               # SQLite store and models
│   ├── tracers/               # Event tracing and correlation
│   └── utils/                 # Logging, paths, DB initialization
├── config/                     # YAML configuration
├── db/                        # SQLite database (gitignored)
├── logs/                      # Application logs (gitignored)
├── tests/                     # Test suite
│   ├── conftest.py           # Shared fixtures
│   ├── unit/                 # Unit tests
│   ├── integration/          # Integration tests
│   └── e2e/                  # End-to-end tests
├── systemd/                   # Systemd service file
├── requirements/              # Dependency files
├── environment.yml            # Conda environment spec
├── logly.sh                  # Control script
├── pyproject.toml            # Package metadata
└── setup.py                  # Package setup
```

## Dependencies

**Runtime dependencies** (minimal):
- `pyyaml>=6.0` - YAML configuration parsing

**Development dependencies**:
- `pytest>=7.0` - Test framework
- `pytest-cov>=4.0` - Coverage reporting

Logly uses Python stdlib where possible to minimize dependencies. Direct parsing of `/proc` filesystem eliminates need for `psutil` or similar tools.

## Important Notes

- **Conda environment required** - Always activate `conda activate logly` before development
- **Never modify hardcoded paths in production code** - paths are intentionally hardcoded for security and consistency
- **Always use test mode for tests** - automatically enabled by conftest.py
- **Parse /proc directly** - avoid external tools for system metrics to minimize dependencies
- **Use context managers** - for database connections and file operations
- **Index timestamps** - critical for time-range query performance
- **Handle log rotation** - LogParser tracks file positions and handles rotation
- **Thread safety** - collectors run in threads, ensure proper locking for shared resources
- **Query thresholds** - configurable in `config/logly.yaml` under `query.thresholds`
