# Core

## What It Does

The core module provides the central orchestration layer for Logly, managing configuration, scheduling periodic tasks, and aggregating time-series data. It coordinates all collection activities and ensures data is processed and stored consistently.

**Core capabilities:**

1. **Configuration Management** - Centralized YAML-based configuration with defaults and validation
2. **Task Scheduling** - Periodic execution of collection and aggregation tasks
3. **Data Aggregation** - Time-series rollup of raw data into hourly and daily summaries

**Key features:**

- YAML configuration with deep merging and defaults
- Hardcoded critical paths (database, logs) for security
- Background threading for non-blocking operation
- Automatic error recovery and logging
- Configurable collection intervals per data type
- Time-based aggregation triggers (hourly, daily)
- Graceful start/stop with cleanup

### Detailed Breakdown

*config*:
Manages application configuration with YAML file loading and defaults. Core functionality:

- **Hardcoded Paths** - Database path and log directory are HARDCODED via `get_db_path()` and `get_logs_dir()` utilities and CANNOT be overridden by config files for security. Config enforcement happens in `_load_config()` after merging.
- **Default Configuration** - Comprehensive defaults defined in DEFAULT_CONFIG dict:
  - **database** - path (hardcoded), retention_days (90)
  - **collection** - system_metrics interval (60s), network_metrics (60s), log_parsing (300s)
  - **system** - enabled (True), metrics list (cpu_percent, cpu_count, memory, disk, load)
  - **network** - enabled (True), metrics list (bytes, packets, connections, ports)
  - **logs** - enabled (True), sources dict (fail2ban, syslog, auth) with paths and enabled flags
  - **aggregation** - enabled (True), intervals (hourly, daily), keep_raw_data_days (7)
  - **export** - default_format (csv), timestamp_format ("%Y-%m-%d %H:%M:%S")
  - **logging** - log_dir (hardcoded)
- **Initialization** - `__init__(config_path)` takes optional config file path, calls `_load_config()`.
- **Config Loading** - `_load_config()` searches default paths (project_root/config/logly.yaml), loads YAML if found, deep merges with defaults using `_deep_merge()`, enforces hardcoded paths, returns merged config dict. Falls back to defaults if file not found or parsing fails.
- **Deep Merge** - `_deep_merge(base, override)` recursively merges dicts, override values take precedence, nested dicts are merged not replaced.
- **Value Access** - `get(key_path, default)` retrieves values by dot-separated path (e.g., "database.path"), returns default if path not found. Convenience methods: `get_database_config()`, `get_collection_config()`, `get_system_config()`, `get_network_config()`, `get_logs_config()`, `get_aggregation_config()`, `get_export_config()`, `get_logging_config()` return section dicts.

Config class provides centralized access to all application settings with type-safe defaults and override protection for critical paths.

*scheduler*:
Orchestrates periodic data collection and aggregation tasks using stdlib sched module. Core functionality:

- **Initialization** - `__init__(config, store)` takes Config and SQLiteStore instances, creates scheduler and thread, initializes collectors conditionally based on config (SystemMetricsCollector, NetworkMonitor, LogParser only if enabled), creates Aggregator, extracts collection intervals from config.
- **Repeating Tasks** - `_schedule_repeating(interval, func, name)` schedules recurring tasks, wrapper function executes task with error handling, reschedules itself if still running, uses scheduler.enter() with interval and priority. Implements self-rescheduling pattern for continuous operation.
- **Collection Methods** - Private methods execute collectors and store results:
  - `_collect_system_metrics()` - Calls system_collector.collect(), stores via insert_system_metric()
  - `_collect_network_metrics()` - Calls network_collector.collect(), stores via insert_network_metric()
  - `_parse_logs()` - Calls log_parser.collect() (returns list), iterates and stores each event via insert_log_event()
  - All methods check if collector exists, catch and log exceptions
- **Aggregation** - `_run_aggregations()` checks current time (minute and hour), runs aggregator.run_hourly_aggregation() at top of hour (minute == 0), runs aggregator.run_daily_aggregation() at midnight (hour == 0, minute == 0). Time-based triggering ensures aggregations align with natural boundaries.
- **Cleanup** - `_cleanup_old_data()` reads retention_days from config (default 90), calls store.cleanup_old_data(retention_days) to remove old records.
- **Start** - `start()` method sets running flag, schedules all enabled collectors with their intervals, schedules aggregations every 3600s (hourly), schedules cleanup every 86400s (daily), starts background daemon thread running `_run()` loop, logs all scheduled tasks.
- **Scheduler Loop** - `_run()` private method runs while running flag is True, calls scheduler.run(blocking=False), sleeps 1 second between checks, catches and logs exceptions with 5s retry delay.
- **Stop** - `stop()` method clears running flag, joins thread with 5s timeout for graceful shutdown.
- **Manual Execution** - `run_once()` method executes all enabled collectors once without scheduling, useful for testing and manual runs.

Scheduler provides non-blocking background orchestration with automatic error recovery and configurable intervals.

*aggregator*:
Handles time-series data aggregation for hourly and daily rollups. Core functionality:

- **Initialization** - `__init__(store, config)` takes SQLiteStore and aggregation config dict, extracts enabled flag (default True), intervals list (default ['hourly', 'daily']), keep_raw_data_days (default 7).
- **Hourly Aggregation** - `run_hourly_aggregation()` checks enabled and 'hourly' in intervals, calculates previous complete hour (current hour minus 1, rounded to hour), converts to Unix timestamp, calls store.compute_hourly_aggregates(hour_timestamp) which calculates averages/maxes for system metrics, sums for network metrics, counts for log events, inserts into hourly_aggregates table. Designed to run at top of each hour to aggregate previous 60 minutes.
- **Daily Aggregation** - `run_daily_aggregation()` checks enabled and 'daily' in intervals, gets yesterday's date, formats as YYYY-MM-DD string, calls store.compute_daily_aggregates(date_str) which aggregates hourly data into daily summaries with unique IP/user counts, inserts into daily_aggregates table. Designed to run at midnight to aggregate previous day.
- **Raw Data Cleanup** - `cleanup_old_raw_data()` placeholder method for future implementation, intended to delete raw metrics older than keep_raw_data_days while preserving aggregates. Currently relies on main retention policy in store.

Aggregator reduces storage requirements and improves query performance by pre-computing statistics at regular intervals. Aggregates enable fast historical queries without scanning millions of raw records.
