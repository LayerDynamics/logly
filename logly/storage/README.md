# Storage

## What We Store

The storage module handles all data persistence for Logly using SQLite as the database backend. It stores time-series metrics, log events, comprehensive trace data, and pre-computed aggregates for efficient querying.

**Core data types:**

1. **System Metrics** - CPU, memory, disk usage, and load averages collected over time
2. **Network Metrics** - Network traffic statistics, packet counts, error rates, and connection states
3. **Log Events** - Parsed log entries from various sources (fail2ban, syslog, auth logs) with metadata
4. **Event Traces** - Comprehensive trace data linking events to processes, network connections, IP reputation, and error analysis
5. **Aggregates** - Pre-computed hourly and daily statistics for fast historical queries

**Database features:**

- Time-series optimized with timestamp indexing for fast range queries
- Composite indexes for common query patterns (source + timestamp, IP + timestamp)
- Foreign key relationships linking traces to their source events
- JSON field support for flexible metadata and nested data structures
- Automatic aggregation system for hourly and daily summaries
- Data retention policies for cleanup of old records

### Detailed Breakdown

*models*:
Defines dataclass models for all stored entities with serialization methods. Key models include:

- **SystemMetric** - CPU percent/count, memory total/available/percent, disk total/used/percent, disk I/O bytes, load averages (1/5/15 min). Includes `now()` factory method and `to_dict()` serialization.
- **NetworkMetric** - Bytes/packets sent/received, errors in/out, drops in/out, connection counts by state (established, listen, time_wait). Includes `now()` factory and `to_dict()`.
- **LogEvent** - Core event model with timestamp, source, level, message, IP address, user, service, action, and flexible JSON metadata. Includes `now()` factory, `to_dict()`, and `from_dict()` with JSON metadata handling.
- **EventTrace** - Complete trace linking log events to causality chains, related services, severity scores, root causes, and trace metadata. Factory method `from_trace_dict()` converts tracer collector output.
- **ProcessTrace** - Process snapshot with PID, name, cmdline, state, parent PID, memory (RSS/VM), CPU times (user/system), thread count, and I/O statistics (bytes and syscalls).
- **NetworkTrace** - Network connection snapshot with local/remote IP/port, connection state, and protocol.
- **ErrorTrace** - Detailed error analysis with error type/category, exception type, severity, file location (path/line), error code, stacktrace flag, root cause hints, and recovery suggestions.

All models include `to_dict()` methods for database storage and handle JSON serialization for complex fields (metadata, lists, nested objects).

*sqlite_store*:
SQLiteStore class provides the complete database interface with connection management, CRUD operations, and query methods. Core functionality:

- **Initialization** - Enforces hardcoded database path validation, creates database directory, executes schema if needed, checks for existing tables to avoid re-initialization.
- **Connection Management** - Context manager pattern for safe connection handling with automatic cleanup and sqlite3.Row factory for dict-like results.
- **System Metrics Operations** - `insert_system_metric()` stores metrics, `get_system_metrics(start_time, end_time, limit)` retrieves time-range queries.
- **Network Metrics Operations** - `insert_network_metric()` and `get_network_metrics(start_time, end_time, limit)` for network data.
- **Log Events Operations** - `insert_log_event()` stores events with JSON metadata, `get_log_events(start_time, end_time, source, level, limit)` supports filtering by source and level.
- **Aggregation System** - `compute_hourly_aggregates(hour_timestamp)` pre-computes hourly stats (CPU/memory averages and maxes, network totals, log event counts by type), `compute_daily_aggregates(date_str)` rolls up hourly data into daily summaries with unique IP/user counts.
- **Tracer Integration** - `insert_event_trace()` stores comprehensive traces with automatic insertion of related process traces, network traces, error traces, and IP reputation updates. Private helper methods handle each trace type.
- **Query Methods** - `get_traces()` retrieves event traces with filtering by time/source/severity, `get_ip_reputation()` looks up IP info, `get_high_threat_ips(threshold)` finds dangerous IPs, `get_error_patterns()` aggregates error statistics by type and category.
- **Maintenance** - `cleanup_old_data(retention_days)` deletes records older than retention period, `get_stats()` returns table row counts and database size.

All operations use parameterized queries for SQL injection protection and proper transaction handling with commit/rollback.

*schema.sql*:
Complete SQL schema defining all tables with proper indexes for performance. Structure includes:

- **Core Tables** - `system_metrics` (CPU/memory/disk/load data), `network_metrics` (traffic and connection stats), `log_events` (parsed log entries with source/level/action).
- **Aggregate Tables** - `hourly_aggregates` (pre-computed hourly stats with unique hour_timestamp), `daily_aggregates` (daily rollups in YYYY-MM-DD format with unique IP/user counts).
- **Tracer Tables** - `event_traces` (master trace table with causality chains and severity scores), `process_traces` (process snapshots with resource usage), `network_traces` (connection snapshots), `error_traces` (detailed error analysis), `ip_reputation` (IP tracking with threat scores and activity counters), `trace_patterns` (detected patterns across events).
- **Metadata Table** - `metadata` (key-value store for schema version, created timestamp, hostname).
- **Indexes** - Timestamp indexes on all time-series tables for range queries, composite indexes (source+timestamp, IP+timestamp), foreign key indexes, specialized indexes (threat_score, severity, error_type).
- **Foreign Keys** - `event_traces.event_id` references `log_events.id`, `process_traces.trace_id` references `event_traces.id`, `network_traces.trace_id` references `event_traces.id`, `error_traces.trace_id` references `event_traces.id`.
- **Optimizations** - Unix timestamps (INTEGER) for fast comparisons, JSON fields (TEXT) for flexible nested data, DEFAULT values for counters and flags, UNIQUE constraints on time-based aggregates.

Schema version 2.0 includes the complete tracer system integration with comprehensive indexing strategy for both time-series queries and relational lookups.
