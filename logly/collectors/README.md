# Collectors

## What It Does

The collectors module gathers system metrics, network statistics, and log events from various sources. Each collector specializes in a specific data type and uses /proc filesystem or log file parsing for minimal dependencies.

**Core capabilities:**

1. **System Metrics** - CPU, memory, disk usage, and load average collection
2. **Network Monitoring** - Network I/O statistics and TCP connection tracking
3. **Log Parsing** - Structured extraction from fail2ban, syslog, auth.log
4. **Trace Collection** - Comprehensive event enrichment with process, network, IP, and error context

**Key features:**

- Zero external dependencies (uses /proc filesystem)
- Incremental log reading with position tracking
- Configurable metric selection per collector
- Regex-based log pattern matching
- Multi-tracer integration for complete event context
- Validation methods for permission checking
- Stateful tracking (CPU deltas, log positions)

### Detailed Breakdown

*base_collector*:
Abstract base class defining collector interface. Core structure:

- **Initialization** - `__init__(config)` takes config dict, extracts enabled flag (default True).
- **Abstract Method** - `collect()` must be implemented by subclasses, returns collected data (type varies by collector).
- **Enabled Check** - `is_enabled()` returns boolean from config.
- **Validation** - `validate()` checks if collector can run (permissions, file access), default True, overridden by subclasses.

Provides consistent interface for all data collection modules with config-driven enable/disable.

*system_metrics*:
Collects system resource metrics from /proc filesystem. Core functionality:

- **Initialization** - Extends BaseCollector, stores metrics_to_collect list from config, initializes state tracking (_last_cpu_stats,_last_disk_io) for delta calculations.
- **Collection** - `collect()` returns SystemMetric.now() with requested metrics conditionally collected based on config:
  - **CPU** - Reads /proc/stat, parses user/nice/system/idle/iowait/irq/softirq times, calculates percentage as (total_diff - idle_diff) / total_diff since last call, stores state for next delta, gets CPU count from os.cpu_count().
  - **Memory** - Parses /proc/meminfo for MemTotal, MemAvailable (or estimates from MemFree + Buffers + Cached), converts KB to bytes, calculates percent as 100 * (1 - available/total).
  - **Disk** - Uses os.statvfs(path) for root filesystem, calculates total (blocks *frsize), free (bavail* frsize), used (total - free), percent (used / total * 100).
  - **Disk I/O** - Parses /proc/diskstats, filters whole disks (sda, vda, nvme) excluding partitions, sums read_sectors (field 5) and write_sectors (field 9), converts sectors to bytes (512 bytes/sector), returns cumulative values.
  - **Load Average** - Reads /proc/loadavg, parses first 3 floats (1min, 5min, 15min averages).
- **Validation** - Checks /proc/stat and /proc/meminfo exist.

All methods handle exceptions gracefully, returning default values (0, None) on errors. CPU percent requires two collections to calculate delta.

*network_monitor*:
Collects network I/O and connection statistics from /proc/net. Core functionality:

- **Initialization** - Extends BaseCollector, stores metrics_to_collect list, initializes _last_net_io state for deltas.
- **Collection** - `collect()` returns NetworkMetric.now() with conditionally collected metrics:
  - **Network I/O** - Parses /proc/net/dev (skips header 2 lines), excludes loopback (lo), for each interface extracts receive fields (bytes, packets, errs, drop in fields 0-3) and transmit fields (bytes, packets, errs, drop in fields 8-11), sums across all interfaces, returns cumulative totals for bytes_sent/recv, packets_sent/recv, errors_in/out, drops_in/out.
  - **Connections** - Parses /proc/net/tcp and /proc/net/tcp6 (skips header), extracts state field (index 3), maps hex codes (01=ESTABLISHED, 0A=LISTEN, 06=TIME_WAIT), counts connections by state.
- **Validation** - Checks /proc/net/dev exists.

Provides comprehensive network visibility without external tools like netstat or ss.

*log_parser*:
Parses system logs using regex patterns to extract structured events. Core functionality:

- **Pattern Definitions** - Compiled regex patterns in PATTERNS dict:
  - **fail2ban_ban** - Matches "[jail] Ban/Unban IP" format
  - **fail2ban_found** - Matches "[jail] Found IP" format
  - **auth_failed** - Matches "Failed password for [invalid user] USERNAME from IP"
  - **auth_accepted** - Matches "Accepted METHOD for USERNAME from IP"
  - **syslog_error** - Matches standard syslog format with timestamp, host, service, message
- **Initialization** - Extends BaseCollector, stores log_sources dict from config (source name -> {enabled, path}), initializes_file_positions dict for tracking read positions per file.
- **Collection** - `collect()` iterates enabled sources, calls `_parse_log_file()` for each, returns aggregated list of LogEvent objects. Handles missing files and parsing exceptions.
- **File Parsing** - `_parse_log_file(source, log_path)` tracks last read position, detects log rotation (file size < last_pos), seeks to last position, reads new lines incrementally, parses each line via `_parse_line()`, updates position after read.
- **Line Parsing** - `_parse_line(source, line)` dispatches to source-specific parsers (_parse_fail2ban, _parse_auth_log,_parse_syslog,_parse_django_log, _parse_nginx_log) or generic parser, returns LogEvent or None.
- **Fail2ban Parser** - Matches ban/unban patterns, extracts jail name, IP address, action (ban/unban/banned), creates LogEvent with source="fail2ban", level="WARNING", action=action, ip_address=IP, service=jail.
- **Auth Parser** - Matches failed/accepted password patterns, extracts username, IP, method, creates LogEvent with appropriate action (failed_login, successful_login).
- **Generic Parser** - Extracts basic info (timestamp, level keywords ERROR/WARN/INFO), creates minimal LogEvent.

Incremental reading ensures only new log lines are processed, preventing duplicate events and reducing I/O.

*tracer_collector*:
Integrates all tracer modules to create comprehensive event traces. Core functionality:

- **Initialization** - Extends BaseCollector, creates instances of all tracers (EventTracer, ProcessTracer, NetworkTracer, IPTracer, ErrorTracer), reads config flags (trace_processes, trace_network, trace_ips, trace_errors) for conditional tracing.
- **Event Tracing** - `trace_event(log_event)` creates comprehensive trace:
  - **Event Trace** - Calls event_tracer.trace_event() for base trace (severity_score, causality chains, related_services)
  - **Process Tracing** - If service identified, calls process_tracer.trace_by_name(service) to find matching processes, adds processes list and resource_summary (aggregated CPU/memory/I/O)
  - **Network Tracing** - If IP present, calls network_tracer.find_connections_by_ip() for active connections (limited to 10), adds network_stats with connection counts
  - **IP Tracing** - Calls ip_tracer.trace_ip() for IP classification (local/private/public), threat_score, activity counts, calls update_ip_activity() to track failed logins/bans
  - **Error Tracing** - If level is ERROR/CRITICAL/WARNING, calls error_tracer.trace_error() for error_type, error_category, severity, root_cause_hints, recovery_suggestions
  - **Metadata** - Adds trace_metadata with traced_at timestamp, tracer_version, tracers_used list
  - Returns unified trace dict combining all tracer outputs
- **Batch Tracing** - `trace_batch(log_events)` calls trace_event() for each event, catches exceptions and returns minimal trace on error
- **Pattern Analysis** - `analyze_traces(log_events)` aggregates traces to find patterns, calls ip_tracer.analyze_ip_patterns() for IP behavior, error_tracer.analyze_error_patterns() for error trends, returns summary with insights

Tracer collector provides the deepest level of analysis by correlating log events with system state, network activity, IP reputation, and error context.
