# Tracers

## How it works

The tracers module provides specialized components for analyzing different aspects of system logs and activity. Each tracer focuses on a specific domain (IPs, networks, processes, events, or errors) and can be used independently or in combination to build a complete picture of system behavior.

**Common workflow:**

1. **Data Collection** - Tracers gather information from various sources (log files, /proc filesystem, system files)
2. **Analysis** - Each tracer processes and enriches the raw data with contextual information
3. **Pattern Detection** - Tracers identify patterns, anomalies, and relationships in the data
4. **Reporting** - Tracers provide structured output with insights, statistics, and actionable recommendations

**Integration:** Tracers work together to provide comprehensive analysis. For example:

- An IP tracer identifies a suspicious IP address
- The network tracer shows active connections from that IP
- The process tracer reveals which processes are handling those connections
- The event tracer links the activity to specific log events
- The error tracer surfaces any related errors or failures

### Detailed Breakdown Of Each Tracer

*ip_tracer*:
The IP tracer identifies and tracks IP addresses to determine their origin, type, and behavior patterns. It maintains a cache of IP information and tracks malicious/whitelisted IPs. Key features include:

- Classifies IPs as localhost, private, cloud, or public
- Detects private IP ranges (192.168.x.x, 10.x.x.x, 172.16-31.x.x, IPv6 private ranges)
- Calculates threat scores (0-100) based on activity patterns
- Tracks failed logins, bans, and suspicious activity per IP
- Auto-blacklists IPs that exceed threat threshold (>=70)
- Analyzes patterns across multiple IPs (by type, subnet, activity)
- Detects potential IP sweeps (multiple IPs from same subnet)
- Exports reputation database with high-threat IPs

*network_tracer*:
The network tracer monitors and analyzes network connections and traffic patterns. It reads system network state from /proc/net/tcp and /proc/net/tcp6. Key features include:

- Traces individual network connections (local/remote address, port, state)
- Parses connection data from system files in both IPv4 and IPv6 formats
- Maps connection states (ESTABLISHED, LISTEN, TIME_WAIT, etc.)
- Finds connections by IP address or port number
- Identifies all listening ports on the system
- Resolves IP addresses to hostnames via reverse DNS
- Provides connection statistics (total, by state)
- Traces connections for specific services (ssh, http, postgresql, redis, etc.)
- Links network connections to process inodes

*process_tracer*:
The process tracer monitors system processes, their resource usage, and relationships. It reads process information from the /proc filesystem. Key features include:

- Traces individual processes by PID with complete information
- Extracts process name, command line, and status details
- Collects CPU time statistics (user mode, kernel mode)
- Monitors memory usage (virtual size, resident set size)
- Tracks I/O statistics (read/write bytes and syscalls)
- Counts open file descriptors per process
- Maps process hierarchy (parent PID, child processes)
- Finds processes by name (searches cmdline and process name)
- Generates resource summaries across multiple processes
- Provides thread counts and context switch statistics

*event_tracer*:
The event tracer connects log events to their complete context, sources, and causality chains. It analyzes LogEvent objects to understand relationships between events. Key features include:

- Traces events to identify related services and components
- Identifies service patterns (nginx, django, postgresql, ssh, fail2ban, etc.)
- Calculates severity scores based on log level and event type
- Maps service relationships (e.g., fail2ban relates to ssh, nginx relates to django)
- Traces causality chains for common event sequences:
  - Failed logins leading to IP bans
  - Authentication failures and their root causes
  - Connection errors (timeout, refused) and their triggers
  - Memory/disk resource exhaustion events
- Extracts event patterns across multiple log entries
- Aggregates statistics by source, level, action, IP, user, and service

*error_tracer*:
The error tracer identifies, categorizes, and analyzes errors to find patterns and suggest recovery actions. It uses regex patterns to match common error types. Key features include:

- Recognizes error patterns across multiple categories:
  - Python exceptions and tracebacks
  - Database errors (connection, query, deadlock)
  - Memory errors (OOM, memory leaks)
  - Disk errors (full disk, I/O errors)
  - Network errors (timeout, refused, unreachable)
  - Permission and filesystem errors
  - Resource exhaustion (too many files)
  - System errors (segfaults, assertions)
- Extracts detailed error information (exception type, file path, line number, error code)
- Calculates error severity (0-100) based on level and content
- Identifies root cause hints for each error category
- Suggests recovery actions and preventive measures
- Analyzes error patterns over time (by type, category, severity)
- Detects recurring errors by signature
- Maintains error history for pattern analysis
