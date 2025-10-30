-- Logly Database Schema
-- Optimized for time-series data with proper indexing

-- System Metrics Table
-- Stores CPU, memory, disk, and load average metrics
CREATE TABLE IF NOT EXISTS system_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,  -- Unix timestamp for fast queries
    cpu_percent REAL,
    cpu_count INTEGER,
    memory_total INTEGER,
    memory_available INTEGER,
    memory_percent REAL,
    disk_total INTEGER,
    disk_used INTEGER,
    disk_percent REAL,
    disk_read_bytes INTEGER,
    disk_write_bytes INTEGER,
    load_1min REAL,
    load_5min REAL,
    load_15min REAL
);

-- Index on timestamp for fast time-range queries
CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp);

-- Network Metrics Table
-- Stores network traffic and connection statistics
CREATE TABLE IF NOT EXISTS network_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    bytes_sent INTEGER,
    bytes_recv INTEGER,
    packets_sent INTEGER,
    packets_recv INTEGER,
    errors_in INTEGER,
    errors_out INTEGER,
    drops_in INTEGER,
    drops_out INTEGER,
    connections_established INTEGER,
    connections_listen INTEGER,
    connections_time_wait INTEGER
);

CREATE INDEX IF NOT EXISTS idx_network_metrics_timestamp ON network_metrics(timestamp);

-- Log Events Table
-- Stores parsed log entries from various sources
CREATE TABLE IF NOT EXISTS log_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    source TEXT NOT NULL,  -- fail2ban, syslog, auth, etc.
    level TEXT,  -- INFO, WARNING, ERROR, CRITICAL
    message TEXT,
    ip_address TEXT,
    user TEXT,
    service TEXT,
    action TEXT,  -- banned, unbanned, failed_login, etc.
    metadata TEXT  -- JSON for additional fields
);

CREATE INDEX IF NOT EXISTS idx_log_events_timestamp ON log_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_log_events_source ON log_events(source);
CREATE INDEX IF NOT EXISTS idx_log_events_level ON log_events(level);
CREATE INDEX IF NOT EXISTS idx_log_events_action ON log_events(action);
-- Composite index for common queries (source + timestamp)
CREATE INDEX IF NOT EXISTS idx_log_events_source_timestamp ON log_events(source, timestamp);

-- Hourly Aggregates Table
-- Pre-computed hourly statistics for faster queries
CREATE TABLE IF NOT EXISTS hourly_aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hour_timestamp INTEGER NOT NULL UNIQUE,  -- Timestamp rounded to hour

    -- System metrics averages
    avg_cpu_percent REAL,
    max_cpu_percent REAL,
    avg_memory_percent REAL,
    max_memory_percent REAL,
    avg_disk_percent REAL,

    -- Network metrics totals
    total_bytes_sent INTEGER,
    total_bytes_recv INTEGER,
    total_packets_sent INTEGER,
    total_packets_recv INTEGER,

    -- Log event counts by type
    log_events_count INTEGER,
    failed_login_count INTEGER,
    banned_ip_count INTEGER,
    error_count INTEGER,
    warning_count INTEGER
);

CREATE INDEX IF NOT EXISTS idx_hourly_aggregates_hour ON hourly_aggregates(hour_timestamp);

-- Daily Aggregates Table
-- Pre-computed daily statistics for long-term analysis
CREATE TABLE IF NOT EXISTS daily_aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,  -- YYYY-MM-DD format

    -- System metrics averages
    avg_cpu_percent REAL,
    max_cpu_percent REAL,
    avg_memory_percent REAL,
    max_memory_percent REAL,
    avg_disk_percent REAL,

    -- Network metrics totals
    total_bytes_sent INTEGER,
    total_bytes_recv INTEGER,

    -- Log event counts
    log_events_count INTEGER,
    failed_login_count INTEGER,
    banned_ip_count INTEGER,
    error_count INTEGER,
    warning_count INTEGER,

    -- Unique IP addresses seen
    unique_ips_banned INTEGER,
    unique_users_failed INTEGER
);

CREATE INDEX IF NOT EXISTS idx_daily_aggregates_date ON daily_aggregates(date);

-- Metadata Table
-- Stores system information and collection metadata
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at INTEGER
);

-- Insert initial metadata
INSERT OR IGNORE INTO metadata (key, value, updated_at) VALUES
    ('schema_version', '2.0', strftime('%s', 'now')),
    ('created_at', strftime('%s', 'now'), strftime('%s', 'now')),
    ('hostname', '', strftime('%s', 'now'));

-- =============================================================================
-- TRACER SYSTEM TABLES
-- Comprehensive event tracing with process, network, IP, and error information
-- =============================================================================

-- Event Traces Table
-- Stores complete traces for important events
CREATE TABLE IF NOT EXISTS event_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,  -- Reference to log_events
    timestamp INTEGER NOT NULL,
    source TEXT,
    level TEXT,
    severity_score INTEGER,  -- 0-100

    -- Event context
    message TEXT,
    action TEXT,
    service TEXT,
    user TEXT,
    ip_address TEXT,

    -- Causality information
    root_cause TEXT,
    trigger_event TEXT,
    causality_chain TEXT,  -- JSON array

    -- Related services
    related_services TEXT,  -- JSON array

    -- Trace metadata
    tracer_version TEXT,
    tracers_used TEXT,  -- JSON array
    traced_at INTEGER,

    FOREIGN KEY (event_id) REFERENCES log_events(id)
);

CREATE INDEX IF NOT EXISTS idx_event_traces_timestamp ON event_traces(timestamp);
CREATE INDEX IF NOT EXISTS idx_event_traces_source ON event_traces(source);
CREATE INDEX IF NOT EXISTS idx_event_traces_level ON event_traces(level);
CREATE INDEX IF NOT EXISTS idx_event_traces_ip ON event_traces(ip_address);
CREATE INDEX IF NOT EXISTS idx_event_traces_severity ON event_traces(severity_score);
CREATE INDEX IF NOT EXISTS idx_event_traces_event_id ON event_traces(event_id);

-- Process Traces Table
-- Stores process information at time of event
CREATE TABLE IF NOT EXISTS process_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id INTEGER,  -- Reference to event_traces
    pid INTEGER NOT NULL,
    name TEXT,
    cmdline TEXT,
    state TEXT,
    parent_pid INTEGER,

    -- Resource usage
    memory_rss INTEGER,  -- Resident set size in KB
    memory_vm INTEGER,   -- Virtual memory size in KB
    cpu_utime INTEGER,
    cpu_stime INTEGER,
    threads INTEGER,

    -- I/O stats
    read_bytes INTEGER,
    write_bytes INTEGER,
    read_syscalls INTEGER,
    write_syscalls INTEGER,

    timestamp INTEGER NOT NULL,

    FOREIGN KEY (trace_id) REFERENCES event_traces(id)
);

CREATE INDEX IF NOT EXISTS idx_process_traces_trace_id ON process_traces(trace_id);
CREATE INDEX IF NOT EXISTS idx_process_traces_pid ON process_traces(pid);
CREATE INDEX IF NOT EXISTS idx_process_traces_name ON process_traces(name);
CREATE INDEX IF NOT EXISTS idx_process_traces_timestamp ON process_traces(timestamp);

-- Network Traces Table
-- Stores network connection information
CREATE TABLE IF NOT EXISTS network_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id INTEGER,  -- Reference to event_traces
    local_ip TEXT,
    local_port INTEGER,
    remote_ip TEXT,
    remote_port INTEGER,
    state TEXT,
    protocol TEXT DEFAULT 'tcp',
    timestamp INTEGER NOT NULL,

    FOREIGN KEY (trace_id) REFERENCES event_traces(id)
);

CREATE INDEX IF NOT EXISTS idx_network_traces_trace_id ON network_traces(trace_id);
CREATE INDEX IF NOT EXISTS idx_network_traces_remote_ip ON network_traces(remote_ip);
CREATE INDEX IF NOT EXISTS idx_network_traces_local_port ON network_traces(local_port);
CREATE INDEX IF NOT EXISTS idx_network_traces_timestamp ON network_traces(timestamp);

-- IP Reputation Table
-- Tracks IP addresses and their behavior
CREATE TABLE IF NOT EXISTS ip_reputation (
    ip TEXT PRIMARY KEY,
    type TEXT,  -- localhost, private, cloud, public
    is_whitelisted INTEGER DEFAULT 0,
    is_blacklisted INTEGER DEFAULT 0,
    threat_score INTEGER DEFAULT 0,  -- 0-100

    -- Activity counters
    first_seen INTEGER,
    last_seen INTEGER,
    total_events INTEGER DEFAULT 0,
    failed_login_count INTEGER DEFAULT 0,
    banned_count INTEGER DEFAULT 0,
    successful_login_count INTEGER DEFAULT 0,

    -- Geographic info (optional)
    country_code TEXT,
    city TEXT,
    asn TEXT,

    -- Tracking
    updated_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_ip_reputation_threat ON ip_reputation(threat_score);
CREATE INDEX IF NOT EXISTS idx_ip_reputation_type ON ip_reputation(type);
CREATE INDEX IF NOT EXISTS idx_ip_reputation_last_seen ON ip_reputation(last_seen);
CREATE INDEX IF NOT EXISTS idx_ip_reputation_blacklisted ON ip_reputation(is_blacklisted);

-- Error Traces Table
-- Stores detailed error analysis
CREATE TABLE IF NOT EXISTS error_traces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id INTEGER,  -- Reference to event_traces
    error_type TEXT,
    error_category TEXT,  -- application, database, resource, network, etc.
    exception_type TEXT,
    severity INTEGER,  -- 0-100

    -- Error location
    file_path TEXT,
    line_number INTEGER,
    error_code TEXT,

    -- Analysis
    has_stacktrace INTEGER DEFAULT 0,
    root_cause_hints TEXT,  -- JSON array
    recovery_suggestions TEXT,  -- JSON array

    timestamp INTEGER NOT NULL,

    FOREIGN KEY (trace_id) REFERENCES event_traces(id)
);

CREATE INDEX IF NOT EXISTS idx_error_traces_trace_id ON error_traces(trace_id);
CREATE INDEX IF NOT EXISTS idx_error_traces_type ON error_traces(error_type);
CREATE INDEX IF NOT EXISTS idx_error_traces_category ON error_traces(error_category);
CREATE INDEX IF NOT EXISTS idx_error_traces_severity ON error_traces(severity);
CREATE INDEX IF NOT EXISTS idx_error_traces_timestamp ON error_traces(timestamp);

-- Trace Patterns Table
-- Stores detected patterns across multiple events
CREATE TABLE IF NOT EXISTS trace_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_type TEXT NOT NULL,  -- error_spike, ip_sweep, service_degradation, etc.
    pattern_name TEXT,
    description TEXT,

    -- Pattern details
    affected_services TEXT,  -- JSON array
    affected_ips TEXT,  -- JSON array
    event_count INTEGER,
    severity INTEGER,  -- 0-100

    -- Time window
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    duration INTEGER,  -- seconds

    -- Analysis
    root_cause TEXT,
    recommendations TEXT,  -- JSON array

    detected_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trace_patterns_type ON trace_patterns(pattern_type);
CREATE INDEX IF NOT EXISTS idx_trace_patterns_time ON trace_patterns(start_time, end_time);
CREATE INDEX IF NOT EXISTS idx_trace_patterns_severity ON trace_patterns(severity);
CREATE INDEX IF NOT EXISTS idx_trace_patterns_detected ON trace_patterns(detected_at);
