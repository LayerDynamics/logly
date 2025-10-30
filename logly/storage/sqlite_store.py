"""
SQLite storage implementation for Logly
Optimized for time-series data with proper indexing
"""

import sqlite3
import json
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
import time

from logly.storage.models import SystemMetric, NetworkMetric, LogEvent
from logly.utils.logger import get_logger
from logly.utils.paths import get_db_path, validate_db_path
from logly.utils.create_db import db_exists, initialize_db_if_needed


logger = get_logger(__name__)


class SQLiteStore:
    """SQLite-based storage for metrics and logs"""

    def __init__(self, db_path: str):
        """
        Initialize SQLite storage

        Args:
            db_path: Path to SQLite database file

        Raises:
            ValueError: If db_path does not match the hardcoded expected path
        """
        # ENFORCE HARDCODED PATH: Validate that the provided path matches expected path
        expected_path = get_db_path()
        if not validate_db_path(db_path):
            raise ValueError(
                f"Database path must be {expected_path}. "
                f"Got: {db_path}. "
                f"Database paths are HARDCODED and cannot be changed."
            )

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # In test mode, directly check if the provided path exists
        # In production mode, use db_exists() which checks the hardcoded path
        test_mode = os.environ.get("LOGLY_TEST_MODE") == "1"

        if test_mode:
            # Test mode: check the specific path provided
            if not self.db_path.exists():
                logger.info(f"Test database does not exist, initializing at {self.db_path}")
                # Don't call initialize_db_if_needed() in test mode as it uses hardcoded path
        else:
            # Production mode: use the standard initialization
            if not db_exists():
                logger.info("Database does not exist, initializing...")
                initialize_db_if_needed()

        self._init_database()

    def _init_database(self):
        """
        Initialize database with schema

        Note: This method now checks if tables exist before executing schema.
        The heavy lifting of database creation is done by create_db.py module.
        """
        with self._connection() as conn:
            # Check if tables already exist
            cursor = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            )
            table_count = cursor.fetchone()[0]

            # Only run schema if no tables exist
            if table_count == 0:
                schema_path = Path(__file__).parent / "schema.sql"
                with open(schema_path, "r") as f:
                    schema = f.read()
                conn.executescript(schema)
                conn.commit()
                logger.info(f"Database schema initialized at {self.db_path}")
            else:
                logger.debug(f"Database already initialized with {table_count} tables")

    @contextmanager
    def _connection(self):
        """Context manager for database connections with concurrency support"""
        # Configure connection for thread safety and concurrency
        # timeout=60 waits up to 60 seconds if database is locked
        # check_same_thread=False allows connection to be used by different threads (needed for testing)
        max_retries = 5
        retry_delay = 0.1  # Start with 100ms

        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=60.0,  # Increased from 30 to 60 seconds
                    check_same_thread=False,
                    uri=True  # Enable URI mode for better file handling
                )
                conn.row_factory = sqlite3.Row

                # Enable WAL mode for better concurrent read/write performance
                # WAL allows multiple readers and one writer simultaneously
                try:
                    # Force WAL mode before any operations
                    conn.execute("PRAGMA journal_mode=WAL").fetchone()
                    conn.execute("PRAGMA synchronous=NORMAL")
                    conn.execute("PRAGMA cache_size=10000")
                    conn.execute("PRAGMA temp_store=MEMORY")
                    conn.execute("PRAGMA busy_timeout=60000")  # 60 seconds in milliseconds
                    conn.execute("PRAGMA wal_autocheckpoint=1000")  # Checkpoint every 1000 pages
                    conn.commit()  # Commit pragma changes
                except sqlite3.OperationalError as e:
                    # If WAL mode fails, continue with default journal mode
                    logger.warning(f"Could not set WAL mode: {e}")

                try:
                    yield conn
                finally:
                    conn.close()
                break  # Success, exit retry loop

            except sqlite3.OperationalError as e:
                if "unable to open database file" in str(e) and attempt < max_retries - 1:
                    # Retry with exponential backoff
                    logger.debug(f"Database connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                else:
                    # Final attempt failed or different error
                    raise

    # System Metrics Operations
    def insert_system_metric(self, metric: SystemMetric) -> int:
        """Insert a system metric record"""
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO system_metrics (
                    timestamp, cpu_percent, cpu_count, memory_total,
                    memory_available, memory_percent, disk_total, disk_used,
                    disk_percent, disk_read_bytes, disk_write_bytes,
                    load_1min, load_5min, load_15min
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    metric.timestamp,
                    metric.cpu_percent,
                    metric.cpu_count,
                    metric.memory_total,
                    metric.memory_available,
                    metric.memory_percent,
                    metric.disk_total,
                    metric.disk_used,
                    metric.disk_percent,
                    metric.disk_read_bytes,
                    metric.disk_write_bytes,
                    metric.load_1min,
                    metric.load_5min,
                    metric.load_15min,
                ),
            )
            conn.commit()
            return cursor.lastrowid or 0

    def get_system_metrics(
        self, start_time: int, end_time: int, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get system metrics within time range

        Args:
            start_time: Unix timestamp start
            end_time: Unix timestamp end
            limit: Optional limit on number of results
        """
        query = """
            SELECT * FROM system_metrics
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp DESC
        """
        if limit:
            query += f" LIMIT {limit}"

        with self._connection() as conn:
            cursor = conn.execute(query, (start_time, end_time))
            return [dict(row) for row in cursor.fetchall()]

    # Network Metrics Operations
    def insert_network_metric(self, metric: NetworkMetric) -> int:
        """Insert a network metric record"""
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO network_metrics (
                    timestamp, bytes_sent, bytes_recv, packets_sent,
                    packets_recv, errors_in, errors_out, drops_in,
                    drops_out, connections_established, connections_listen,
                    connections_time_wait
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    metric.timestamp,
                    metric.bytes_sent,
                    metric.bytes_recv,
                    metric.packets_sent,
                    metric.packets_recv,
                    metric.errors_in,
                    metric.errors_out,
                    metric.drops_in,
                    metric.drops_out,
                    metric.connections_established,
                    metric.connections_listen,
                    metric.connections_time_wait,
                ),
            )
            conn.commit()
            return cursor.lastrowid or 0

    def get_network_metrics(
        self, start_time: int, end_time: int, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get network metrics within time range"""
        query = """
            SELECT * FROM network_metrics
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp DESC
        """
        if limit:
            query += f" LIMIT {limit}"

        with self._connection() as conn:
            cursor = conn.execute(query, (start_time, end_time))
            return [dict(row) for row in cursor.fetchall()]

    # Log Events Operations
    def insert_log_event(self, event: LogEvent) -> int:
        """Insert a log event record"""
        data = event.to_dict()

        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO log_events (
                    timestamp, source, level, message, ip_address,
                    user, service, action, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    data["timestamp"],
                    data["source"],
                    data.get("level"),
                    data["message"],
                    data.get("ip_address"),
                    data.get("user"),
                    data.get("service"),
                    data.get("action"),
                    data.get("metadata"),
                ),
            )
            conn.commit()
            return cursor.lastrowid or 0

    def get_log_events(
        self,
        start_time: int,
        end_time: int,
        source: Optional[str] = None,
        level: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get log events within time range with optional filters

        Args:
            start_time: Unix timestamp start
            end_time: Unix timestamp end
            source: Filter by log source (fail2ban, syslog, etc.)
            level: Filter by log level (INFO, WARNING, ERROR)
            limit: Optional limit on number of results
        """
        query = "SELECT * FROM log_events WHERE timestamp BETWEEN ? AND ?"
        params: List[Any] = [start_time, end_time]

        if source:
            query += " AND source = ?"
            params.append(source)

        if level:
            query += " AND level = ?"
            params.append(level)

        query += " ORDER BY timestamp DESC"

        if limit:
            query += f" LIMIT {limit}"

        with self._connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    # Aggregation Operations
    def compute_hourly_aggregates(self, hour_timestamp: int):
        """
        Compute and store hourly aggregates for the given hour

        Args:
            hour_timestamp: Unix timestamp rounded to the hour
        """
        hour_end = hour_timestamp + 3600  # One hour later

        with self._connection() as conn:
            # Compute system metrics aggregates
            sys_stats = conn.execute(
                """
                SELECT
                    AVG(cpu_percent) as avg_cpu,
                    MAX(cpu_percent) as max_cpu,
                    AVG(memory_percent) as avg_mem,
                    MAX(memory_percent) as max_mem,
                    AVG(disk_percent) as avg_disk
                FROM system_metrics
                WHERE timestamp >= ? AND timestamp < ?
            """,
                (hour_timestamp, hour_end),
            ).fetchone()

            # Compute network metrics aggregates
            net_stats = conn.execute(
                """
                SELECT
                    SUM(bytes_sent) as total_sent,
                    SUM(bytes_recv) as total_recv,
                    SUM(packets_sent) as total_packets_sent,
                    SUM(packets_recv) as total_packets_recv
                FROM network_metrics
                WHERE timestamp >= ? AND timestamp < ?
            """,
                (hour_timestamp, hour_end),
            ).fetchone()

            # Compute log event counts
            log_stats = conn.execute(
                """
                SELECT
                    COUNT(*) as total_events,
                    SUM(CASE WHEN action = 'failed_login' THEN 1 ELSE 0 END) as failed_logins,
                    SUM(CASE WHEN action = 'banned' THEN 1 ELSE 0 END) as banned_ips,
                    SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as errors,
                    SUM(CASE WHEN level = 'WARNING' THEN 1 ELSE 0 END) as warnings
                FROM log_events
                WHERE timestamp >= ? AND timestamp < ?
            """,
                (hour_timestamp, hour_end),
            ).fetchone()

            # Only insert if we have at least some data
            # COUNT(*) always returns a value, but AVG/SUM return None if no rows
            if log_stats["total_events"] > 0 or sys_stats["avg_cpu"] is not None or net_stats["total_sent"] is not None:
                # Insert or replace hourly aggregate
                conn.execute(
                    """
                    INSERT OR REPLACE INTO hourly_aggregates (
                        hour_timestamp, avg_cpu_percent, max_cpu_percent,
                        avg_memory_percent, max_memory_percent, avg_disk_percent,
                        total_bytes_sent, total_bytes_recv, total_packets_sent,
                        total_packets_recv, log_events_count, failed_login_count,
                        banned_ip_count, error_count, warning_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        hour_timestamp,
                        sys_stats["avg_cpu"],
                        sys_stats["max_cpu"],
                        sys_stats["avg_mem"],
                        sys_stats["max_mem"],
                        sys_stats["avg_disk"],
                        net_stats["total_sent"] or 0,
                        net_stats["total_recv"] or 0,
                        net_stats["total_packets_sent"] or 0,
                        net_stats["total_packets_recv"] or 0,
                        log_stats["total_events"] or 0,
                        log_stats["failed_logins"] or 0,
                        log_stats["banned_ips"] or 0,
                        log_stats["errors"] or 0,
                        log_stats["warnings"] or 0,
                    ),
                )
                conn.commit()
                logger.debug(f"Computed hourly aggregates for timestamp {hour_timestamp}")
            else:
                logger.debug(f"No data to aggregate for hour {hour_timestamp}")

    def compute_daily_aggregates(self, date_str: str):
        """
        Compute and store daily aggregates for the given date

        Args:
            date_str: Date in YYYY-MM-DD format
        """
        with self._connection() as conn:
            # Use hourly aggregates if available, otherwise raw data
            sys_stats = conn.execute(
                """
                SELECT
                    AVG(avg_cpu_percent) as avg_cpu,
                    MAX(max_cpu_percent) as max_cpu,
                    AVG(avg_memory_percent) as avg_mem,
                    MAX(max_memory_percent) as max_mem,
                    AVG(avg_disk_percent) as avg_disk,
                    SUM(total_bytes_sent) as total_sent,
                    SUM(total_bytes_recv) as total_recv
                FROM hourly_aggregates
                WHERE date(hour_timestamp, 'unixepoch') = ?
            """,
                (date_str,),
            ).fetchone()

            log_stats = conn.execute(
                """
                SELECT
                    SUM(log_events_count) as total_events,
                    SUM(failed_login_count) as failed_logins,
                    SUM(banned_ip_count) as banned_ips,
                    SUM(error_count) as errors,
                    SUM(warning_count) as warnings
                FROM hourly_aggregates
                WHERE date(hour_timestamp, 'unixepoch') = ?
            """,
                (date_str,),
            ).fetchone()

            # Count unique IPs and users for the day
            unique_stats = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT ip_address) as unique_banned_ips,
                    COUNT(DISTINCT user) as unique_failed_users
                FROM log_events
                WHERE date(timestamp, 'unixepoch') = ?
            """,
                (date_str,),
            ).fetchone()

            # Insert or replace daily aggregate
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_aggregates (
                    date, avg_cpu_percent, max_cpu_percent, avg_memory_percent,
                    max_memory_percent, avg_disk_percent, total_bytes_sent,
                    total_bytes_recv, log_events_count, failed_login_count,
                    banned_ip_count, error_count, warning_count,
                    unique_ips_banned, unique_users_failed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    date_str,
                    sys_stats["avg_cpu"],
                    sys_stats["max_cpu"],
                    sys_stats["avg_mem"],
                    sys_stats["max_mem"],
                    sys_stats["avg_disk"],
                    sys_stats["total_sent"],
                    sys_stats["total_recv"],
                    log_stats["total_events"],
                    log_stats["failed_logins"],
                    log_stats["banned_ips"],
                    log_stats["errors"],
                    log_stats["warnings"],
                    unique_stats["unique_banned_ips"],
                    unique_stats["unique_failed_users"],
                ),
            )
            conn.commit()

        logger.debug(f"Computed daily aggregates for date {date_str}")

    # Maintenance Operations
    def cleanup_old_data(self, retention_days: int):
        """
        Delete data older than retention period

        Args:
            retention_days: Number of days to retain data
        """
        cutoff_time = int(time.time()) - (retention_days * 86400)

        with self._connection() as conn:
            cursor = conn.execute(
                "DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_time,)
            )
            deleted_sys = cursor.rowcount

            cursor = conn.execute(
                "DELETE FROM network_metrics WHERE timestamp < ?", (cutoff_time,)
            )
            deleted_net = cursor.rowcount

            cursor = conn.execute(
                "DELETE FROM log_events WHERE timestamp < ?", (cutoff_time,)
            )
            deleted_log = cursor.rowcount

            conn.commit()

        logger.info(
            f"Cleaned up old data: {deleted_sys} system metrics, "
            f"{deleted_net} network metrics, {deleted_log} log events"
        )

    def get_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        with self._connection() as conn:
            stats = {}

            for table in [
                "system_metrics",
                "network_metrics",
                "log_events",
                "hourly_aggregates",
                "daily_aggregates",
                "event_traces",
                "process_traces",
                "network_traces",
                "error_traces",
                "ip_reputation",
                "trace_patterns"
            ]:
                try:
                    count = conn.execute(
                        f"SELECT COUNT(*) as count FROM {table}"
                    ).fetchone()
                    stats[table] = count["count"]
                except Exception:
                    # Table might not exist yet
                    stats[table] = 0

            # Database size
            size = self.db_path.stat().st_size
            stats["database_size_mb"] = round(size / (1024 * 1024), 2)

            return stats

    # =============================================================================
    # TRACER SYSTEM METHODS
    # Integrated from TraceStore for unified database access
    # =============================================================================

    def insert_event_trace(self, trace: Dict[str, Any]) -> int:
        """
        Insert an event trace

        Args:
            trace: Trace dictionary from TracerCollector

        Returns:
            Trace ID
        """
        with self._connection() as conn:
            cursor = conn.execute("""
                INSERT INTO event_traces (
                    event_id, timestamp, source, level, severity_score,
                    message, action, service, user, ip_address,
                    root_cause, trigger_event, causality_chain,
                    related_services, tracer_version, tracers_used, traced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trace.get('event_id'),
                trace['timestamp'],
                trace.get('source'),
                trace.get('level'),
                trace.get('severity_score', 0),
                trace.get('message'),
                trace.get('action'),
                trace.get('service'),
                trace.get('user'),
                trace.get('ip_address'),
                trace.get('causality', {}).get('root_cause') if trace.get('causality') else None,
                trace.get('causality', {}).get('trigger') if trace.get('causality') else None,
                json.dumps(trace.get('causality', {}).get('chain', [])),
                json.dumps(trace.get('related_services', [])),
                trace.get('trace_metadata', {}).get('tracer_version'),
                json.dumps(trace.get('trace_metadata', {}).get('tracers_used', [])),
                trace.get('trace_metadata', {}).get('traced_at', trace['timestamp'])
            ))
            trace_id = cursor.lastrowid
            assert trace_id is not None, "Failed to get trace_id from insert"

            # Insert related process traces
            if trace.get('processes'):
                self._insert_process_traces(conn, trace_id, trace['processes'], trace['timestamp'])

            # Insert network traces
            if trace.get('network_connections'):
                self._insert_network_traces(conn, trace_id, trace['network_connections'], trace['timestamp'])

            # Insert error trace
            if trace.get('error_info'):
                self._insert_error_trace(conn, trace_id, trace['error_info'])

            # Update IP reputation
            if trace.get('ip_info'):
                self._update_ip_reputation(conn, trace['ip_info'])

            conn.commit()
            return trace_id

    def _insert_process_traces(self, conn, trace_id: int, processes: List[Dict], timestamp: int):
        """Insert process trace records"""
        for proc in processes:
            status = proc.get('status', {})
            stats = proc.get('stats', {})
            io_stats = proc.get('io', {})

            conn.execute("""
                INSERT INTO process_traces (
                    trace_id, pid, name, cmdline, state, parent_pid,
                    memory_rss, memory_vm, cpu_utime, cpu_stime, threads,
                    read_bytes, write_bytes, read_syscalls, write_syscalls, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trace_id, proc['pid'], proc.get('name'), proc.get('cmdline'),
                status.get('state'), proc.get('parent_pid'),
                status.get('vm_rss', 0), status.get('vm_size', 0),
                stats.get('utime', 0), stats.get('stime', 0), status.get('threads', 0),
                io_stats.get('read_bytes', 0), io_stats.get('write_bytes', 0),
                io_stats.get('read_syscalls', 0), io_stats.get('write_syscalls', 0),
                timestamp
            ))

    def _insert_network_traces(self, conn, trace_id: int, connections: List[Dict], timestamp: int):
        """Insert network trace records"""
        for conn_info in connections:
            conn.execute("""
                INSERT INTO network_traces (
                    trace_id, local_ip, local_port, remote_ip, remote_port,
                    state, protocol, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trace_id,
                conn_info.get('local_ip'),
                conn_info.get('local_port'),
                conn_info.get('remote_ip'),
                conn_info.get('remote_port'),
                conn_info.get('state'),
                conn_info.get('protocol', 'tcp'),
                timestamp
            ))

    def _insert_error_trace(self, conn, trace_id: int, error_info: Dict):
        """Insert error trace record"""
        conn.execute("""
            INSERT INTO error_traces (
                trace_id, error_type, error_category, exception_type, severity,
                file_path, line_number, error_code, has_stacktrace,
                root_cause_hints, recovery_suggestions, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trace_id,
            error_info.get('error_type'),
            error_info.get('error_category'),
            error_info.get('exception_type'),
            error_info.get('severity', 0),
            error_info.get('file_path'),
            error_info.get('line_number'),
            error_info.get('error_code'),
            1 if error_info.get('has_stacktrace') else 0,
            json.dumps(error_info.get('root_cause_hints', [])),
            json.dumps(error_info.get('recovery_suggestions', [])),
            error_info.get('timestamp', int(time.time()))
        ))

    def _update_ip_reputation(self, conn, ip_info: Dict):
        """Update or insert IP reputation"""
        ip = ip_info['ip']
        now = int(time.time())

        # Check if exists
        existing = conn.execute(
            "SELECT * FROM ip_reputation WHERE ip = ?", (ip,)
        ).fetchone()

        if existing:
            # Update
            conn.execute("""
                UPDATE ip_reputation SET
                    type = ?,
                    is_whitelisted = ?,
                    is_blacklisted = ?,
                    threat_score = ?,
                    last_seen = ?,
                    total_events = total_events + 1,
                    failed_login_count = ?,
                    banned_count = ?,
                    updated_at = ?
                WHERE ip = ?
            """, (
                ip_info.get('type'),
                1 if ip_info.get('is_whitelisted') else 0,
                1 if ip_info.get('is_known_malicious') else 0,
                ip_info.get('threat_score', 0),
                now,
                ip_info.get('failed_login_count', 0),
                ip_info.get('banned_count', 0),
                now,
                ip
            ))
        else:
            # Insert
            conn.execute("""
                INSERT INTO ip_reputation (
                    ip, type, is_whitelisted, is_blacklisted, threat_score,
                    first_seen, last_seen, total_events, failed_login_count,
                    banned_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ip,
                ip_info.get('type'),
                1 if ip_info.get('is_whitelisted') else 0,
                1 if ip_info.get('is_known_malicious') else 0,
                ip_info.get('threat_score', 0),
                now,
                now,
                1,
                ip_info.get('failed_login_count', 0),
                ip_info.get('banned_count', 0),
                now
            ))

    def get_traces(self, start_time: int, end_time: int,
                   source: Optional[str] = None,
                   min_severity: Optional[int] = None,
                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get event traces within time range

        Args:
            start_time: Start timestamp
            end_time: End timestamp
            source: Optional source filter
            min_severity: Minimum severity score
            limit: Result limit

        Returns:
            List of traces
        """
        query = "SELECT * FROM event_traces WHERE timestamp BETWEEN ? AND ?"
        params: List[Any] = [start_time, end_time]

        if source:
            query += " AND source = ?"
            params.append(source)

        if min_severity is not None:
            query += " AND severity_score >= ?"
            params.append(min_severity)

        query += " ORDER BY timestamp DESC"

        if limit:
            query += f" LIMIT {limit}"

        with self._connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_ip_reputation(self, ip_address: str) -> Optional[Dict[str, Any]]:
        """Get IP reputation info"""
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM ip_reputation WHERE ip = ?",
                (ip_address,)
            ).fetchone()

            if row:
                return dict(row)
        return None

    def get_high_threat_ips(self, threshold: int = 70) -> List[Dict[str, Any]]:
        """Get IPs with high threat scores"""
        with self._connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM ip_reputation
                WHERE threat_score >= ?
                ORDER BY threat_score DESC, last_seen DESC
            """, (threshold,))
            return [dict(row) for row in cursor.fetchall()]

    def get_error_patterns(self, start_time: int, end_time: int) -> Dict[str, Any]:
        """Get error pattern statistics"""
        with self._connection() as conn:
            # Count by error type
            by_type = conn.execute("""
                SELECT error_type, COUNT(*) as count
                FROM error_traces
                WHERE timestamp BETWEEN ? AND ?
                GROUP BY error_type
                ORDER BY count DESC
            """, (start_time, end_time)).fetchall()

            # Count by category
            by_category = conn.execute("""
                SELECT error_category, COUNT(*) as count
                FROM error_traces
                WHERE timestamp BETWEEN ? AND ?
                GROUP BY error_category
                ORDER BY count DESC
            """, (start_time, end_time)).fetchall()

            return {
                'by_type': [dict(row) for row in by_type],
                'by_category': [dict(row) for row in by_category],
            }

    def get_error_traces(
        self,
        start_time: int,
        end_time: int,
        category: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get individual error trace records within time range

        Args:
            start_time: Start timestamp
            end_time: End timestamp
            category: Optional error category filter
            limit: Result limit

        Returns:
            List of error traces
        """
        query = "SELECT * FROM error_traces WHERE timestamp BETWEEN ? AND ?"
        params: List[Any] = [start_time, end_time]

        if category:
            query += " AND error_category = ?"
            params.append(category)

        query += " ORDER BY timestamp DESC"

        if limit:
            query += f" LIMIT {limit}"

        with self._connection() as conn:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
