"""
Log parser for fail2ban, syslog, auth.log, and other system logs
Uses regex patterns to extract structured data
"""

import re
from pathlib import Path
from typing import List, Optional

from logly.collectors.base_collector import BaseCollector
from logly.storage.models import LogEvent
from logly.utils.logger import get_logger


logger = get_logger(__name__)


class LogParser(BaseCollector):
    """Parses system logs and extracts structured events"""

    # Regex patterns for different log types
    PATTERNS = {
        "fail2ban_ban": re.compile(
            r"\[(?P<jail>[\w-]+)\]\s+(?P<action>Ban|Unban)\s+(?P<ip>[\d.]+)"
        ),
        "fail2ban_found": re.compile(r"\[(?P<jail>[\w-]+)\]\s+Found\s+(?P<ip>[\d.]+)"),
        "auth_failed": re.compile(
            r"Failed password for (?:invalid user )?(?P<user>\w+) from (?P<ip>[\d.]+)"
        ),
        "auth_accepted": re.compile(
            r"Accepted (?P<method>\w+) for (?P<user>\w+) from (?P<ip>[\d.]+)"
        ),
        "syslog_error": re.compile(
            r"(?P<timestamp>\w+\s+\d+\s+[\d:]+)\s+(?P<host>\S+)\s+(?P<service>\S+?)(?:\[\d+\])?\s*:\s*(?P<message>.*)"
        ),
    }

    def __init__(self, config: dict):
        super().__init__(config)
        self.log_sources = config.get("sources", {})
        self._file_positions = {}  # Track file positions to read only new lines

    def collect(self) -> List[LogEvent]:
        """
        Collect and parse log events from all configured sources

        Returns:
            List of LogEvent objects
        """
        events = []

        for source_name, source_config in self.log_sources.items():
            if not source_config.get("enabled", True):
                continue

            log_path = source_config.get("path")
            if not log_path or not Path(log_path).exists():
                logger.debug(f"Log file not found: {log_path}")
                continue

            try:
                source_events = self._parse_log_file(source_name, log_path)
                events.extend(source_events)
            except Exception as e:
                logger.error(f"Error parsing {source_name} at {log_path}: {e}")

        return events

    def _parse_log_file(self, source: str, log_path: str) -> List[LogEvent]:
        """
        Parse a log file and extract events

        Args:
            source: Source name (fail2ban, syslog, auth, etc.)
            log_path: Path to log file

        Returns:
            List of LogEvent objects
        """
        events = []
        path = Path(log_path)

        # Get last read position
        last_pos = self._file_positions.get(log_path, 0)

        try:
            # Check if file was rotated (size is smaller than last position)
            current_size = path.stat().st_size
            if current_size < last_pos:
                logger.info(f"Log file {log_path} was rotated, starting from beginning")
                last_pos = 0

            with open(log_path, "r") as f:
                # Seek to last position
                f.seek(last_pos)

                # Read new lines
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Parse line based on source type
                    event = self._parse_line(source, line)
                    if event:
                        events.append(event)

                # Update file position
                self._file_positions[log_path] = f.tell()

        except Exception as e:
            logger.error(f"Error reading log file {log_path}: {e}")

        return events

    def _parse_line(self, source: str, line: str) -> Optional[LogEvent]:
        """
        Parse a single log line

        Args:
            source: Source name
            line: Log line

        Returns:
            LogEvent or None if line doesn't match patterns
        """
        # Determine which parser to use based on source
        if source == "fail2ban":
            return self._parse_fail2ban(line)
        elif source == "auth":
            return self._parse_auth_log(line)
        elif source == "syslog":
            return self._parse_syslog(line)
        elif source == "django":
            return self._parse_django_log(line)
        elif source == "nginx":
            return self._parse_nginx_log(line)
        else:
            # Generic parser
            return self._parse_generic(source, line)

    def _parse_fail2ban(self, line: str) -> Optional[LogEvent]:
        """Parse fail2ban log line"""
        # Check for ban/unban
        match = self.PATTERNS["fail2ban_ban"].search(line)
        if match:
            action = match.group("action").lower()
            return LogEvent.now(
                source="fail2ban",
                message=line,
                level="WARNING" if action == "ban" else "INFO",
                ip_address=match.group("ip"),
                service=match.group("jail"),
                action=action,
                metadata={"jail": match.group("jail")},
            )

        # Check for "Found" entries
        match = self.PATTERNS["fail2ban_found"].search(line)
        if match:
            return LogEvent.now(
                source="fail2ban",
                message=line,
                level="INFO",
                ip_address=match.group("ip"),
                service=match.group("jail"),
                action="found",
                metadata={"jail": match.group("jail")},
            )

        return None

    def _parse_auth_log(self, line: str) -> Optional[LogEvent]:
        """Parse auth.log line"""
        # Check for failed password
        match = self.PATTERNS["auth_failed"].search(line)
        if match:
            return LogEvent.now(
                source="auth",
                message=line,
                level="WARNING",
                ip_address=match.group("ip"),
                user=match.group("user"),
                service="ssh",
                action="failed_login",
            )

        # Check for accepted authentication
        match = self.PATTERNS["auth_accepted"].search(line)
        if match:
            return LogEvent.now(
                source="auth",
                message=line,
                level="INFO",
                ip_address=match.group("ip"),
                user=match.group("user"),
                service="ssh",
                action="successful_login",
                metadata={"method": match.group("method")},
            )

        return None

    def _parse_syslog(self, line: str) -> Optional[LogEvent]:
        """Parse syslog line"""
        # Look for error/warning keywords
        level = "INFO"
        if "error" in line.lower() or "fail" in line.lower():
            level = "ERROR"
        elif "warning" in line.lower() or "warn" in line.lower():
            level = "WARNING"

        match = self.PATTERNS["syslog_error"].search(line)
        if match:
            return LogEvent.now(
                source="syslog",
                message=match.group("message"),
                level=level,
                service=match.group("service"),
                metadata={"host": match.group("host"), "full_line": line},
            )

        # If no pattern match but contains keywords, still capture
        if level in ["ERROR", "WARNING"]:
            return LogEvent.now(source="syslog", message=line, level=level)

        return None

    def _parse_django_log(self, line: str) -> Optional[LogEvent]:
        """Parse Django log line"""
        # Django logs typically have format: [LEVEL] message
        level_match = re.match(r"\[(?P<level>\w+)\]\s+(?P<message>.*)", line)
        if level_match:
            level = level_match.group("level").upper()
            message = level_match.group("message")

            return LogEvent.now(
                source="django", message=message, level=level, service="django"
            )

        # If no level marker, treat as INFO
        if line.strip():
            return LogEvent.now(
                source="django", message=line, level="INFO", service="django"
            )

        return None

    def _parse_nginx_log(self, line: str) -> Optional[LogEvent]:
        """Parse nginx access log line"""
        # Nginx access log format: IP - - [timestamp] "request" status size "referer" "user-agent"
        nginx_pattern = re.compile(
            r"(?P<ip>[\d.]+)\s+-\s+-\s+\[(?P<timestamp>[^\]]+)\]\s+"
            r'"(?P<request>[^"]*)"\s+(?P<status>\d+)\s+(?P<size>\d+)'
        )

        match = nginx_pattern.search(line)
        if match:
            status = int(match.group("status"))
            level = "ERROR" if status >= 500 else "WARNING" if status >= 400 else "INFO"

            return LogEvent.now(
                source="nginx",
                message=line,
                level=level,
                ip_address=match.group("ip"),
                service="nginx",
                action="http_request",
                metadata={
                    "request": match.group("request"),
                    "status": status,
                    "size": int(match.group("size")),
                },
            )

        return None

    def _parse_generic(self, source: str, line: str) -> Optional[LogEvent]:
        """Generic parser for unknown log formats"""
        if not line.strip():
            return None

        # Determine level from keywords
        level = "INFO"
        if any(word in line.lower() for word in ["critical", "fatal"]):
            level = "CRITICAL"
        elif any(word in line.lower() for word in ["error", "err"]):
            level = "ERROR"
        elif any(word in line.lower() for word in ["warning", "warn"]):
            level = "WARNING"

        return LogEvent.now(source=source, message=line, level=level)

    def validate(self) -> bool:
        """Validate at least one log source is accessible"""
        for source_config in self.log_sources.values():
            if source_config.get("enabled"):
                log_path = source_config.get("path")
                if log_path and Path(log_path).exists():
                    return True
        return False
