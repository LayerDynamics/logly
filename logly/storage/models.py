"""
Data models for Logly metrics and log events
"""

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import time
import json


@dataclass
class SystemMetric:
    """System metrics data model"""

    timestamp: int
    cpu_percent: Optional[float] = None
    cpu_count: Optional[int] = None
    memory_total: Optional[int] = None
    memory_available: Optional[int] = None
    memory_percent: Optional[float] = None
    disk_total: Optional[int] = None
    disk_used: Optional[int] = None
    disk_percent: Optional[float] = None
    disk_read_bytes: Optional[int] = None
    disk_write_bytes: Optional[int] = None
    load_1min: Optional[float] = None
    load_5min: Optional[float] = None
    load_15min: Optional[float] = None

    @classmethod
    def now(cls, **kwargs):
        """Create a metric with current timestamp"""
        return cls(timestamp=int(time.time()), **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class NetworkMetric:
    """Network metrics data model"""

    timestamp: int
    bytes_sent: Optional[int] = None
    bytes_recv: Optional[int] = None
    packets_sent: Optional[int] = None
    packets_recv: Optional[int] = None
    errors_in: Optional[int] = None
    errors_out: Optional[int] = None
    drops_in: Optional[int] = None
    drops_out: Optional[int] = None
    connections_established: Optional[int] = None
    connections_listen: Optional[int] = None
    connections_time_wait: Optional[int] = None

    @classmethod
    def now(cls, **kwargs):
        """Create a metric with current timestamp"""
        return cls(timestamp=int(time.time()), **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class LogEvent:
    """Log event data model"""

    timestamp: int
    source: str
    message: str
    level: Optional[str] = None
    ip_address: Optional[str] = None
    user: Optional[str] = None
    service: Optional[str] = None
    action: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @classmethod
    def now(cls, source: str, message: str, **kwargs):
        """Create a log event with current timestamp"""
        return cls(timestamp=int(time.time()), source=source, message=message, **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, serializing metadata as JSON"""
        data = asdict(self)
        if self.metadata:
            data["metadata"] = json.dumps(self.metadata)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEvent":
        """Create LogEvent from dictionary, deserializing metadata"""
        if "metadata" in data and isinstance(data["metadata"], str):
            data["metadata"] = json.loads(data["metadata"])
        return cls(**data)


@dataclass
class EventTrace:
    """Event trace data model"""
    timestamp: int
    source: str
    message: str
    severity_score: int = 0
    event_id: Optional[int] = None
    level: Optional[str] = None
    action: Optional[str] = None
    service: Optional[str] = None
    user: Optional[str] = None
    ip_address: Optional[str] = None
    root_cause: Optional[str] = None
    trigger_event: Optional[str] = None
    causality_chain: Optional[list] = None
    related_services: Optional[list] = None
    tracer_version: Optional[str] = None
    tracers_used: Optional[list] = None
    traced_at: Optional[int] = None

    @classmethod
    def from_trace_dict(cls, trace: Dict[str, Any]) -> "EventTrace":
        """Create EventTrace from tracer collector output"""
        causality = trace.get("causality", {})
        metadata = trace.get("trace_metadata", {})

        return cls(
            event_id=trace.get("event_id"),
            timestamp=trace["timestamp"],
            source=trace.get("source", ""),
            message=trace.get("message", ""),
            level=trace.get("level"),
            severity_score=trace.get("severity_score", 0),
            action=trace.get("action"),
            service=trace.get("service"),
            user=trace.get("user"),
            ip_address=trace.get("ip_address"),
            root_cause=causality.get("root_cause") if causality else None,
            trigger_event=causality.get("trigger") if causality else None,
            causality_chain=causality.get("chain") if causality else None,
            related_services=trace.get("related_services"),
            tracer_version=metadata.get("tracer_version"),
            tracers_used=metadata.get("tracers_used"),
            traced_at=metadata.get("traced_at", trace["timestamp"])
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "source": self.source,
            "level": self.level,
            "severity_score": self.severity_score,
            "message": self.message,
            "action": self.action,
            "service": self.service,
            "user": self.user,
            "ip_address": self.ip_address,
            "root_cause": self.root_cause,
            "trigger_event": self.trigger_event,
            "causality_chain": json.dumps(self.causality_chain) if self.causality_chain else None,
            "related_services": json.dumps(self.related_services) if self.related_services else None,
            "tracer_version": self.tracer_version,
            "tracers_used": json.dumps(self.tracers_used) if self.tracers_used else None,
            "traced_at": self.traced_at
        }


@dataclass
class ProcessTrace:
    """Process trace data model"""
    trace_id: int
    pid: int
    timestamp: int
    name: Optional[str] = None
    cmdline: Optional[str] = None
    state: Optional[str] = None
    parent_pid: Optional[int] = None
    memory_rss: int = 0
    memory_vm: int = 0
    cpu_utime: int = 0
    cpu_stime: int = 0
    threads: int = 0
    read_bytes: int = 0
    write_bytes: int = 0
    read_syscalls: int = 0
    write_syscalls: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class NetworkTrace:
    """Network trace data model"""
    trace_id: int
    timestamp: int
    local_ip: Optional[str] = None
    local_port: Optional[int] = None
    remote_ip: Optional[str] = None
    remote_port: Optional[int] = None
    state: Optional[str] = None
    protocol: str = "tcp"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class ErrorTrace:
    """Error trace data model"""
    trace_id: int
    timestamp: int
    error_type: Optional[str] = None
    error_category: Optional[str] = None
    exception_type: Optional[str] = None
    severity: int = 0
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    error_code: Optional[str] = None
    has_stacktrace: bool = False
    root_cause_hints: Optional[list] = None
    recovery_suggestions: Optional[list] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            "trace_id": self.trace_id,
            "timestamp": self.timestamp,
            "error_type": self.error_type,
            "error_category": self.error_category,
            "exception_type": self.exception_type,
            "severity": self.severity,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "error_code": self.error_code,
            "has_stacktrace": 1 if self.has_stacktrace else 0,
            "root_cause_hints": json.dumps(self.root_cause_hints) if self.root_cause_hints else None,
            "recovery_suggestions": json.dumps(self.recovery_suggestions) if self.recovery_suggestions else None
        }
