"""
Event tracer - traces events to their sources and contexts
"""

import re
from typing import Dict, Any, List, Optional

from logly.storage.models import LogEvent
from logly.utils.logger import get_logger


logger = get_logger(__name__)


class EventTracer:
    """Traces events to their sources, processes, and contexts"""

    def __init__(self):
        """Initialize event tracer"""
        self._service_patterns = self._load_service_patterns()

    def _load_service_patterns(self) -> Dict[str, re.Pattern]:
        """Load regex patterns for identifying services"""
        return {
            'nginx': re.compile(r'nginx(?:\[\d+\])?', re.IGNORECASE),
            'apache': re.compile(r'apache2?(?:\[\d+\])?', re.IGNORECASE),
            'django': re.compile(r'(?:django|gunicorn|uwsgi)(?:\[\d+\])?', re.IGNORECASE),
            'postgresql': re.compile(r'postgres(?:ql)?(?:\[\d+\])?', re.IGNORECASE),
            'mysql': re.compile(r'mysql(?:d)?(?:\[\d+\])?', re.IGNORECASE),
            'redis': re.compile(r'redis(?:-server)?(?:\[\d+\])?', re.IGNORECASE),
            'ssh': re.compile(r'sshd?(?:\[\d+\])?', re.IGNORECASE),
            'fail2ban': re.compile(r'fail2ban(?:-server)?(?:\[\d+\])?', re.IGNORECASE),
            'systemd': re.compile(r'systemd(?:\[\d+\])?', re.IGNORECASE),
            'docker': re.compile(r'docker(?:d)?(?:\[\d+\])?', re.IGNORECASE),
        }

    def trace_event(self, log_event: LogEvent) -> Dict[str, Any]:
        """
        Trace a log event to its complete context

        Args:
            log_event: LogEvent to trace

        Returns:
            Dict with trace information
        """
        trace = {
            'event_id': None,
            'timestamp': log_event.timestamp,
            'source': log_event.source,
            'message': log_event.message,
            'level': log_event.level,
            'ip_address': log_event.ip_address,
            'user': log_event.user,
            'service': log_event.service,
            'action': log_event.action,
            'metadata': log_event.metadata or {},
            'related_services': [],
            'causality': None,
            'severity_score': self._calculate_severity(log_event),
        }

        # Identify related services
        related_services = self._identify_related_services(log_event)
        if related_services:
            trace['related_services'] = related_services

        # Add causality chain if available
        causality = self._trace_causality(log_event)
        if causality:
            trace['causality'] = causality

        return trace

    def _calculate_severity(self, log_event: LogEvent) -> int:
        """
        Calculate severity score (0-100)

        Args:
            log_event: Event to score

        Returns:
            Severity score
        """
        score = 0

        # Base score on log level
        level_scores = {
            'DEBUG': 0,
            'INFO': 10,
            'WARNING': 30,
            'ERROR': 60,
            'CRITICAL': 90,
        }
        score = level_scores.get(log_event.level or 'INFO', 10)

        # Increase for security events
        if log_event.action in ['ban', 'failed_login', 'unauthorized']:
            score += 20

        # Increase for repeated events (if metadata indicates)
        if log_event.metadata and log_event.metadata.get('count', 1) > 5:
            score += 10

        # Cap at 100
        return min(score, 100)

    def _identify_related_services(self, log_event: LogEvent) -> List[str]:
        """Identify services related to this event"""
        related = []

        # Based on source
        service_relationships = {
            'fail2ban': ['ssh', 'nginx', 'apache', 'auth'],
            'nginx': ['django', 'gunicorn', 'uwsgi', 'php-fpm'],
            'apache': ['django', 'php', 'wsgi'],
            'django': ['postgresql', 'mysql', 'redis', 'nginx', 'celery'],
            'auth': ['ssh', 'fail2ban', 'pam'],
            'postgresql': ['django', 'pgbouncer'],
            'mysql': ['django', 'wordpress'],
            'docker': ['nginx', 'redis', 'postgresql'],
        }

        if log_event.source in service_relationships:
            related.extend(service_relationships[log_event.source])

        # Based on message content
        for service_name, pattern in self._service_patterns.items():
            if pattern.search(log_event.message):
                if service_name not in related:
                    related.append(service_name)

        return list(set(related))  # Remove duplicates

    def _trace_causality(self, log_event: LogEvent) -> Optional[Dict[str, Any]]:
        """
        Trace the causality chain of an event

        Args:
            log_event: Event to trace

        Returns:
            Causality chain information
        """
        causality = {}

        # For failed logins -> ban chain
        if log_event.action == 'ban' and log_event.source == 'fail2ban':
            causality['trigger'] = 'repeated_failed_logins'
            causality['chain'] = [
                {'step': 'initial_failed_authentication', 'service': 'ssh'},
                {'step': 'repeated_failures_detected', 'service': 'fail2ban'},
                {'step': 'ip_banned', 'service': 'fail2ban'},
            ]
            causality['root_cause'] = 'brute_force_attempt'

        # For authentication failures
        elif log_event.action == 'failed_login':
            causality['trigger'] = 'authentication_failure'
            causality['chain'] = [
                {'step': 'connection_established', 'service': log_event.service or 'ssh'},
                {'step': 'authentication_attempted', 'service': log_event.service or 'ssh'},
                {'step': 'authentication_failed', 'service': log_event.service or 'ssh'},
            ]
            causality['root_cause'] = 'invalid_credentials'

        # For connection errors
        elif log_event.level == 'ERROR' and 'connection' in log_event.message.lower():
            if 'timeout' in log_event.message.lower():
                causality['trigger'] = 'connection_timeout'
                causality['chain'] = [
                    {'step': 'connection_attempt', 'service': log_event.service},
                    {'step': 'waiting_for_response', 'service': log_event.service},
                    {'step': 'timeout_reached', 'service': log_event.service},
                ]
                causality['root_cause'] = 'network_latency_or_service_unresponsive'

            elif 'refused' in log_event.message.lower():
                causality['trigger'] = 'connection_refused'
                causality['chain'] = [
                    {'step': 'connection_attempt', 'service': log_event.service},
                    {'step': 'connection_refused', 'service': log_event.service},
                ]
                causality['root_cause'] = 'service_not_listening_or_firewall'

        # For memory errors
        elif log_event.level in ['ERROR', 'CRITICAL']:
            if any(keyword in log_event.message.lower() for keyword in ['memory', 'oom', 'out of memory']):
                causality['trigger'] = 'memory_exhaustion'
                causality['chain'] = [
                    {'step': 'memory_allocation_request', 'service': log_event.service},
                    {'step': 'insufficient_memory', 'service': 'system'},
                    {'step': 'oom_condition', 'service': 'system'},
                ]
                causality['root_cause'] = 'memory_leak_or_insufficient_resources'

            elif any(keyword in log_event.message.lower() for keyword in ['disk', 'space', 'no space']):
                causality['trigger'] = 'disk_space_exhausted'
                causality['chain'] = [
                    {'step': 'write_operation_attempted', 'service': log_event.service},
                    {'step': 'insufficient_disk_space', 'service': 'system'},
                    {'step': 'operation_failed', 'service': log_event.service},
                ]
                causality['root_cause'] = 'disk_space_exhaustion'

        return causality if causality else None

    def extract_event_patterns(self, events: List[LogEvent]) -> Dict[str, Any]:
        """
        Analyze multiple events to find patterns

        Args:
            events: List of events to analyze

        Returns:
            Pattern analysis
        """
        patterns = {
            'total_events': len(events),
            'by_source': {},
            'by_level': {},
            'by_action': {},
            'ip_frequency': {},
            'user_frequency': {},
            'service_frequency': {},
            'time_distribution': {},
        }

        for event in events:
            # Count by source
            patterns['by_source'][event.source] = patterns['by_source'].get(event.source, 0) + 1

            # Count by level
            if event.level:
                patterns['by_level'][event.level] = patterns['by_level'].get(event.level, 0) + 1

            # Count by action
            if event.action:
                patterns['by_action'][event.action] = patterns['by_action'].get(event.action, 0) + 1

            # Count by IP
            if event.ip_address:
                patterns['ip_frequency'][event.ip_address] = patterns['ip_frequency'].get(event.ip_address, 0) + 1

            # Count by user
            if event.user:
                patterns['user_frequency'][event.user] = patterns['user_frequency'].get(event.user, 0) + 1

            # Count by service
            if event.service:
                patterns['service_frequency'][event.service] = patterns['service_frequency'].get(event.service, 0) + 1

        return patterns
