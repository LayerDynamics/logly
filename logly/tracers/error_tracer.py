"""
Error tracer - traces errors to their sources and contexts
"""

import re
from typing import Dict, Any, List, Optional
from collections import Counter

from logly.utils.logger import get_logger


logger = get_logger(__name__)


class ErrorTracer:
    """Traces errors and exceptions to identify patterns and root causes"""

    def __init__(self):
        """Initialize error tracer"""
        self._error_patterns = self._load_error_patterns()
        self._error_history = []

    def _load_error_patterns(self) -> Dict[str, re.Pattern]:
        """Load regex patterns for error identification"""
        return {
            # Python exceptions
            'python_exception': re.compile(r'(\w+(?:Error|Exception)):\s*(.+)'),
            'python_traceback': re.compile(r'Traceback \(most recent call last\)'),

            # Database errors
            'db_connection': re.compile(r'(?:connection|connect).+(?:refused|failed|timeout)', re.IGNORECASE),
            'db_query': re.compile(r'(?:SQL|query).+(?:error|failed|syntax)', re.IGNORECASE),
            'db_deadlock': re.compile(r'deadlock', re.IGNORECASE),

            # Memory errors
            'out_of_memory': re.compile(r'(?:out of memory|OOM|MemoryError)', re.IGNORECASE),
            'memory_leak': re.compile(r'memory.+(?:leak|exhausted)', re.IGNORECASE),

            # Disk errors
            'disk_full': re.compile(r'(?:no space|disk full|ENOSPC)', re.IGNORECASE),
            'disk_io': re.compile(r'(?:I/O error|disk.+error)', re.IGNORECASE),

            # Network errors
            'connection_timeout': re.compile(r'connection.+timeout', re.IGNORECASE),
            'connection_refused': re.compile(r'connection.+refused', re.IGNORECASE),
            'network_unreachable': re.compile(r'network.+unreachable', re.IGNORECASE),

            # Permission errors
            'permission_denied': re.compile(r'(?:permission denied|EACCES)', re.IGNORECASE),
            'file_not_found': re.compile(r'(?:file not found|ENOENT|No such file)', re.IGNORECASE),

            # Resource errors
            'too_many_files': re.compile(r'(?:too many.+files|EMFILE)', re.IGNORECASE),
            'resource_unavailable': re.compile(r'resource.+(?:unavailable|busy)', re.IGNORECASE),

            # Application errors
            'segmentation_fault': re.compile(r'segmentation fault|SIGSEGV', re.IGNORECASE),
            'assertion_failed': re.compile(r'assertion.+failed', re.IGNORECASE),
        }

    def trace_error(self, error_message: str, source: str = None, level: str = 'ERROR') -> Dict[str, Any]:
        """
        Trace an error message to extract context

        Args:
            error_message: Error message text
            source: Source of the error (service, file, etc.)
            level: Error level

        Returns:
            Error trace information
        """
        trace = {
            'message': error_message,
            'source': source,
            'level': level,
            'error_type': None,
            'error_category': None,
            'exception_type': None,
            'file_path': None,
            'line_number': None,
            'error_code': None,
            'has_stacktrace': False,
            'severity': self._calculate_severity(level, error_message),
            'root_cause_hints': [],
            'recovery_suggestions': [],
        }

        # Identify error type and category
        for error_type, pattern in self._error_patterns.items():
            if pattern.search(error_message):
                if not trace['error_type']:
                    trace['error_type'] = error_type
                trace['error_category'] = self._categorize_error(error_type)
                break

        # Extract Python exception
        exception_match = self._error_patterns['python_exception'].search(error_message)
        if exception_match:
            trace['exception_type'] = exception_match.group(1)
            trace['error_details'] = exception_match.group(2)

        # Check for traceback
        if self._error_patterns['python_traceback'].search(error_message):
            trace['has_stacktrace'] = True

        # Extract file and line number
        file_match = re.search(r'(?:File\s+|at\s+)(["\']?)([/\w.]+\.py)\1.*?(?:line\s+)?(\d+)', error_message, re.IGNORECASE)
        if file_match:
            trace['file_path'] = file_match.group(2)
            if file_match.group(3):
                trace['line_number'] = int(file_match.group(3))

        # Extract error codes
        code_match = re.search(r'(?:error|errno|code)[:\s#]+(\d+)', error_message, re.IGNORECASE)
        if code_match:
            trace['error_code'] = code_match.group(1)

        # Add root cause hints
        trace['root_cause_hints'] = self._identify_root_causes(trace)

        # Add recovery suggestions
        trace['recovery_suggestions'] = self._suggest_recovery(trace)

        # Store in history
        self._error_history.append(trace)

        return trace

    def _categorize_error(self, error_type: str) -> str:
        """Categorize error into high-level categories"""
        categories = {
            'python_exception': 'application',
            'python_traceback': 'application',
            'db_connection': 'database',
            'db_query': 'database',
            'db_deadlock': 'database',
            'out_of_memory': 'resource',
            'memory_leak': 'resource',
            'disk_full': 'resource',
            'disk_io': 'resource',
            'connection_timeout': 'network',
            'connection_refused': 'network',
            'network_unreachable': 'network',
            'permission_denied': 'security',
            'file_not_found': 'filesystem',
            'too_many_files': 'resource',
            'resource_unavailable': 'resource',
            'segmentation_fault': 'system',
            'assertion_failed': 'application',
        }
        return categories.get(error_type, 'unknown')

    def _calculate_severity(self, level: str, message: str) -> int:
        """
        Calculate error severity (0-100)

        Args:
            level: Error level
            message: Error message

        Returns:
            Severity score
        """
        # Base score on level
        level_scores = {
            'DEBUG': 0,
            'INFO': 10,
            'WARNING': 30,
            'ERROR': 60,
            'CRITICAL': 90,
            'FATAL': 100,
        }
        score = level_scores.get(level.upper(), 50)

        # Increase for critical keywords
        critical_keywords = [
            'fatal', 'critical', 'crash', 'panic', 'segfault',
            'out of memory', 'disk full', 'deadlock'
        ]
        for keyword in critical_keywords:
            if keyword in message.lower():
                score += 15
                break

        # Increase for database errors
        if any(word in message.lower() for word in ['database', 'sql', 'query']):
            score += 10

        return min(score, 100)

    def _identify_root_causes(self, trace: Dict[str, Any]) -> List[str]:
        """Identify potential root causes for an error"""
        hints = []

        error_type = trace.get('error_type')
        category = trace.get('error_category')
        message = trace.get('message', '').lower()

        # Database errors
        if category == 'database':
            if error_type == 'db_connection':
                hints.append('Database service may be down or unreachable')
                hints.append('Check network connectivity to database server')
                hints.append('Verify database credentials and connection string')
            elif error_type == 'db_deadlock':
                hints.append('Multiple transactions competing for same resources')
                hints.append('Review transaction isolation levels')
                hints.append('Optimize query order to avoid deadlocks')

        # Resource errors
        elif category == 'resource':
            if error_type == 'out_of_memory':
                hints.append('Application consuming too much memory')
                hints.append('Check for memory leaks in application code')
                hints.append('Consider increasing system memory or swap')
            elif error_type == 'disk_full':
                hints.append('Filesystem has run out of space')
                hints.append('Check for large log files or temporary files')
                hints.append('Review log rotation policies')
            elif error_type == 'too_many_files':
                hints.append('Process has exceeded open file limit')
                hints.append('Check ulimit settings')
                hints.append('Look for file descriptor leaks')

        # Network errors
        elif category == 'network':
            if error_type == 'connection_timeout':
                hints.append('Remote service not responding in time')
                hints.append('Network latency or bandwidth issues')
                hints.append('Service may be overloaded')
            elif error_type == 'connection_refused':
                hints.append('Service not running or not listening on expected port')
                hints.append('Firewall may be blocking connection')
                hints.append('Check service configuration')

        # Permission errors
        elif category == 'security':
            hints.append('Insufficient permissions to access resource')
            hints.append('Check file/directory ownership and permissions')
            hints.append('Verify process is running with correct user/group')

        return hints

    def _suggest_recovery(self, trace: Dict[str, Any]) -> List[str]:
        """Suggest recovery actions for an error"""
        suggestions = []

        error_type = trace.get('error_type')
        category = trace.get('error_category')

        # Database errors
        if category == 'database':
            suggestions.append('Implement database connection retry logic with exponential backoff')
            suggestions.append('Add database connection pooling')
            suggestions.append('Set up database health checks')

        # Resource errors
        elif category == 'resource':
            if error_type in ['out_of_memory', 'memory_leak']:
                suggestions.append('Implement memory monitoring and alerting')
                suggestions.append('Add automatic process restart on high memory usage')
                suggestions.append('Profile application to find memory leaks')
            elif error_type == 'disk_full':
                suggestions.append('Implement automatic log rotation')
                suggestions.append('Add disk space monitoring and alerts')
                suggestions.append('Clean up old temporary files regularly')

        # Network errors
        elif category == 'network':
            suggestions.append('Implement retry logic with circuit breaker pattern')
            suggestions.append('Add connection timeouts to prevent hanging')
            suggestions.append('Set up health check endpoints')

        # General suggestions
        suggestions.append('Add detailed logging around the error location')
        suggestions.append('Set up alerting for this error type')

        return suggestions

    def analyze_error_patterns(self, time_window: int = 3600) -> Dict[str, Any]:
        """
        Analyze recent error patterns

        Args:
            time_window: Time window in seconds to analyze

        Returns:
            Error pattern analysis
        """
        # For now, analyze all errors in history
        # In production, filter by time_window

        analysis = {
            'total_errors': len(self._error_history),
            'by_type': Counter(),
            'by_category': Counter(),
            'by_source': Counter(),
            'by_severity': {
                'low': 0,      # 0-30
                'medium': 0,   # 31-60
                'high': 0,     # 61-80
                'critical': 0, # 81-100
            },
            'top_errors': [],
            'recurring_errors': [],
        }

        error_signatures = Counter()

        for trace in self._error_history:
            # Count by type and category
            if trace.get('error_type'):
                analysis['by_type'][trace['error_type']] += 1

            if trace.get('error_category'):
                analysis['by_category'][trace['error_category']] += 1

            if trace.get('source'):
                analysis['by_source'][trace['source']] += 1

            # Count by severity
            severity = trace.get('severity', 0)
            if severity <= 30:
                analysis['by_severity']['low'] += 1
            elif severity <= 60:
                analysis['by_severity']['medium'] += 1
            elif severity <= 80:
                analysis['by_severity']['high'] += 1
            else:
                analysis['by_severity']['critical'] += 1

            # Create error signature for recurring detection
            signature = f"{trace.get('error_type')}:{trace.get('file_path')}:{trace.get('line_number')}"
            error_signatures[signature] += 1

        # Find top errors
        analysis['top_errors'] = [
            {'type': error_type, 'count': count}
            for error_type, count in analysis['by_type'].most_common(10)
        ]

        # Find recurring errors (same signature multiple times)
        analysis['recurring_errors'] = [
            {'signature': sig, 'count': count}
            for sig, count in error_signatures.items()
            if count > 1
        ]

        return analysis

    def get_error_timeline(self) -> List[Dict[str, Any]]:
        """Get chronological error timeline"""
        return self._error_history.copy()

    def clear_history(self):
        """Clear error history"""
        self._error_history.clear()
