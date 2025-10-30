"""
Tracer collector - integrates all tracers to collect comprehensive traces
"""

from typing import List, Dict, Any

from logly.collectors.base_collector import BaseCollector
from logly.storage.models import LogEvent
from logly.tracers.event_tracer import EventTracer
from logly.tracers.process_tracer import ProcessTracer
from logly.tracers.network_tracer import NetworkTracer
from logly.tracers.ip_tracer import IPTracer
from logly.tracers.error_tracer import ErrorTracer
from logly.utils.logger import get_logger


logger = get_logger(__name__)


class TracerCollector(BaseCollector):
    """
    Collects comprehensive traces by integrating all tracer modules

    This collector enhances log events with:
    - Process information
    - Network connection details
    - IP reputation and geolocation
    - Error analysis and root cause hints
    - Causality chains
    """

    def __init__(self, config: dict):
        super().__init__(config)

        # Initialize all tracers
        self.event_tracer = EventTracer()
        self.process_tracer = ProcessTracer()
        self.network_tracer = NetworkTracer()
        self.ip_tracer = IPTracer()
        self.error_tracer = ErrorTracer()

        # Configuration
        self.trace_processes = config.get('trace_processes', True)
        self.trace_network = config.get('trace_network', True)
        self.trace_ips = config.get('trace_ips', True)
        self.trace_errors = config.get('trace_errors', True)

    def collect(self) -> List[Dict[str, Any]]:
        """
        This collector doesn't collect data directly.
        Instead, it's meant to be called to enrich log events.

        Returns:
            Empty list (use trace_event instead)
        """
        logger.debug("TracerCollector.collect() called - use trace_event() instead")
        return []

    def trace_event(self, log_event: LogEvent) -> Dict[str, Any]:
        """
        Create a comprehensive trace for a log event

        Args:
            log_event: LogEvent to trace

        Returns:
            Complete trace information
        """
        # Start with event trace
        trace = self.event_tracer.trace_event(log_event)

        # Add process information if service is identified
        if self.trace_processes and log_event.service:
            process_traces = self.process_tracer.trace_by_name(log_event.service)
            if process_traces:
                trace['processes'] = process_traces

                # Add aggregated resource summary
                pids = [p['pid'] for p in process_traces]
                trace['resource_summary'] = self.process_tracer.get_resource_summary(pids)

        # Add network information if IP is present
        if self.trace_network and log_event.ip_address:
            # Network connection trace
            connections = self.network_tracer.find_connections_by_ip(log_event.ip_address)
            if connections:
                trace['network_connections'] = connections[:10]  # Limit to 10

            # Connection stats
            trace['network_stats'] = self.network_tracer.get_connection_stats()

        # Add IP trace if IP is present
        if self.trace_ips and log_event.ip_address:
            ip_trace = self.ip_tracer.trace_ip(log_event.ip_address)
            trace['ip_info'] = ip_trace

            # Update IP activity tracking
            if log_event.action:
                self.ip_tracer.update_ip_activity(log_event.ip_address, log_event.action)

        # Add error trace if it's an error/warning event
        if self.trace_errors and log_event.level in ['ERROR', 'CRITICAL', 'WARNING']:
            error_trace = self.error_tracer.trace_error(
                log_event.message,
                source=log_event.source,
                level=log_event.level
            )
            trace['error_info'] = error_trace

        # Add trace metadata
        trace['trace_metadata'] = {
            'traced_at': log_event.timestamp,
            'tracer_version': '1.0',
            'tracers_used': self._get_active_tracers(),
        }

        return trace

    def trace_batch(self, log_events: List[LogEvent]) -> List[Dict[str, Any]]:
        """
        Trace multiple log events in batch

        Args:
            log_events: List of LogEvents to trace

        Returns:
            List of trace information
        """
        traces = []

        for event in log_events:
            try:
                trace = self.trace_event(event)
                traces.append(trace)
            except Exception as e:
                logger.error(f"Error tracing event: {e}")
                # Add minimal trace on error
                traces.append({
                    'event_id': None,
                    'timestamp': event.timestamp,
                    'source': event.source,
                    'error': str(e),
                })

        return traces

    def analyze_traces(self, log_events: List[LogEvent]) -> Dict[str, Any]:
        """
        Analyze multiple events to find patterns and insights

        Args:
            log_events: List of events to analyze

        Returns:
            Comprehensive analysis
        """
        analysis = {
            'total_events': len(log_events),
            'event_patterns': {},
            'error_patterns': {},
            'ip_patterns': {},
            'network_summary': {},
            'process_summary': {},
        }

        # Extract IPs and errors for analysis
        ip_addresses = [e.ip_address for e in log_events if e.ip_address]
        error_events = [e for e in log_events if e.level in ['ERROR', 'CRITICAL', 'WARNING']]

        # Event pattern analysis
        analysis['event_patterns'] = self.event_tracer.extract_event_patterns(log_events)

        # IP pattern analysis
        if ip_addresses:
            analysis['ip_patterns'] = self.ip_tracer.analyze_ip_patterns(ip_addresses)

            # Detect IP sweeps
            sweeps = self.ip_tracer.detect_ip_sweep(ip_addresses)
            if sweeps:
                analysis['ip_patterns']['suspicious_sweeps'] = sweeps

        # Error pattern analysis
        if error_events:
            # Trace all errors first
            for event in error_events:
                if event.level:  # Only trace if level is not None
                    self.error_tracer.trace_error(
                        event.message,
                        source=event.source,
                        level=event.level
                    )

            # Get pattern analysis
            analysis['error_patterns'] = self.error_tracer.analyze_error_patterns()

        # Network summary
        if self.trace_network:
            analysis['network_summary'] = self.network_tracer.get_connection_stats()
            analysis['network_summary']['listening_ports'] = len(
                self.network_tracer.get_listening_ports()
            )

        # Process summary for known services
        if self.trace_processes:
            all_processes = self.process_tracer.get_all_processes()
            analysis['process_summary'] = {
                'total_processes': len(all_processes),
            }

        return analysis

    def get_trace_summary(self) -> Dict[str, Any]:
        """
        Get summary of tracer state and statistics

        Returns:
            Tracer summary information
        """
        return {
            'tracers_enabled': self._get_active_tracers(),
            'ip_reputation': self.ip_tracer.export_ip_reputation(),
            'error_timeline_count': len(self.error_tracer.get_error_timeline()),
            'network_connections': len(self.network_tracer.get_all_connections()),
            'total_processes': len(self.process_tracer.get_all_processes()),
        }

    def _get_active_tracers(self) -> List[str]:
        """Get list of active tracers"""
        active = ['event']  # Event tracer is always active

        if self.trace_processes:
            active.append('process')
        if self.trace_network:
            active.append('network')
        if self.trace_ips:
            active.append('ip')
        if self.trace_errors:
            active.append('error')

        return active

    def trace_specific_ip(self, ip_address: str) -> Dict[str, Any]:
        """
        Perform deep trace on a specific IP address

        Args:
            ip_address: IP address to trace

        Returns:
            Complete IP trace
        """
        trace = {
            'ip': ip_address,
            'ip_info': self.ip_tracer.trace_ip(ip_address),
            'connections': self.network_tracer.find_connections_by_ip(ip_address),
            'hostname': self.network_tracer.resolve_hostname(ip_address),
        }

        return trace

    def trace_specific_service(self, service_name: str) -> Dict[str, Any]:
        """
        Perform deep trace on a specific service

        Args:
            service_name: Service name to trace

        Returns:
            Complete service trace
        """
        trace = {
            'service': service_name,
            'processes': self.process_tracer.trace_by_name(service_name),
            'connections': self.network_tracer.trace_service_connections(service_name),
        }

        # Add resource summary
        if trace['processes']:
            pids = [p['pid'] for p in trace['processes']]
            trace['resource_summary'] = self.process_tracer.get_resource_summary(pids)

        return trace

    def clear_caches(self):
        """Clear all tracer caches"""
        self.ip_tracer.clear_cache()
        self.error_tracer.clear_history()
        logger.info("Cleared all tracer caches")

    def validate(self) -> bool:
        """Validate tracer collector can run"""
        # Tracers use similar paths as other collectors
        return True
