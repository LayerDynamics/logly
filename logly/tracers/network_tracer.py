"""
Network tracer - traces network connections and traffic
"""

from pathlib import Path
from typing import Dict, Any, List, Optional
import socket

from logly.utils.logger import get_logger


logger = get_logger(__name__)


class NetworkTracer:
    """Traces network connections and their relationships"""

    def __init__(self):
        """Initialize network tracer"""
        self._connection_cache = {}

    def trace_connection(self, local_addr: str, remote_addr: str) -> Dict[str, Any]:
        """
        Trace a network connection

        Args:
            local_addr: Local address (ip:port)
            remote_addr: Remote address (ip:port)

        Returns:
            Connection trace information
        """
        trace = {
            'local_address': local_addr,
            'remote_address': remote_addr,
            'local_ip': None,
            'local_port': None,
            'remote_ip': None,
            'remote_port': None,
            'state': None,
            'process_info': None,
        }

        # Parse addresses
        if ':' in local_addr:
            trace['local_ip'], port_str = local_addr.rsplit(':', 1)
            try:
                trace['local_port'] = int(port_str)
            except ValueError:
                pass

        if ':' in remote_addr:
            trace['remote_ip'], port_str = remote_addr.rsplit(':', 1)
            try:
                trace['remote_port'] = int(port_str)
            except ValueError:
                pass

        return trace

    def get_all_connections(self) -> List[Dict[str, Any]]:
        """
        Get all active network connections

        Returns:
            List of connection information
        """
        connections = []

        # Parse TCP connections (IPv4 and IPv6)
        for tcp_file in ['/proc/net/tcp', '/proc/net/tcp6']:
            tcp_path = Path(tcp_file)
            if not tcp_path.exists():
                continue

            try:
                with open(tcp_path, 'r') as f:
                    # Skip header
                    next(f)

                    for line in f:
                        conn = self._parse_connection_line(line, tcp_file)
                        if conn:
                            connections.append(conn)

            except Exception as e:
                logger.debug(f"Error reading {tcp_file}: {e}")

        return connections

    def _parse_connection_line(self, line: str, source_file: str) -> Optional[Dict[str, Any]]:
        """Parse a connection line from /proc/net/tcp"""
        try:
            fields = line.split()
            if len(fields) < 10:
                return None

            local_addr = fields[1]
            remote_addr = fields[2]
            state = fields[3]
            inode = fields[9]

            # Parse hex addresses
            local_ip, local_port = self._parse_hex_address(local_addr, source_file)
            remote_ip, remote_port = self._parse_hex_address(remote_addr, source_file)

            # Map state codes
            state_map = {
                '01': 'ESTABLISHED',
                '02': 'SYN_SENT',
                '03': 'SYN_RECV',
                '04': 'FIN_WAIT1',
                '05': 'FIN_WAIT2',
                '06': 'TIME_WAIT',
                '07': 'CLOSE',
                '08': 'CLOSE_WAIT',
                '09': 'LAST_ACK',
                '0A': 'LISTEN',
                '0B': 'CLOSING',
            }

            return {
                'local_ip': local_ip,
                'local_port': local_port,
                'remote_ip': remote_ip,
                'remote_port': remote_port,
                'state': state_map.get(state, state),
                'inode': inode,
            }

        except Exception as e:
            logger.debug(f"Error parsing connection line: {e}")
            return None

    def _parse_hex_address(self, hex_addr: str, source_file: str) -> tuple:
        """
        Parse hex address from /proc/net/tcp format

        Args:
            hex_addr: Hex encoded address (e.g., "0100007F:0050")
            source_file: Source file path to determine IPv4 vs IPv6

        Returns:
            Tuple of (ip_string, port_int)
        """
        try:
            ip_hex, port_hex = hex_addr.split(':')
            port = int(port_hex, 16)

            # IPv4
            if 'tcp6' not in source_file and len(ip_hex) == 8:
                ip_int = int(ip_hex, 16)
                ip = '.'.join([
                    str((ip_int >> 0) & 0xFF),
                    str((ip_int >> 8) & 0xFF),
                    str((ip_int >> 16) & 0xFF),
                    str((ip_int >> 24) & 0xFF),
                ])
                return ip, port

            # IPv6 (simplified - just return hex for now)
            else:
                return ip_hex, port

        except Exception:
            return hex_addr, 0

    def find_connections_by_ip(self, ip_address: str) -> List[Dict[str, Any]]:
        """
        Find all connections involving an IP address

        Args:
            ip_address: IP address to search for

        Returns:
            List of matching connections
        """
        all_connections = self.get_all_connections()
        matching = []

        for conn in all_connections:
            if conn['local_ip'] == ip_address or conn['remote_ip'] == ip_address:
                matching.append(conn)

        return matching

    def find_connections_by_port(self, port: int, local: bool = True) -> List[Dict[str, Any]]:
        """
        Find connections by port number

        Args:
            port: Port number to search for
            local: If True, search local ports; if False, search remote ports

        Returns:
            List of matching connections
        """
        all_connections = self.get_all_connections()
        matching = []

        for conn in all_connections:
            if local and conn['local_port'] == port:
                matching.append(conn)
            elif not local and conn['remote_port'] == port:
                matching.append(conn)

        return matching

    def get_listening_ports(self) -> List[Dict[str, Any]]:
        """
        Get all listening ports

        Returns:
            List of listening port information
        """
        all_connections = self.get_all_connections()
        listening = []

        for conn in all_connections:
            if conn['state'] == 'LISTEN':
                listening.append({
                    'port': conn['local_port'],
                    'ip': conn['local_ip'],
                    'protocol': 'tcp',
                })

        return listening

    def resolve_hostname(self, ip_address: str) -> Optional[str]:
        """
        Resolve IP address to hostname

        Args:
            ip_address: IP address to resolve

        Returns:
            Hostname or None
        """
        try:
            hostname, _, _ = socket.gethostbyaddr(ip_address)
            return hostname
        except (socket.herror, socket.gaierror):
            return None

    def get_connection_stats(self) -> Dict[str, int]:
        """
        Get statistics about network connections

        Returns:
            Connection statistics
        """
        all_connections = self.get_all_connections()

        stats = {
            'total': len(all_connections),
            'established': 0,
            'listen': 0,
            'time_wait': 0,
            'close_wait': 0,
            'syn_sent': 0,
            'syn_recv': 0,
            'other': 0,
        }

        for conn in all_connections:
            state = conn['state'].lower()
            if state == 'established':
                stats['established'] += 1
            elif state == 'listen':
                stats['listen'] += 1
            elif state == 'time_wait':
                stats['time_wait'] += 1
            elif state == 'close_wait':
                stats['close_wait'] += 1
            elif state == 'syn_sent':
                stats['syn_sent'] += 1
            elif state == 'syn_recv':
                stats['syn_recv'] += 1
            else:
                stats['other'] += 1

        return stats

    def trace_service_connections(self, service_name: str) -> List[Dict[str, Any]]:
        """
        Trace connections for a specific service

        Args:
            service_name: Service name to trace

        Returns:
            List of connections for the service
        """
        # Common service ports
        service_ports = {
            'ssh': [22],
            'http': [80, 8000, 8080],
            'https': [443, 8443],
            'postgresql': [5432],
            'mysql': [3306],
            'redis': [6379],
            'mongodb': [27017],
            'nginx': [80, 443, 8000, 8080],
            'django': [8000, 8080],
        }

        ports = service_ports.get(service_name.lower(), [])
        if not ports:
            return []

        connections = []
        for port in ports:
            connections.extend(self.find_connections_by_port(port, local=True))

        return connections
