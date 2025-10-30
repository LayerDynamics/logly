"""
IP tracer - traces IP addresses to their origin and reputation
"""

import re
from pathlib import Path
from typing import Dict, Any, Optional, List
from collections import defaultdict

from logly.utils.logger import get_logger


logger = get_logger(__name__)


class IPTracer:
    """Traces IP addresses to determine origin, type, and behavior patterns"""

    def __init__(self):
        """Initialize IP tracer"""
        self._ip_cache = {}
        self._known_malicious = set()
        self._whitelisted = set()

    def trace_ip(self, ip_address: str) -> Dict[str, Any]:
        """
        Trace an IP address

        Args:
            ip_address: IP address to trace

        Returns:
            IP trace information
        """
        # Check cache
        if ip_address in self._ip_cache:
            return self._ip_cache[ip_address]

        trace = {
            'ip': ip_address,
            'type': self._classify_ip(ip_address),
            'is_local': self._is_local_ip(ip_address),
            'is_private': self._is_private_ip(ip_address),
            'is_whitelisted': ip_address in self._whitelisted,
            'is_known_malicious': ip_address in self._known_malicious,
            'reverse_dns': None,
            'first_seen': None,
            'last_seen': None,
            'activity_count': 0,
            'failed_login_count': 0,
            'banned_count': 0,
            'threat_score': 0,
        }

        # Calculate threat score
        trace['threat_score'] = self._calculate_threat_score(trace)

        # Cache it
        self._ip_cache[ip_address] = trace

        return trace

    def _classify_ip(self, ip: str) -> str:
        """Classify IP address type"""
        if self._is_local_ip(ip):
            return 'localhost'
        elif self._is_private_ip(ip):
            return 'private'
        elif self._is_cloud_provider(ip):
            return 'cloud'
        else:
            return 'public'

    def _is_local_ip(self, ip: str) -> bool:
        """Check if IP is localhost"""
        return ip in ['127.0.0.1', '::1', 'localhost', '0.0.0.0']

    def _is_private_ip(self, ip: str) -> bool:
        """Check if IP is in private range"""
        if ip.startswith('192.168.'):
            return True
        if ip.startswith('10.'):
            return True
        if ip.startswith('172.'):
            octets = ip.split('.')
            if len(octets) >= 2:
                try:
                    second = int(octets[1])
                    return 16 <= second <= 31
                except ValueError:
                    pass
        if ip.startswith('fc00:') or ip.startswith('fd00:'):  # IPv6 private
            return True
        if ip.startswith('fe80:'):  # IPv6 link-local
            return True
        return False

    def _is_cloud_provider(self, ip: str) -> bool:
        """Check if IP belongs to known cloud provider (simplified)"""
        # This is a simplified check - in production, use IP range databases
        # AWS, GCP, Azure, etc. have published IP ranges
        # For now, this is just a placeholder
        return False

    def _calculate_threat_score(self, trace: Dict[str, Any]) -> int:
        """
        Calculate threat score (0-100) for an IP

        Args:
            trace: IP trace information

        Returns:
            Threat score
        """
        score = 0

        # Base scoring
        if trace['is_known_malicious']:
            score += 90
        elif trace['is_whitelisted']:
            score = 0
        elif trace['is_local'] or trace['is_private']:
            score = 0
        else:
            score = 10  # Unknown external IPs get base score

        # Add points for suspicious activity
        score += min(trace['failed_login_count'] * 5, 30)
        score += min(trace['banned_count'] * 20, 40)

        return min(score, 100)

    def update_ip_activity(self, ip_address: str, activity_type: str):
        """
        Update activity tracking for an IP

        Args:
            ip_address: IP address
            activity_type: Type of activity (e.g., 'failed_login', 'banned')
        """
        if ip_address not in self._ip_cache:
            self.trace_ip(ip_address)

        trace = self._ip_cache[ip_address]
        trace['activity_count'] += 1

        if activity_type == 'failed_login':
            trace['failed_login_count'] += 1
        elif activity_type == 'banned':
            trace['banned_count'] += 1

        # Recalculate threat score
        trace['threat_score'] = self._calculate_threat_score(trace)

        # Auto-mark as malicious if threshold exceeded
        if trace['threat_score'] >= 70:
            self._known_malicious.add(ip_address)
            trace['is_known_malicious'] = True

    def whitelist_ip(self, ip_address: str):
        """Add IP to whitelist"""
        self._whitelisted.add(ip_address)
        if ip_address in self._ip_cache:
            self._ip_cache[ip_address]['is_whitelisted'] = True
            self._ip_cache[ip_address]['threat_score'] = 0

    def blacklist_ip(self, ip_address: str):
        """Add IP to blacklist"""
        self._known_malicious.add(ip_address)
        if ip_address in self._ip_cache:
            self._ip_cache[ip_address]['is_known_malicious'] = True
            self._ip_cache[ip_address]['threat_score'] = 100

    def analyze_ip_patterns(self, ip_addresses: List[str]) -> Dict[str, Any]:
        """
        Analyze patterns across multiple IP addresses

        Args:
            ip_addresses: List of IP addresses to analyze

        Returns:
            Pattern analysis
        """
        patterns = {
            'total_ips': len(set(ip_addresses)),
            'by_type': defaultdict(int),
            'by_subnet': defaultdict(int),
            'by_country_code': defaultdict(int),  # Placeholder
            'suspicious_ips': [],
            'high_activity_ips': [],
        }

        ip_activity = defaultdict(int)

        for ip in ip_addresses:
            ip_activity[ip] += 1
            trace = self.trace_ip(ip)

            # Count by type
            patterns['by_type'][trace['type']] += 1

            # Count by subnet (Class C for IPv4)
            subnet = self._get_subnet(ip)
            if subnet:
                patterns['by_subnet'][subnet] += 1

            # Identify suspicious IPs
            if trace['threat_score'] >= 50 and ip not in patterns['suspicious_ips']:
                patterns['suspicious_ips'].append({
                    'ip': ip,
                    'threat_score': trace['threat_score'],
                    'type': trace['type'],
                })

        # Find high-activity IPs
        for ip, count in sorted(ip_activity.items(), key=lambda x: x[1], reverse=True)[:10]:
            patterns['high_activity_ips'].append({
                'ip': ip,
                'count': count,
            })

        # Convert defaultdicts to regular dicts
        patterns['by_type'] = dict(patterns['by_type'])
        patterns['by_subnet'] = dict(patterns['by_subnet'])
        patterns['by_country_code'] = dict(patterns['by_country_code'])

        return patterns

    def _get_subnet(self, ip: str) -> Optional[str]:
        """Get Class C subnet for IPv4"""
        if ':' in ip:  # IPv6
            return None

        parts = ip.split('.')
        if len(parts) >= 3:
            return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24"

        return None

    def detect_ip_sweep(self, ip_addresses: List[str], threshold: int = 10) -> List[str]:
        """
        Detect potential IP sweeps (many IPs from same subnet)

        Args:
            ip_addresses: List of IP addresses
            threshold: Minimum count to consider a sweep

        Returns:
            List of suspicious subnets
        """
        subnet_counts = defaultdict(int)

        for ip in ip_addresses:
            subnet = self._get_subnet(ip)
            if subnet:
                subnet_counts[subnet] += 1

        suspicious_subnets = [
            subnet for subnet, count in subnet_counts.items()
            if count >= threshold
        ]

        return suspicious_subnets

    def get_ip_geolocation(self, ip_address: str) -> Optional[Dict[str, Any]]:
        """
        Get geolocation for IP (placeholder - requires external API)

        Args:
            ip_address: IP address to geolocate

        Returns:
            Geolocation information or None
        """
        # In production, this would call a geolocation API
        # For now, return placeholder
        logger.debug(f"Geolocation lookup for {ip_address} not implemented")
        return None

    def export_ip_reputation(self) -> Dict[str, Any]:
        """
        Export IP reputation data

        Returns:
            IP reputation database
        """
        return {
            'cached_ips': len(self._ip_cache),
            'known_malicious': list(self._known_malicious),
            'whitelisted': list(self._whitelisted),
            'high_threat_ips': [
                {
                    'ip': ip,
                    'threat_score': trace['threat_score'],
                    'activity_count': trace['activity_count'],
                }
                for ip, trace in self._ip_cache.items()
                if trace['threat_score'] >= 70
            ],
        }

    def clear_cache(self):
        """Clear IP cache"""
        self._ip_cache.clear()
