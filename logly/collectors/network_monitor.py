"""
Network activity monitor - traffic stats and connection counts
Uses /proc/net on Linux, netstat on macOS for minimal dependencies
"""

import platform
import subprocess
from pathlib import Path

from logly.collectors.base_collector import BaseCollector
from logly.storage.models import NetworkMetric
from logly.utils.logger import get_logger


logger = get_logger(__name__)

# Detect platform once at module load
IS_LINUX = platform.system() == "Linux"
IS_MACOS = platform.system() == "Darwin"


class NetworkMonitor(BaseCollector):
    """Collects network metrics using /proc/net"""

    def __init__(self, config: dict):
        super().__init__(config)
        self.metrics_to_collect = config.get("metrics", [])
        self._last_net_io = None

    def collect(self) -> NetworkMetric:
        """
        Collect network metrics

        Returns:
            NetworkMetric object with current network stats
        """
        metric = NetworkMetric.now()

        # Collect network I/O stats
        if any(
            m in self.metrics_to_collect
            for m in ["bytes_sent", "bytes_recv", "packets_sent", "packets_recv"]
        ):
            net_io = self._get_network_io_stats()
            metric.bytes_sent = net_io.get("bytes_sent")
            metric.bytes_recv = net_io.get("bytes_recv")
            metric.packets_sent = net_io.get("packets_sent")
            metric.packets_recv = net_io.get("packets_recv")
            metric.errors_in = net_io.get("errors_in")
            metric.errors_out = net_io.get("errors_out")
            metric.drops_in = net_io.get("drops_in")
            metric.drops_out = net_io.get("drops_out")

        # Collect connection stats
        if (
            "connections" in self.metrics_to_collect
            or "listening_ports" in self.metrics_to_collect
        ):
            conn_stats = self._get_connection_stats()
            metric.connections_established = conn_stats.get("established")
            metric.connections_listen = conn_stats.get("listen")
            metric.connections_time_wait = conn_stats.get("time_wait")

        return metric

    def _get_network_io_stats(self) -> dict:
        """
        Get network I/O statistics

        Returns:
            Dict with bytes_sent, bytes_recv, packets, errors, drops
        """
        if IS_LINUX:
            return self._get_network_io_stats_linux()
        elif IS_MACOS:
            return self._get_network_io_stats_macos()
        else:
            logger.warning(f"Unsupported platform: {platform.system()}")
            return {
                "bytes_recv": 0,
                "bytes_sent": 0,
                "packets_recv": 0,
                "packets_sent": 0,
                "errors_in": 0,
                "errors_out": 0,
                "drops_in": 0,
                "drops_out": 0,
            }

    def _get_network_io_stats_linux(self) -> dict:
        """Get network I/O stats on Linux using /proc/net/dev"""
        try:
            # Parse /proc/net/dev
            # Skip loopback interface (lo)
            total_bytes_recv = 0
            total_bytes_sent = 0
            total_packets_recv = 0
            total_packets_sent = 0
            total_errors_in = 0
            total_errors_out = 0
            total_drops_in = 0
            total_drops_out = 0

            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()

            # Skip header lines (first 2 lines)
            for line in lines[2:]:
                if ":" not in line:
                    continue

                # Parse interface line
                iface, data = line.split(":", 1)
                iface = iface.strip()

                # Skip loopback
                if iface == "lo":
                    continue

                fields = data.split()
                if len(fields) >= 16:
                    # Receive: bytes, packets, errs, drop, ...
                    bytes_recv = int(fields[0])
                    packets_recv = int(fields[1])
                    errs_in = int(fields[2])
                    drop_in = int(fields[3])

                    # Transmit: bytes, packets, errs, drop, ...
                    bytes_sent = int(fields[8])
                    packets_sent = int(fields[9])
                    errs_out = int(fields[10])
                    drop_out = int(fields[11])

                    total_bytes_recv += bytes_recv
                    total_bytes_sent += bytes_sent
                    total_packets_recv += packets_recv
                    total_packets_sent += packets_sent
                    total_errors_in += errs_in
                    total_errors_out += errs_out
                    total_drops_in += drop_in
                    total_drops_out += drop_out

            return {
                "bytes_recv": total_bytes_recv,
                "bytes_sent": total_bytes_sent,
                "packets_recv": total_packets_recv,
                "packets_sent": total_packets_sent,
                "errors_in": total_errors_in,
                "errors_out": total_errors_out,
                "drops_in": total_drops_in,
                "drops_out": total_drops_out,
            }

        except Exception as e:
            logger.error(f"Error reading network I/O stats: {e}")
            return {
                "bytes_recv": 0,
                "bytes_sent": 0,
                "packets_recv": 0,
                "packets_sent": 0,
                "errors_in": 0,
                "errors_out": 0,
                "drops_in": 0,
                "drops_out": 0,
            }

    def _get_network_io_stats_macos(self) -> dict:
        """Get network I/O stats on macOS using netstat"""
        try:
            # Use netstat -ib to get interface statistics
            result = subprocess.run(
                ["netstat", "-ib"],
                capture_output=True,
                text=True,
                timeout=2
            )

            if result.returncode != 0:
                return {
                    "bytes_recv": 0,
                    "bytes_sent": 0,
                    "packets_recv": 0,
                    "packets_sent": 0,
                    "errors_in": 0,
                    "errors_out": 0,
                    "drops_in": 0,
                    "drops_out": 0,
                }

            total_bytes_recv = 0
            total_bytes_sent = 0
            total_packets_recv = 0
            total_packets_sent = 0
            total_errors_in = 0
            total_errors_out = 0
            total_drops_in = 0
            total_drops_out = 0

            lines = result.stdout.strip().split('\n')
            # Skip header line
            for line in lines[1:]:
                fields = line.split()
                if len(fields) >= 10:
                    iface = fields[0]
                    # Skip loopback (lo0)
                    if iface.startswith('lo'):
                        continue

                    try:
                        # netstat -ib format: Name Mtu Network Address Ipkts Ierrs Ibytes Opkts Oerrs Obytes Coll Drop
                        ipkts = int(fields[4])
                        ierrs = int(fields[5])
                        ibytes = int(fields[6])
                        opkts = int(fields[7])
                        oerrs = int(fields[8])
                        obytes = int(fields[9])
                        drops = int(fields[10]) if len(fields) > 10 else 0

                        total_packets_recv += ipkts
                        total_errors_in += ierrs
                        total_bytes_recv += ibytes
                        total_packets_sent += opkts
                        total_errors_out += oerrs
                        total_bytes_sent += obytes
                        total_drops_in += drops
                    except (ValueError, IndexError):
                        # Skip lines that don't have the expected format
                        continue

            return {
                "bytes_recv": total_bytes_recv,
                "bytes_sent": total_bytes_sent,
                "packets_recv": total_packets_recv,
                "packets_sent": total_packets_sent,
                "errors_in": total_errors_in,
                "errors_out": total_errors_out,
                "drops_in": total_drops_in,
                "drops_out": total_drops_out,
            }

        except Exception as e:
            logger.error(f"Error reading network I/O stats: {e}")
            return {
                "bytes_recv": 0,
                "bytes_sent": 0,
                "packets_recv": 0,
                "packets_sent": 0,
                "errors_in": 0,
                "errors_out": 0,
                "drops_in": 0,
                "drops_out": 0,
            }

    def _get_connection_stats(self) -> dict:
        """
        Get TCP connection statistics

        Returns:
            Dict with connection counts by state
        """
        if IS_LINUX:
            return self._get_connection_stats_linux()
        elif IS_MACOS:
            return self._get_connection_stats_macos()
        else:
            logger.warning(f"Unsupported platform: {platform.system()}")
            return {"established": 0, "listen": 0, "time_wait": 0, "other": 0}

    def _get_connection_stats_linux(self) -> dict:
        """Get connection stats on Linux using /proc/net/tcp"""
        try:
            # TCP connection states (hex values from kernel)
            # 01=ESTABLISHED, 0A=LISTEN, 06=TIME_WAIT
            state_counts = {"established": 0, "listen": 0, "time_wait": 0, "other": 0}

            # Check both IPv4 and IPv6
            for tcp_file in ["/proc/net/tcp", "/proc/net/tcp6"]:
                if not Path(tcp_file).exists():
                    continue

                with open(tcp_file, "r") as f:
                    lines = f.readlines()

                # Skip header (first line)
                for line in lines[1:]:
                    fields = line.split()
                    if len(fields) < 4:
                        continue

                    # State is in field 3 (0-indexed)
                    state = fields[3]

                    if state == "01":  # ESTABLISHED
                        state_counts["established"] += 1
                    elif state == "0A":  # LISTEN
                        state_counts["listen"] += 1
                    elif state == "06":  # TIME_WAIT
                        state_counts["time_wait"] += 1
                    else:
                        state_counts["other"] += 1

            return state_counts

        except Exception as e:
            logger.error(f"Error reading connection stats: {e}")
            return {"established": 0, "listen": 0, "time_wait": 0, "other": 0}

    def _get_connection_stats_macos(self) -> dict:
        """Get connection stats on macOS using netstat"""
        try:
            # Use netstat -an to get connection states
            result = subprocess.run(
                ["netstat", "-an", "-p", "tcp"],
                capture_output=True,
                text=True,
                timeout=2
            )

            if result.returncode != 0:
                return {"established": 0, "listen": 0, "time_wait": 0, "other": 0}

            state_counts = {"established": 0, "listen": 0, "time_wait": 0, "other": 0}

            lines = result.stdout.strip().split('\n')
            for line in lines:
                line_upper = line.upper()
                if 'ESTABLISHED' in line_upper:
                    state_counts["established"] += 1
                elif 'LISTEN' in line_upper:
                    state_counts["listen"] += 1
                elif 'TIME_WAIT' in line_upper:
                    state_counts["time_wait"] += 1
                elif 'tcp' in line.lower() or 'tcp4' in line.lower() or 'tcp6' in line.lower():
                    # Count other TCP connection states
                    if not any(x in line_upper for x in ['ESTABLISHED', 'LISTEN', 'TIME_WAIT', 'Proto']):
                        state_counts["other"] += 1

            return state_counts

        except Exception as e:
            logger.error(f"Error reading connection stats: {e}")
            return {"established": 0, "listen": 0, "time_wait": 0, "other": 0}

    def validate(self) -> bool:
        """Validate collector can access network metrics"""
        if IS_LINUX:
            return Path("/proc/net/dev").exists()
        elif IS_MACOS:
            # Check if we can run netstat
            try:
                result = subprocess.run(
                    ["netstat", "-ib"],
                    capture_output=True,
                    timeout=2
                )
                return result.returncode == 0
            except Exception:
                return False
        else:
            return False
