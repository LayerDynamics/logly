"""
System metrics collector - CPU, memory, disk, load average
Uses /proc filesystem on Linux, subprocess/os calls on macOS for minimal dependencies
"""

import os
import platform
import subprocess
from pathlib import Path
from typing import Optional, Tuple

from logly.collectors.base_collector import BaseCollector
from logly.storage.models import SystemMetric
from logly.utils.logger import get_logger


logger = get_logger(__name__)

# Detect platform once at module load
IS_LINUX = platform.system() == "Linux"
IS_MACOS = platform.system() == "Darwin"


class SystemMetricsCollector(BaseCollector):
    """Collects system metrics using /proc filesystem"""

    def __init__(self, config: dict):
        super().__init__(config)
        self.metrics_to_collect = config.get("metrics", [])
        self._last_cpu_stats = None
        self._last_disk_io = None

    def collect(self) -> SystemMetric:
        """
        Collect system metrics

        Returns:
            SystemMetric object with current system stats
        """
        metric = SystemMetric.now()

        # Collect requested metrics
        if (
            "cpu_percent" in self.metrics_to_collect
            or "cpu_count" in self.metrics_to_collect
        ):
            cpu_percent, cpu_count = self._get_cpu_stats()
            if "cpu_percent" in self.metrics_to_collect:
                metric.cpu_percent = cpu_percent
            if "cpu_count" in self.metrics_to_collect:
                metric.cpu_count = cpu_count

        if any(m.startswith("memory_") for m in self.metrics_to_collect):
            mem_stats = self._get_memory_stats()
            if "memory_total" in self.metrics_to_collect:
                metric.memory_total = mem_stats["total"]
            if "memory_available" in self.metrics_to_collect:
                metric.memory_available = mem_stats["available"]
            if "memory_percent" in self.metrics_to_collect:
                metric.memory_percent = mem_stats["percent"]

        if any(m.startswith("disk_") for m in self.metrics_to_collect):
            disk_stats = self._get_disk_stats()
            # Support both "disk_usage" and "disk_percent" for backwards compatibility
            if "disk_usage" in self.metrics_to_collect or "disk_percent" in self.metrics_to_collect:
                metric.disk_total = disk_stats["total"]
                metric.disk_used = disk_stats["used"]
                metric.disk_percent = disk_stats["percent"]
            if "disk_io" in self.metrics_to_collect:
                disk_io = self._get_disk_io_stats()
                metric.disk_read_bytes = disk_io["read_bytes"]
                metric.disk_write_bytes = disk_io["write_bytes"]

        if "load_average" in self.metrics_to_collect:
            load = self._get_load_average()
            metric.load_1min = load[0]
            metric.load_5min = load[1]
            metric.load_15min = load[2]

        return metric

    def _get_cpu_stats(self) -> Tuple[Optional[float], int]:
        """
        Get CPU usage percentage and count

        Returns:
            Tuple of (cpu_percent, cpu_count)
        """
        if IS_LINUX:
            return self._get_cpu_stats_linux()
        elif IS_MACOS:
            return self._get_cpu_stats_macos()
        else:
            logger.warning(f"Unsupported platform: {platform.system()}")
            return None, os.cpu_count() or 1

    def _get_cpu_stats_linux(self) -> Tuple[Optional[float], int]:
        """Get CPU stats on Linux using /proc/stat"""
        try:
            # Read /proc/stat for CPU times
            with open("/proc/stat", "r") as f:
                line = f.readline()  # First line is aggregate CPU
                fields = line.split()
                if fields[0] != "cpu":
                    return None, 0

                # Parse CPU times: user, nice, system, idle, iowait, irq, softirq
                times = [int(x) for x in fields[1:8]]
                idle = times[3]
                total = sum(times)

                # Calculate percentage since last call
                cpu_percent = None
                if self._last_cpu_stats:
                    last_idle, last_total = self._last_cpu_stats
                    total_diff = total - last_total
                    idle_diff = idle - last_idle
                    if total_diff > 0:
                        cpu_percent = round(
                            100.0 * (total_diff - idle_diff) / total_diff, 2
                        )

                self._last_cpu_stats = (idle, total)

                # Get CPU count
                cpu_count = os.cpu_count() or 1

                return cpu_percent, cpu_count

        except Exception as e:
            logger.error(f"Error reading CPU stats: {e}")
            return None, 0

    def _get_cpu_stats_macos(self) -> Tuple[Optional[float], int]:
        """Get CPU stats on macOS using top command (faster than iostat)"""
        try:
            # Get CPU count
            cpu_count = os.cpu_count() or 1

            # Use top in non-interactive mode with 1 sample for speed
            # top -l 1 gives instant snapshot
            result = subprocess.run(
                ["top", "-l", "1", "-n", "0"],
                capture_output=True,
                text=True,
                timeout=2
            )

            if result.returncode == 0:
                # Parse CPU usage line: "CPU usage: 5.12% user, 10.25% sys, 84.62% idle"
                for line in result.stdout.split('\n'):
                    if 'CPU usage' in line:
                        # Extract percentages
                        import re
                        # Match patterns like "5.12% user" and "10.25% sys"
                        user_match = re.search(r'([\d.]+)%\s+user', line)
                        sys_match = re.search(r'([\d.]+)%\s+sys', line)

                        if user_match and sys_match:
                            user = float(user_match.group(1))
                            system = float(sys_match.group(1))
                            cpu_percent = round(user + system, 2)
                            return cpu_percent, cpu_count

            # Fallback: return None for percent, but still return CPU count
            return None, cpu_count

        except Exception as e:
            logger.error(f"Error reading CPU stats: {e}")
            return None, os.cpu_count() or 1

    def _get_memory_stats(self) -> dict:
        """
        Get memory statistics

        Returns:
            Dict with total, available, and percent
        """
        if IS_LINUX:
            return self._get_memory_stats_linux()
        elif IS_MACOS:
            return self._get_memory_stats_macos()
        else:
            logger.warning(f"Unsupported platform: {platform.system()}")
            return {"total": 0, "available": 0, "percent": 0.0}

    def _get_memory_stats_linux(self) -> dict:
        """Get memory stats on Linux using /proc/meminfo"""
        try:
            mem_info = {}
            with open("/proc/meminfo", "r") as f:
                lines = f.readlines()

            for line in lines:
                parts = line.split()
                if len(parts) >= 2:
                    key = parts[0].rstrip(":")
                    value = int(parts[1]) * 1024  # Convert from KB to bytes
                    mem_info[key] = value

            total = mem_info.get("MemTotal", 0)
            available = mem_info.get("MemAvailable", 0)

            # If MemAvailable not present, estimate it
            if available == 0:
                free = mem_info.get("MemFree", 0)
                buffers = mem_info.get("Buffers", 0)
                cached = mem_info.get("Cached", 0)
                available = free + buffers + cached

            percent = 0.0
            if total > 0:
                percent = round(100.0 * (1 - available / total), 2)

            return {"total": total, "available": available, "percent": percent}

        except Exception as e:
            logger.error(f"Error reading memory stats: {e}")
            return {"total": 0, "available": 0, "percent": 0.0}

    def _get_memory_stats_macos(self) -> dict:
        """Get memory stats on macOS using sysctl and vm_stat"""
        try:
            # Get total memory using sysctl
            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                timeout=2
            )
            total = int(result.stdout.strip()) if result.returncode == 0 else 0

            # Get memory statistics using vm_stat
            result = subprocess.run(
                ["vm_stat"],
                capture_output=True,
                text=True,
                timeout=2
            )

            if result.returncode == 0:
                # Parse vm_stat output
                # Pages are 4096 bytes on macOS
                page_size = 4096
                free_pages = 0
                inactive_pages = 0

                for line in result.stdout.split('\n'):
                    if 'Pages free:' in line:
                        free_pages = int(line.split(':')[1].strip().rstrip('.'))
                    elif 'Pages inactive:' in line:
                        inactive_pages = int(line.split(':')[1].strip().rstrip('.'))

                # Available memory is approximately free + inactive
                available = (free_pages + inactive_pages) * page_size

                percent = 0.0
                if total > 0:
                    percent = round(100.0 * (1 - available / total), 2)

                return {"total": total, "available": available, "percent": percent}

            return {"total": total, "available": 0, "percent": 0.0}

        except Exception as e:
            logger.error(f"Error reading memory stats: {e}")
            return {"total": 0, "available": 0, "percent": 0.0}

    def _get_disk_stats(self, path: str = "/") -> dict:
        """
        Get disk usage statistics

        Args:
            path: Mount point to check (default: root)

        Returns:
            Dict with total, used, and percent
        """
        try:
            stat = os.statvfs(path)
            total = stat.f_blocks * stat.f_frsize
            free = stat.f_bavail * stat.f_frsize
            used = total - free

            percent = 0.0
            if total > 0:
                percent = round(100.0 * used / total, 2)

            return {"total": total, "used": used, "percent": percent}

        except Exception as e:
            logger.error(f"Error reading disk stats: {e}")
            return {"total": 0, "used": 0, "percent": 0.0}

    def _get_disk_io_stats(self) -> dict:
        """
        Get disk I/O statistics from /proc/diskstats

        Returns:
            Dict with read_bytes and write_bytes (cumulative)
        """
        try:
            # Parse /proc/diskstats for the main disk
            # Format: major minor name reads ... sectors_read ... writes ... sectors_written ...
            total_read_sectors = 0
            total_write_sectors = 0

            with open("/proc/diskstats", "r") as f:
                lines = f.readlines()

            for line in lines:
                fields = line.split()
                if len(fields) >= 14:
                    device = fields[2]
                    # Skip partition devices, only count whole disks (sda, vda, nvme0n1, etc.)
                    if device.startswith(("sd", "vd", "hd", "nvme")) and not any(
                        c.isdigit() for c in device[-1]
                    ):
                        read_sectors = int(fields[5])
                        write_sectors = int(fields[9])
                        total_read_sectors += read_sectors
                        total_write_sectors += write_sectors

            # Convert sectors to bytes (512 bytes per sector)
            read_bytes = total_read_sectors * 512
            write_bytes = total_write_sectors * 512

            return {"read_bytes": read_bytes, "write_bytes": write_bytes}

        except Exception as e:
            logger.error(f"Error reading disk I/O stats: {e}")
            return {"read_bytes": 0, "write_bytes": 0}

    def _get_load_average(self) -> Tuple[float, float, float]:
        """
        Get system load average

        Returns:
            Tuple of (1min, 5min, 15min) load averages
        """
        try:
            if IS_LINUX:
                with open("/proc/loadavg", "r") as f:
                    line = f.readline()
                    loads = line.split()[:3]
                    return (float(loads[0]), float(loads[1]), float(loads[2]))
            else:
                # os.getloadavg() works on Unix-like systems including macOS
                return os.getloadavg()

        except Exception as e:
            logger.error(f"Error reading load average: {e}")
            return (0.0, 0.0, 0.0)

    def validate(self) -> bool:
        """Validate collector can access system metrics"""
        if IS_LINUX:
            return Path("/proc/stat").exists() and Path("/proc/meminfo").exists()
        elif IS_MACOS:
            # Check if we can run sysctl
            try:
                result = subprocess.run(
                    ["sysctl", "-n", "hw.memsize"],
                    capture_output=True,
                    timeout=2
                )
                return result.returncode == 0
            except Exception:
                return False
        else:
            return False
