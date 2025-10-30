"""
Process tracer - traces system processes and their resource usage
"""

from pathlib import Path
from typing import Dict, Any, List, Optional

from logly.utils.logger import get_logger


logger = get_logger(__name__)


class ProcessTracer:
    """Traces system processes and their relationships"""

    def __init__(self):
        """Initialize process tracer"""
        self._proc_path = Path('/proc')

    def trace_process(self, pid: int) -> Optional[Dict[str, Any]]:
        """
        Trace a specific process

        Args:
            pid: Process ID to trace

        Returns:
            Complete process information
        """
        proc_dir = self._proc_path / str(pid)

        if not proc_dir.exists():
            logger.debug(f"Process {pid} not found")
            return None

        trace = {
            'pid': pid,
            'name': None,
            'cmdline': self._get_cmdline(pid),
            'status': self._get_status(pid),
            'stats': self._get_stats(pid),
            'io': self._get_io_stats(pid),
            'memory_info': {},
            'open_files_count': 0,
            'connections_count': 0,
            'parent_pid': None,
            'children': [],
        }

        # Extract name from status
        if trace['status']:
            trace['name'] = trace['status'].get('name')
            trace['parent_pid'] = trace['status'].get('ppid')
            trace['memory_info'] = {
                'vm_size': trace['status'].get('vm_size', 0),
                'vm_rss': trace['status'].get('vm_rss', 0),
            }

        # Count open files
        trace['open_files_count'] = self._count_open_files(pid)

        # Get children
        trace['children'] = self._get_child_processes(pid)

        return trace

    def trace_by_name(self, process_name: str) -> List[Dict[str, Any]]:
        """
        Trace all processes matching a name

        Args:
            process_name: Process name to search for

        Returns:
            List of process traces
        """
        traces = []
        pids = self.find_process_by_name(process_name)

        for pid in pids:
            trace = self.trace_process(pid)
            if trace:
                traces.append(trace)

        return traces

    def _get_cmdline(self, pid: int) -> Optional[str]:
        """Get process command line"""
        try:
            cmdline_path = self._proc_path / str(pid) / 'cmdline'
            if cmdline_path.exists():
                cmdline = cmdline_path.read_text()
                return cmdline.replace('\x00', ' ').strip()
        except Exception as e:
            logger.debug(f"Error reading cmdline for {pid}: {e}")
        return None

    def _get_status(self, pid: int) -> Dict[str, Any]:
        """Get process status"""
        status = {}

        try:
            status_path = self._proc_path / str(pid) / 'status'
            if status_path.exists():
                content = status_path.read_text()

                for line in content.split('\n'):
                    if ':' not in line:
                        continue

                    key, value = line.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip()

                    # Parse specific fields
                    if key == 'name':
                        status['name'] = value
                    elif key == 'state':
                        status['state'] = value.split()[0]
                    elif key == 'ppid':
                        status['ppid'] = int(value)
                    elif key == 'uid':
                        status['uid'] = value
                    elif key == 'vmsize':
                        status['vm_size'] = self._parse_memory(value)
                    elif key == 'vmrss':
                        status['vm_rss'] = self._parse_memory(value)
                    elif key == 'threads':
                        status['threads'] = int(value)
                    elif key == 'voluntary_ctxt_switches':
                        status['voluntary_switches'] = int(value)
                    elif key == 'nonvoluntary_ctxt_switches':
                        status['nonvoluntary_switches'] = int(value)

        except Exception as e:
            logger.debug(f"Error reading status for {pid}: {e}")

        return status

    def _get_stats(self, pid: int) -> Dict[str, Any]:
        """Get process statistics from /proc/[pid]/stat"""
        stats = {}

        try:
            stat_path = self._proc_path / str(pid) / 'stat'
            if stat_path.exists():
                content = stat_path.read_text()

                # Parse the stat file (format: pid (comm) state ppid pgrp ...)
                # Find the last ')' to handle process names with spaces
                rparen = content.rfind(')')
                if rparen == -1:
                    return stats

                fields = content[rparen+2:].split()

                if len(fields) >= 20:
                    stats['state'] = fields[0]
                    stats['ppid'] = int(fields[1])
                    stats['utime'] = int(fields[11])  # CPU time in user mode
                    stats['stime'] = int(fields[12])  # CPU time in kernel mode
                    stats['priority'] = int(fields[15])
                    stats['nice'] = int(fields[16])
                    stats['num_threads'] = int(fields[17])
                    stats['start_time'] = int(fields[19])

        except Exception as e:
            logger.debug(f"Error reading stats for {pid}: {e}")

        return stats

    def _get_io_stats(self, pid: int) -> Dict[str, Any]:
        """Get process I/O statistics"""
        io_stats = {}

        try:
            io_path = self._proc_path / str(pid) / 'io'
            if io_path.exists():
                content = io_path.read_text()

                for line in content.split('\n'):
                    if ':' not in line:
                        continue

                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()

                    if key == 'rchar':
                        io_stats['read_bytes'] = int(value)
                    elif key == 'wchar':
                        io_stats['write_bytes'] = int(value)
                    elif key == 'syscr':
                        io_stats['read_syscalls'] = int(value)
                    elif key == 'syscw':
                        io_stats['write_syscalls'] = int(value)

        except Exception as e:
            logger.debug(f"Error reading I/O stats for {pid}: {e}")

        return io_stats

    def _count_open_files(self, pid: int) -> int:
        """Count open files for a process"""
        try:
            fd_dir = self._proc_path / str(pid) / 'fd'
            if fd_dir.exists():
                return len(list(fd_dir.iterdir()))
        except Exception:
            pass
        return 0

    def _get_child_processes(self, pid: int) -> List[int]:
        """Get child processes of a PID"""
        children = []

        try:
            for entry in self._proc_path.iterdir():
                if not entry.name.isdigit():
                    continue

                try:
                    stat_path = entry / 'stat'
                    if stat_path.exists():
                        content = stat_path.read_text()
                        rparen = content.rfind(')')
                        if rparen != -1:
                            fields = content[rparen+2:].split()
                            if len(fields) >= 2:
                                ppid = int(fields[1])
                                if ppid == pid:
                                    children.append(int(entry.name))
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"Error finding children of {pid}: {e}")

        return children

    def _parse_memory(self, value: str) -> int:
        """Parse memory value from status file (e.g., '1234 kB')"""
        try:
            parts = value.split()
            if len(parts) >= 1:
                return int(parts[0])
        except ValueError:
            pass
        return 0

    def get_all_processes(self) -> List[int]:
        """Get list of all process IDs"""
        pids = []

        try:
            for entry in self._proc_path.iterdir():
                if entry.name.isdigit():
                    pids.append(int(entry.name))
        except Exception as e:
            logger.error(f"Error listing processes: {e}")

        return sorted(pids)

    def find_process_by_name(self, name: str) -> List[int]:
        """Find processes by name"""
        matching_pids = []

        for pid in self.get_all_processes():
            cmdline = self._get_cmdline(pid)
            status = self._get_status(pid)

            # Check cmdline
            if cmdline and name.lower() in cmdline.lower():
                matching_pids.append(pid)
                continue

            # Check process name
            if status and status.get('name'):
                if name.lower() in status['name'].lower():
                    matching_pids.append(pid)

        return matching_pids

    def get_resource_summary(self, pids: List[int]) -> Dict[str, Any]:
        """
        Get resource usage summary for a list of processes

        Args:
            pids: List of process IDs

        Returns:
            Aggregated resource usage
        """
        summary = {
            'total_memory_rss': 0,
            'total_memory_vm': 0,
            'total_read_bytes': 0,
            'total_write_bytes': 0,
            'process_count': len(pids),
            'thread_count': 0,
        }

        for pid in pids:
            status = self._get_status(pid)
            io_stats = self._get_io_stats(pid)

            if status:
                summary['total_memory_rss'] += status.get('vm_rss', 0)
                summary['total_memory_vm'] += status.get('vm_size', 0)
                summary['thread_count'] += status.get('threads', 0)

            if io_stats:
                summary['total_read_bytes'] += io_stats.get('read_bytes', 0)
                summary['total_write_bytes'] += io_stats.get('write_bytes', 0)

        return summary
