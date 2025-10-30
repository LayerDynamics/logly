"""
Report generator for summary statistics
"""

import logging
from datetime import datetime
from typing import Dict, Any

from logly.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate summary reports"""

    def __init__(self, store: SQLiteStore):
        """
        Initialize report generator

        Args:
            store: SQLiteStore instance
        """
        self.store = store

    def generate_summary_report(self, output_path: str, start_time: int, end_time: int):
        """
        Generate a text summary report

        Args:
            output_path: Output file path
            start_time: Start timestamp
            end_time: End timestamp
        """
        logger.info(f"Generating summary report to {output_path}")

        # Get statistics
        stats = self._compute_statistics(start_time, end_time)

        # Generate report
        report_lines = []
        report_lines.append("=" * 70)
        report_lines.append("LOGLY SUMMARY REPORT")
        report_lines.append("=" * 70)
        report_lines.append("")
        report_lines.append(
            f"Report Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}"
        )
        report_lines.append(f"Duration: {(end_time - start_time) / 3600:.1f} hours")
        report_lines.append("")

        # System Metrics Summary
        report_lines.append("-" * 70)
        report_lines.append("SYSTEM METRICS")
        report_lines.append("-" * 70)
        if stats["system"]:
            report_lines.append(
                f"  CPU Usage (avg):        {stats['system'].get('avg_cpu', 0):.1f}%"
            )
            report_lines.append(
                f"  CPU Usage (max):        {stats['system'].get('max_cpu', 0):.1f}%"
            )
            report_lines.append(
                f"  Memory Usage (avg):     {stats['system'].get('avg_memory', 0):.1f}%"
            )
            report_lines.append(
                f"  Memory Usage (max):     {stats['system'].get('max_memory', 0):.1f}%"
            )
            report_lines.append(
                f"  Disk Usage (avg):       {stats['system'].get('avg_disk', 0):.1f}%"
            )
        else:
            report_lines.append("  No system metrics found")
        report_lines.append("")

        # Network Metrics Summary
        report_lines.append("-" * 70)
        report_lines.append("NETWORK METRICS")
        report_lines.append("-" * 70)
        if stats["network"]:
            report_lines.append(
                f"  Bytes Sent (total):     {self._format_bytes(stats['network'].get('total_sent', 0))}"
            )
            report_lines.append(
                f"  Bytes Received (total): {self._format_bytes(stats['network'].get('total_recv', 0))}"
            )
            report_lines.append(
                f"  Packets Sent:           {stats['network'].get('total_packets_sent', 0):,}"
            )
            report_lines.append(
                f"  Packets Received:       {stats['network'].get('total_packets_recv', 0):,}"
            )
        else:
            report_lines.append("  No network metrics found")
        report_lines.append("")

        # Log Events Summary
        report_lines.append("-" * 70)
        report_lines.append("LOG EVENTS")
        report_lines.append("-" * 70)
        if stats["logs"]:
            report_lines.append(
                f"  Total Events:           {stats['logs'].get('total', 0):,}"
            )
            report_lines.append(
                f"  Failed Logins:          {stats['logs'].get('failed_logins', 0):,}"
            )
            report_lines.append(
                f"  Banned IPs:             {stats['logs'].get('banned_ips', 0):,}"
            )
            report_lines.append(
                f"  Errors:                 {stats['logs'].get('errors', 0):,}"
            )
            report_lines.append(
                f"  Warnings:               {stats['logs'].get('warnings', 0):,}"
            )
        else:
            report_lines.append("  No log events found")
        report_lines.append("")

        # Database Statistics
        report_lines.append("-" * 70)
        report_lines.append("DATABASE STATISTICS")
        report_lines.append("-" * 70)
        db_stats = self.store.get_stats()
        report_lines.append(
            f"  System Metrics Records: {db_stats.get('system_metrics', 0):,}"
        )
        report_lines.append(
            f"  Network Metrics Records:{db_stats.get('network_metrics', 0):,}"
        )
        report_lines.append(
            f"  Log Events Records:     {db_stats.get('log_events', 0):,}"
        )
        report_lines.append(
            f"  Hourly Aggregates:      {db_stats.get('hourly_aggregates', 0):,}"
        )
        report_lines.append(
            f"  Daily Aggregates:       {db_stats.get('daily_aggregates', 0):,}"
        )
        report_lines.append(
            f"  Database Size:          {db_stats.get('database_size_mb', 0):.2f} MB"
        )
        report_lines.append("")

        report_lines.append("=" * 70)

        # Write report
        with open(output_path, "w") as f:
            f.write("\n".join(report_lines))

        logger.info(f"Generated summary report at {output_path}")

    def _compute_statistics(self, start_time: int, end_time: int) -> Dict[str, Any]:
        """Compute statistics for the time period"""
        stats: Dict[str, Any] = {"system": None, "network": None, "logs": None}

        # System metrics stats
        system_metrics = self.store.get_system_metrics(start_time, end_time)
        if system_metrics:
            cpu_values = [
                m["cpu_percent"] for m in system_metrics if m.get("cpu_percent")
            ]
            mem_values = [
                m["memory_percent"] for m in system_metrics if m.get("memory_percent")
            ]
            disk_values = [
                m["disk_percent"] for m in system_metrics if m.get("disk_percent")
            ]

            stats["system"] = {
                "avg_cpu": sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                "max_cpu": max(cpu_values) if cpu_values else 0,
                "avg_memory": sum(mem_values) / len(mem_values) if mem_values else 0,
                "max_memory": max(mem_values) if mem_values else 0,
                "avg_disk": sum(disk_values) / len(disk_values) if disk_values else 0,
            }

        # Network metrics stats
        network_metrics = self.store.get_network_metrics(start_time, end_time)
        if network_metrics:
            # Get first and last to calculate delta
            first = network_metrics[-1] if network_metrics else {}
            last = network_metrics[0] if network_metrics else {}

            stats["network"] = {
                "total_sent": (last.get("bytes_sent", 0) - first.get("bytes_sent", 0))
                if first and last
                else 0,
                "total_recv": (last.get("bytes_recv", 0) - first.get("bytes_recv", 0))
                if first and last
                else 0,
                "total_packets_sent": (
                    (last.get("packets_sent") or 0) - (first.get("packets_sent") or 0)
                )
                if first and last
                else 0,
                "total_packets_recv": (
                    (last.get("packets_recv") or 0) - (first.get("packets_recv") or 0)
                )
                if first and last
                else 0,
            }

        # Log events stats
        log_events = self.store.get_log_events(start_time, end_time)
        if log_events:
            stats["logs"] = {
                "total": len(log_events),
                "failed_logins": sum(
                    1 for e in log_events if e.get("action") == "failed_login"
                ),
                "banned_ips": sum(1 for e in log_events if e.get("action") == "ban"),
                "errors": sum(1 for e in log_events if e.get("level") == "ERROR"),
                "warnings": sum(1 for e in log_events if e.get("level") == "WARNING"),
            }

        return stats

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human-readable format"""
        if bytes_value == 0:
            return "0 B"

        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        value = float(bytes_value)

        while value >= 1024 and unit_index < len(units) - 1:
            value /= 1024
            unit_index += 1

        # For bytes, show as integer; for larger units, show with 2 decimals
        if unit_index == 0:
            return f"{int(value)} {units[unit_index]}"
        return f"{value:.2f} {units[unit_index]}"

    def generate_full_report(self, output_path: str, start_time: int, end_time: int):
        """
        Generate a full HTML report with metrics, charts, and analysis

        Args:
            output_path: Output file path for HTML report
            start_time: Start timestamp
            end_time: End timestamp
        """
        logger.info(f"Generating full HTML report to {output_path}")

        # Get statistics
        stats = self._compute_statistics(start_time, end_time)

        # Build HTML report
        html_lines = []
        html_lines.append("<!DOCTYPE html>")
        html_lines.append("<html>")
        html_lines.append("<head>")
        html_lines.append("    <meta charset='UTF-8'>")
        html_lines.append("    <title>Logly Full Report</title>")
        html_lines.append("    <style>")
        html_lines.append(
            "        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }"
        )
        html_lines.append(
            "        .container { max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }"
        )
        html_lines.append(
            "        h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }"
        )
        html_lines.append(
            "        h2 { color: #555; border-bottom: 2px solid #ddd; padding-bottom: 8px; margin-top: 30px; }"
        )
        html_lines.append("        .section { margin: 20px 0; }")
        html_lines.append(
            "        .metric { background-color: #f9f9f9; padding: 15px; margin: 10px 0; border-left: 4px solid #4CAF50; }"
        )
        html_lines.append("        .metric-label { font-weight: bold; color: #666; }")
        html_lines.append("        .metric-value { font-size: 1.2em; color: #333; }")
        html_lines.append(
            "        table { width: 100%; border-collapse: collapse; margin: 15px 0; }"
        )
        html_lines.append(
            "        th { background-color: #4CAF50; color: white; padding: 12px; text-align: left; }"
        )
        html_lines.append(
            "        td { padding: 10px; border-bottom: 1px solid #ddd; }"
        )
        html_lines.append("        tr:hover { background-color: #f5f5f5; }")
        html_lines.append("        .timestamp { color: #888; font-size: 0.9em; }")
        html_lines.append("    </style>")
        html_lines.append("</head>")
        html_lines.append("<body>")
        html_lines.append("    <div class='container'>")
        html_lines.append("        <h1>Logly Full System Report</h1>")
        html_lines.append(
            f"        <p class='timestamp'>Generated: {datetime.fromtimestamp(int(datetime.now().timestamp()))}</p>"
        )
        html_lines.append(
            f"        <p class='timestamp'>Report Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}</p>"
        )

        # System Metrics Section
        html_lines.append("        <h2>System Metrics</h2>")
        html_lines.append("        <div class='section'>")
        if stats["system"]:
            html_lines.append("            <div class='metric'>")
            html_lines.append(
                "                <span class='metric-label'>Average CPU Usage:</span>"
            )
            html_lines.append(
                f"                <span class='metric-value'>{stats['system'].get('avg_cpu', 0):.1f}%</span>"
            )
            html_lines.append("            </div>")
            html_lines.append("            <div class='metric'>")
            html_lines.append(
                "                <span class='metric-label'>Peak CPU Usage:</span>"
            )
            html_lines.append(
                f"                <span class='metric-value'>{stats['system'].get('max_cpu', 0):.1f}%</span>"
            )
            html_lines.append("            </div>")
            html_lines.append("            <div class='metric'>")
            html_lines.append(
                "                <span class='metric-label'>Average Memory Usage:</span>"
            )
            html_lines.append(
                f"                <span class='metric-value'>{stats['system'].get('avg_memory', 0):.1f}%</span>"
            )
            html_lines.append("            </div>")
        else:
            html_lines.append("            <p>No system metrics available</p>")
        html_lines.append("        </div>")

        # Network Activity Section
        html_lines.append("        <h2>Network Activity</h2>")
        html_lines.append("        <div class='section'>")
        if stats["network"]:
            html_lines.append("            <div class='metric'>")
            html_lines.append(
                "                <span class='metric-label'>Total Bytes Sent:</span>"
            )
            html_lines.append(
                f"                <span class='metric-value'>{self._format_bytes(stats['network'].get('total_sent', 0))}</span>"
            )
            html_lines.append("            </div>")
            html_lines.append("            <div class='metric'>")
            html_lines.append(
                "                <span class='metric-label'>Total Bytes Received:</span>"
            )
            html_lines.append(
                f"                <span class='metric-value'>{self._format_bytes(stats['network'].get('total_recv', 0))}</span>"
            )
            html_lines.append("            </div>")
        else:
            html_lines.append("            <p>No network metrics available</p>")
        html_lines.append("        </div>")

        # Log Events Section
        html_lines.append("        <h2>Log Events</h2>")
        html_lines.append("        <div class='section'>")
        if stats["logs"]:
            html_lines.append("            <table>")
            html_lines.append("                <tr>")
            html_lines.append("                    <th>Event Type</th>")
            html_lines.append("                    <th>Count</th>")
            html_lines.append("                </tr>")
            html_lines.append("                <tr>")
            html_lines.append("                    <td>Total Events</td>")
            html_lines.append(
                f"                    <td>{stats['logs'].get('total', 0):,}</td>"
            )
            html_lines.append("                </tr>")
            html_lines.append("                <tr>")
            html_lines.append("                    <td>Failed Logins</td>")
            html_lines.append(
                f"                    <td>{stats['logs'].get('failed_logins', 0):,}</td>"
            )
            html_lines.append("                </tr>")
            html_lines.append("                <tr>")
            html_lines.append("                    <td>Banned IPs</td>")
            html_lines.append(
                f"                    <td>{stats['logs'].get('banned_ips', 0):,}</td>"
            )
            html_lines.append("                </tr>")
            html_lines.append("                <tr>")
            html_lines.append("                    <td>Errors</td>")
            html_lines.append(
                f"                    <td>{stats['logs'].get('errors', 0):,}</td>"
            )
            html_lines.append("                </tr>")
            html_lines.append("            </table>")
        else:
            html_lines.append("            <p>No log events available</p>")
        html_lines.append("        </div>")

        # Database Stats Section
        html_lines.append("        <h2>Database Statistics</h2>")
        html_lines.append("        <div class='section'>")
        db_stats = self.store.get_stats()
        html_lines.append("            <table>")
        html_lines.append("                <tr>")
        html_lines.append("                    <th>Metric</th>")
        html_lines.append("                    <th>Value</th>")
        html_lines.append("                </tr>")
        html_lines.append("                <tr>")
        html_lines.append("                    <td>System Metrics</td>")
        html_lines.append(
            f"                    <td>{db_stats.get('system_metrics', 0):,}</td>"
        )
        html_lines.append("                </tr>")
        html_lines.append("                <tr>")
        html_lines.append("                    <td>Network Metrics</td>")
        html_lines.append(
            f"                    <td>{db_stats.get('network_metrics', 0):,}</td>"
        )
        html_lines.append("                </tr>")
        html_lines.append("                <tr>")
        html_lines.append("                    <td>Log Events</td>")
        html_lines.append(
            f"                    <td>{db_stats.get('log_events', 0):,}</td>"
        )
        html_lines.append("                </tr>")
        html_lines.append("                <tr>")
        html_lines.append("                    <td>Database Size</td>")
        html_lines.append(
            f"                    <td>{db_stats.get('database_size_mb', 0):.2f} MB</td>"
        )
        html_lines.append("                </tr>")
        html_lines.append("            </table>")
        html_lines.append("        </div>")

        html_lines.append("    </div>")
        html_lines.append("</body>")
        html_lines.append("</html>")

        # Write report
        with open(output_path, "w") as f:
            f.write("\n".join(html_lines))

        logger.info(f"Generated full HTML report at {output_path}")

    def generate_security_report(
        self, output_path: str, start_time: int, end_time: int
    ):
        """
        Generate a security-focused HTML report

        Args:
            output_path: Output file path for HTML report
            start_time: Start timestamp
            end_time: End timestamp
        """
        logger.info(f"Generating security report to {output_path}")

        # Get log events for security analysis
        log_events = self.store.get_log_events(start_time, end_time)

        # Analyze security-related events
        failed_logins = [e for e in log_events if e.get("action") == "failed_login"]
        banned_ips = [e for e in log_events if e.get("action") == "ban"]
        errors = [e for e in log_events if e.get("level") == "ERROR"]
        warnings = [e for e in log_events if e.get("level") == "WARNING"]

        # Group failed logins by IP
        failed_by_ip: Dict[str, int] = {}
        for event in failed_logins:
            source_ip = event.get("source_ip", "unknown")
            failed_by_ip[source_ip] = failed_by_ip.get(source_ip, 0) + 1

        # Build HTML report
        html_lines = []
        html_lines.append("<!DOCTYPE html>")
        html_lines.append("<html>")
        html_lines.append("<head>")
        html_lines.append("    <meta charset='UTF-8'>")
        html_lines.append("    <title>Logly Security Report</title>")
        html_lines.append("    <style>")
        html_lines.append(
            "        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }"
        )
        html_lines.append(
            "        .container { max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }"
        )
        html_lines.append(
            "        h1 { color: #d32f2f; border-bottom: 3px solid #d32f2f; padding-bottom: 10px; }"
        )
        html_lines.append(
            "        h2 { color: #555; border-bottom: 2px solid #ddd; padding-bottom: 8px; margin-top: 30px; }"
        )
        html_lines.append("        .section { margin: 20px 0; }")
        html_lines.append(
            "        .alert { background-color: #ffebee; padding: 15px; margin: 10px 0; border-left: 4px solid #d32f2f; }"
        )
        html_lines.append(
            "        .warning { background-color: #fff3e0; padding: 15px; margin: 10px 0; border-left: 4px solid #f57c00; }"
        )
        html_lines.append(
            "        .info { background-color: #e3f2fd; padding: 15px; margin: 10px 0; border-left: 4px solid #1976d2; }"
        )
        html_lines.append(
            "        table { width: 100%; border-collapse: collapse; margin: 15px 0; }"
        )
        html_lines.append(
            "        th { background-color: #d32f2f; color: white; padding: 12px; text-align: left; }"
        )
        html_lines.append(
            "        td { padding: 10px; border-bottom: 1px solid #ddd; }"
        )
        html_lines.append("        tr:hover { background-color: #f5f5f5; }")
        html_lines.append("        .timestamp { color: #888; font-size: 0.9em; }")
        html_lines.append("        .critical { color: #d32f2f; font-weight: bold; }")
        html_lines.append("    </style>")
        html_lines.append("</head>")
        html_lines.append("<body>")
        html_lines.append("    <div class='container'>")
        html_lines.append("        <h1>Security Incident Report</h1>")
        html_lines.append(
            f"        <p class='timestamp'>Generated: {datetime.fromtimestamp(int(datetime.now().timestamp()))}</p>"
        )
        html_lines.append(
            f"        <p class='timestamp'>Report Period: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}</p>"
        )

        # Executive Summary
        html_lines.append("        <h2>Executive Summary</h2>")
        html_lines.append("        <div class='section'>")
        html_lines.append("            <div class='alert'>")
        html_lines.append(
            f"                <p><strong>Total Security Events:</strong> {len(log_events)}</p>"
        )
        html_lines.append(
            f"                <p><strong>Failed Login Attempts:</strong> <span class='critical'>{len(failed_logins)}</span></p>"
        )
        html_lines.append(
            f"                <p><strong>IPs Banned:</strong> <span class='critical'>{len(banned_ips)}</span></p>"
        )
        html_lines.append(
            f"                <p><strong>Errors:</strong> {len(errors)}</p>"
        )
        html_lines.append(
            f"                <p><strong>Warnings:</strong> {len(warnings)}</p>"
        )
        html_lines.append("            </div>")
        html_lines.append("        </div>")

        # Failed Login Analysis
        html_lines.append("        <h2>Failed Login Analysis</h2>")
        html_lines.append("        <div class='section'>")
        if failed_by_ip:
            html_lines.append("            <table>")
            html_lines.append("                <tr>")
            html_lines.append("                    <th>Source IP</th>")
            html_lines.append("                    <th>Failed Attempts</th>")
            html_lines.append("                    <th>Risk Level</th>")
            html_lines.append("                </tr>")

            # Sort by count descending
            for ip, count in sorted(
                failed_by_ip.items(), key=lambda x: x[1], reverse=True
            ):
                risk_level = (
                    "HIGH" if count >= 10 else "MEDIUM" if count >= 5 else "LOW"
                )
                risk_class = (
                    "critical" if count >= 10 else "warning" if count >= 5 else ""
                )
                html_lines.append("                <tr>")
                html_lines.append(f"                    <td>{ip}</td>")
                html_lines.append(f"                    <td>{count}</td>")
                html_lines.append(
                    f"                    <td class='{risk_class}'>{risk_level}</td>"
                )
                html_lines.append("                </tr>")

            html_lines.append("            </table>")
        else:
            html_lines.append("            <p>No failed login attempts detected</p>")
        html_lines.append("        </div>")

        # Banned IPs
        html_lines.append("        <h2>Banned IP Addresses</h2>")
        html_lines.append("        <div class='section'>")
        if banned_ips:
            html_lines.append("            <table>")
            html_lines.append("                <tr>")
            html_lines.append("                    <th>IP Address</th>")
            html_lines.append("                    <th>Ban Time</th>")
            html_lines.append("                    <th>Service</th>")
            html_lines.append("                </tr>")

            for event in banned_ips[:50]:  # Limit to 50 most recent
                ban_time = datetime.fromtimestamp(event.get("timestamp", 0))
                ip = event.get("source_ip", "unknown")
                service = event.get("service", "unknown")
                html_lines.append("                <tr>")
                html_lines.append(f"                    <td>{ip}</td>")
                html_lines.append(f"                    <td>{ban_time}</td>")
                html_lines.append(f"                    <td>{service}</td>")
                html_lines.append("                </tr>")

            html_lines.append("            </table>")
        else:
            html_lines.append("            <p>No IPs banned during this period</p>")
        html_lines.append("        </div>")

        # Errors and Warnings
        html_lines.append("        <h2>Critical Events</h2>")
        html_lines.append("        <div class='section'>")
        if errors or warnings:
            html_lines.append("            <div class='warning'>")
            html_lines.append(
                f"                <p><strong>Total Errors:</strong> {len(errors)}</p>"
            )
            html_lines.append(
                f"                <p><strong>Total Warnings:</strong> {len(warnings)}</p>"
            )

            # Show recent errors
            if errors:
                html_lines.append("                <h3>Recent Errors:</h3>")
                html_lines.append("                <ul>")
                for event in errors[:10]:  # Show 10 most recent
                    msg = event.get("message", "No message")
                    timestamp = datetime.fromtimestamp(event.get("timestamp", 0))
                    html_lines.append(
                        f"                    <li>[{timestamp}] {msg}</li>"
                    )
                html_lines.append("                </ul>")

            html_lines.append("            </div>")
        else:
            html_lines.append("            <p>No critical events detected</p>")
        html_lines.append("        </div>")

        # Recommendations
        html_lines.append("        <h2>Recommendations</h2>")
        html_lines.append("        <div class='section'>")
        html_lines.append("            <div class='info'>")
        html_lines.append("                <ul>")

        if len(failed_logins) > 50:
            html_lines.append(
                "                    <li>High number of failed login attempts detected. Consider implementing rate limiting or CAPTCHA.</li>"
            )

        if any(count >= 10 for count in failed_by_ip.values()):
            html_lines.append(
                "                    <li>Multiple brute force attempts detected. Review firewall rules and consider geo-blocking.</li>"
            )

        if len(banned_ips) > 20:
            html_lines.append(
                "                    <li>Significant ban activity. Consider reducing fail2ban threshold or implementing additional security layers.</li>"
            )

        if len(errors) > 100:
            html_lines.append(
                "                    <li>High error rate detected. Investigate application logs and system health.</li>"
            )

        # Default recommendations
        html_lines.append(
            "                    <li>Regularly review security logs and update security policies.</li>"
        )
        html_lines.append(
            "                    <li>Ensure all systems are patched and up to date.</li>"
        )
        html_lines.append(
            "                    <li>Monitor for unusual patterns in authentication attempts.</li>"
        )

        html_lines.append("                </ul>")
        html_lines.append("            </div>")
        html_lines.append("        </div>")

        html_lines.append("    </div>")
        html_lines.append("</body>")
        html_lines.append("</html>")

        # Write report
        with open(output_path, "w") as f:
            f.write("\n".join(html_lines))

        logger.info(f"Generated security report at {output_path}")
