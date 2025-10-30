# Exporters

## What It Does

The exporters module provides tools for exporting collected data and generating reports in various formats. It enables users to extract data from the database for analysis, archival, or integration with external systems.

**Core capabilities:**

1. **CSV Export** - Export metrics and events to CSV format for spreadsheet analysis
2. **JSON Export** - Export data to structured JSON with metadata for programmatic processing
3. **Report Generation** - Generate human-readable text summary reports with statistics

**Key features:**

- Configurable timestamp formatting for readability
- Automatic timestamp conversion from Unix epochs
- Optional filtering by source and level for log events
- Structured JSON output with metadata (type, time range, count)
- Summary statistics calculation (averages, totals, maximums)
- Human-readable byte formatting (B, KB, MB, GB, TB)
- Database statistics inclusion in reports

### Detailed Breakdown

*csv_exporter*:
Exports data to CSV format with comma-separated values and headers. Core functionality:

- **Initialization** - Takes SQLiteStore and optional timestamp_format (default: "%Y-%m-%d %H:%M:%S") for converting Unix timestamps to readable strings.
- **System Metrics Export** - `export_system_metrics(output_path, start_time, end_time)` retrieves system metrics from database, adds timestamp_str column with formatted dates, writes CSV with all metric fields (cpu_percent, memory_percent, disk_percent, load averages, etc.), logs count of exported records.
- **Network Metrics Export** - `export_network_metrics(output_path, start_time, end_time)` retrieves network metrics, adds timestamp_str, writes CSV with network fields (bytes_sent/recv, packets_sent/recv, errors_in/out, drops_in/out, connection counts), logs count.
- **Log Events Export** - `export_log_events(output_path, start_time, end_time, source, level)` retrieves log events with optional filtering by source and level, adds timestamp_str, writes CSV with event fields (timestamp, source, level, message, ip_address, user, service, action, metadata), logs count.
- **CSV Format** - Uses Python csv.DictWriter for proper escaping and quoting, writes header row with field names, preserves all database columns, adds human-readable timestamp as extra column.

All export methods handle empty result sets with warnings and skip file creation if no data found.

*json_exporter*:
Exports data to JSON format with structured metadata envelopes. Core functionality:

- **Initialization** - Takes SQLiteStore and optional timestamp_format (default: "%Y-%m-%d %H:%M:%S") for timestamp conversion.
- **System Metrics Export** - `export_system_metrics(output_path, start_time, end_time)` retrieves metrics, adds timestamp_str to each record, writes JSON with envelope structure: `{"type": "system_metrics", "start_time": int, "end_time": int, "count": int, "data": [metrics]}`, uses 2-space indentation for readability.
- **Network Metrics Export** - `export_network_metrics(output_path, start_time, end_time)` same pattern with `"type": "network_metrics"` and network data array.
- **Log Events Export** - `export_log_events(output_path, start_time, end_time, source, level)` retrieves filtered events, adds timestamp_str, writes JSON with envelope: `{"type": "log_events", "start_time": int, "end_time": int, "filters": {"source": str|null, "level": str|null}, "count": int, "data": [events]}`, preserves filter parameters in metadata.
- **JSON Format** - Uses Python json.dump with indent=2 for pretty-printing, includes metadata envelope for context, preserves all database fields, maintains Unix timestamps alongside readable strings for programmatic processing.

All export methods log operations and handle empty result sets gracefully.

*report_generator*:
Generates human-readable text summary reports with formatted statistics. Core functionality:

- **Initialization** - Takes SQLiteStore for data access.
- **Summary Report Generation** - `generate_summary_report(output_path, start_time, end_time)` computes statistics via `_compute_statistics()`, formats multi-section text report with headers and separators, writes to file. Report structure:
  - **Header Section** - 70-character border, title "LOGLY SUMMARY REPORT", report period with formatted dates, duration in hours.
  - **System Metrics Section** - Average and maximum CPU/memory/disk usage percentages, formatted to 1 decimal place.
  - **Network Metrics Section** - Total bytes sent/received with human-readable formatting (B/KB/MB/GB/TB), total packets sent/received with thousand separators.
  - **Log Events Section** - Total event count, failed login count, banned IP count, error count, warning count, all with thousand separators.
  - **Database Statistics Section** - Record counts for each table (system_metrics, network_metrics, log_events, hourly_aggregates, daily_aggregates), database size in MB with 2 decimal places.
  - **Footer** - 70-character border for visual closure.
- **Statistics Computation** - `_compute_statistics(start_time, end_time)` private method calculates:
  - **System Stats** - Extracts cpu_percent, memory_percent, disk_percent from all metrics, calculates averages (sum/count), finds maximums.
  - **Network Stats** - Gets first and last metrics in time range, calculates deltas (last - first) for bytes_sent, bytes_recv, packets_sent, packets_recv to show total transfer during period.
  - **Log Stats** - Counts total events, filters by action ("failed_login", "ban") and level ("ERROR", "WARNING") using list comprehensions.
  - Returns dict with "system", "network", "logs" keys containing computed statistics or None if no data.
- **Byte Formatting** - `_format_bytes(bytes_value)` private helper converts bytes to human-readable format, automatically selects appropriate unit (B/KB/MB/GB/TB) by dividing by 1024, formats integers for bytes and 2 decimals for larger units.

Report uses fixed-width formatting with consistent column alignment for readability. Handles missing data by displaying "No [type] found" messages.
