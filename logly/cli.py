"""
Command-line interface for Logly
"""

import argparse
import logging
import sys
import time
import signal

from logly.core.config import Config
from logly.core.scheduler import Scheduler
from logly.storage.sqlite_store import SQLiteStore
from logly.exporters.csv_exporter import CSVExporter
from logly.exporters.json_exporter import JSONExporter
from logly.exporters.report_generator import ReportGenerator
from logly.query import IssueDetector, AnalysisEngine, QueryBuilder
import json


def setup_logging(config: Config):
    """Setup logging configuration"""
    from logly.utils.logger import initialize_logging

    # Initialize daily rotating logger (uses hardcoded path: logly/logs/)
    initialize_logging()

    # Also setup console logging for CLI output
    log_config = config.get_logging_config()
    level = getattr(logging, log_config.get('level', 'INFO'))
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Add console handler to root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)


def cmd_start(args):
    """Start the Logly daemon"""
    config = Config(args.config)
    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info("Starting Logly daemon")

    # Initialize storage
    db_config = config.get_database_config()
    store = SQLiteStore(db_config['path'])

    # Initialize scheduler
    scheduler = Scheduler(config, store)

    # Setup signal handlers for graceful shutdown
    # Note: This only works in the main thread
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        scheduler.stop()
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except ValueError:
        # Signal handlers can only be set in the main thread
        # This is expected when running in test mode or background threads
        logger.debug("Could not set signal handlers (not in main thread)")

    # Start scheduler
    scheduler.start()

    logger.info("Logly daemon started. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping Logly daemon...")
        scheduler.stop()
        raise


def cmd_collect(args):
    """Run collection once (for testing)"""
    config = Config(args.config)
    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info("Running one-time collection")

    # Initialize storage
    db_config = config.get_database_config()
    store = SQLiteStore(db_config['path'])

    # Initialize scheduler and run once
    scheduler = Scheduler(config, store)
    scheduler.run_once()

    logger.info("Collection complete")


def cmd_status(args):
    """Show database statistics"""
    config = Config(args.config)

    # Initialize storage
    db_config = config.get_database_config()
    store = SQLiteStore(db_config['path'])

    stats = store.get_stats()

    print("\n" + "=" * 60)
    print("LOGLY STATUS")
    print("=" * 60)
    print(f"Database Path:           {db_config['path']}")
    print(f"Database Size:           {stats['database_size_mb']} MB")
    print(f"System Metrics:          {stats['system_metrics']:,} records")
    print(f"Network Metrics:         {stats['network_metrics']:,} records")
    print(f"Log Events:              {stats['log_events']:,} records")
    print(f"Hourly Aggregates:       {stats['hourly_aggregates']:,} records")
    print(f"Daily Aggregates:        {stats['daily_aggregates']:,} records")
    print("=" * 60 + "\n")


def cmd_db_size(args):
    """Show database size information"""
    from logly.utils.db_size import get_db_info

    config = Config(args.config)
    db_config = config.get_database_config()
    db_path = db_config['path']

    db_info = get_db_info(db_path)

    print("\n" + "=" * 60)
    print("DATABASE SIZE REPORT")
    print("=" * 60)
    print(f"Database Path:     {db_info['path']}")
    print(f"Exists:            {'Yes' if db_info['exists'] else 'No'}")

    if db_info['exists']:
        print(f"Size (bytes):      {db_info['size_bytes']:,}")
        print(f"Size (MB):         {db_info['size_mb']}")
        print(f"Size (GB):         {db_info['size_gb']}")
        print(f"Formatted:         {db_info['formatted_size']}")
    else:
        print("Database file does not exist yet.")

    print("=" * 60 + "\n")


def cmd_export(args):
    """Export data to CSV or JSON"""
    config = Config(args.config)
    setup_logging(config)

    logger = logging.getLogger(__name__)

    # Parse time range
    if args.hours:
        end_time = int(time.time())
        start_time = end_time - (args.hours * 3600)
    elif args.days:
        end_time = int(time.time())
        start_time = end_time - (args.days * 86400)
    else:
        # Default to last 24 hours
        end_time = int(time.time())
        start_time = end_time - 86400

    # Initialize storage
    db_config = config.get_database_config()
    store = SQLiteStore(db_config['path'])

    # Determine format
    fmt = args.format or config.get('export.default_format', 'csv')
    timestamp_format = config.get('export.timestamp_format', '%Y-%m-%d %H:%M:%S')

    # Create exporter
    if fmt == 'csv':
        exporter = CSVExporter(store, timestamp_format)
    elif fmt == 'json':
        exporter = JSONExporter(store, timestamp_format)
    else:
        logger.error(f"Unknown format: {fmt}")
        return

    # Export requested data type
    if args.type == 'system':
        exporter.export_system_metrics(args.output, start_time, end_time)
    elif args.type == 'network':
        exporter.export_network_metrics(args.output, start_time, end_time)
    elif args.type == 'logs':
        exporter.export_log_events(args.output, start_time, end_time,
                                   source=args.source, level=args.level)

    logger.info(f"Data exported to {args.output}")


def cmd_report(args):
    """Generate a summary report"""
    config = Config(args.config)
    setup_logging(config)

    logger = logging.getLogger(__name__)

    # Parse time range
    if args.hours:
        end_time = int(time.time())
        start_time = end_time - (args.hours * 3600)
    elif args.days:
        end_time = int(time.time())
        start_time = end_time - (args.days * 86400)
    else:
        # Default to last 24 hours
        end_time = int(time.time())
        start_time = end_time - 86400

    # Initialize storage
    db_config = config.get_database_config()
    store = SQLiteStore(db_config['path'])

    # Generate report
    generator = ReportGenerator(store)
    generator.generate_summary_report(args.output, start_time, end_time)

    logger.info(f"Report generated at {args.output}")

    # Print to console if requested
    if args.print:
        with open(args.output, 'r') as f:
            print(f.read())


def cmd_query(args):
    """Query for issues and problems"""
    config = Config(args.config)
    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info(f"Running query: {args.query_type}")

    # Initialize storage and query components
    db_config = config.get_database_config()
    store = SQLiteStore(db_config['path'])

    query_config = config.get('query', {})
    thresholds = query_config.get('thresholds', {})

    detector = IssueDetector(store, thresholds)
    engine = AnalysisEngine(store, query_config)
    query_builder = QueryBuilder(store)

    # Determine time window
    hours = args.hours or query_config.get('default_time_window', 24)
    logger.info(f"Time window: {hours} hours")

    # Execute the appropriate query subcommand
    if args.query_type == 'security':
        _query_security(detector, engine, query_builder, hours, args, logger)
    elif args.query_type == 'performance':
        _query_performance(detector, query_builder, hours, args, logger)
    elif args.query_type == 'errors':
        _query_errors(detector, engine, query_builder, hours, args, logger)
    elif args.query_type == 'health':
        _query_health(engine, query_builder, hours, args, logger)
    elif args.query_type == 'ips':
        _query_ips(detector, query_builder, args, logger)

    logger.info(f"Query completed: {args.query_type}")


def _query_security(detector, engine, query_builder, hours, args, logger):
    """Query security issues"""
    logger.info("Analyzing security posture")
    print("\n" + "=" * 70)
    print("SECURITY ANALYSIS")
    print("=" * 70)

    # Get security report using engine
    report = engine.analyze_security_posture(hours=hours)

    # Also get threat summary using QueryBuilder convenience method
    threat_summary = query_builder.threat_summary(hours=hours)
    logger.debug(f"Found {threat_summary['high_threat_ips']} high-threat IPs")

    print(f"Time Window:           {hours} hours")
    print(f"Security Posture:      {report.security_posture.upper()}")
    print(f"Risk Score:            {report.risk_score}/100")
    print(f"\nThreats Detected:      {report.total_threats}")
    print(f"High-Threat IPs:       {report.high_threat_ips}")
    print(f"Failed Login Attempts: {report.failed_login_attempts}")
    print(f"Successful Bans:       {report.successful_bans}")

    if report.top_threat_ips:
        print(f"\nTop {len(report.top_threat_ips)} Threat IPs:")
        print("-" * 70)
        for ip_info in report.top_threat_ips:
            print(f"  {ip_info['ip_address']:<16} Threat: {ip_info['threat_score']:>3}/100  "
                  f"Failed Logins: {ip_info['failed_logins']:>3}  Bans: {ip_info['bans']}")

    if report.recommendations:
        print("\nRecommendations:")
        print("-" * 70)
        for rec in report.recommendations:
            print(f"  • {rec}")

    print("=" * 70 + "\n")

    # Export to JSON if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Security report exported to {args.output}")
        print(f"Security report exported to {args.output}\n")


def _query_performance(detector, query_builder, hours, args, logger):
    """Query performance issues"""
    logger.info("Analyzing performance issues")
    print("\n" + "=" * 70)
    print("PERFORMANCE ISSUES")
    print("=" * 70)
    print(f"Time Window: {hours} hours\n")

    # Find all performance issues
    cpu_issues = detector.find_high_cpu_periods(hours=hours)
    memory_issues = detector.find_high_memory_periods(hours=hours)
    disk_issues = detector.find_disk_space_issues(hours=1)

    # Also get system metrics using QueryBuilder
    avg_cpu = query_builder.metrics().system().in_last_hours(hours).avg('cpu_percent')
    avg_memory = query_builder.metrics().system().in_last_hours(hours).avg('memory_percent')
    logger.debug(f"Avg CPU: {avg_cpu:.2f}%, Avg Memory: {avg_memory:.2f}%")

    all_issues = cpu_issues + memory_issues + disk_issues

    if not all_issues:
        print("No performance issues detected.")
    else:
        print(f"Found {len(all_issues)} performance issue(s):\n")
        for i, issue in enumerate(all_issues, 1):
            print(f"{i}. [{issue.severity_level.name}] {issue.title}")
            print(f"   {issue.description}")
            if issue.recommendations:
                print(f"   Recommendation: {issue.recommendations[0]}")
            print()

    print("=" * 70 + "\n")

    # Export to JSON if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump([issue.to_dict() for issue in all_issues], f, indent=2)
        logger.info(f"Performance issues exported to {args.output}")
        print(f"Performance issues exported to {args.output}\n")


def _query_errors(detector, engine, query_builder, hours, args, logger):
    """Query error issues"""
    logger.info("Analyzing error trends")
    print("\n" + "=" * 70)
    print("ERROR ANALYSIS")
    print("=" * 70)

    # Get error trend report
    days = hours // 24 if hours >= 24 else 1
    report = engine.analyze_error_trends(days=days)

    # Also use QueryBuilder to get recent errors
    recent_errors = query_builder.recent_errors(hours=hours)
    logger.debug(f"Found {len(recent_errors)} recent error events")

    print(f"Time Period:       {days} days")
    print(f"Total Errors:      {report.total_errors}")
    print(f"Error Rate:        {report.error_rate:.2f} errors/hour")
    print(f"Unique Types:      {report.unique_error_types}")
    print(f"Trend:             {report.trend.upper()}")

    if report.errors_by_source:
        print("\nTop Error Sources:")
        print("-" * 70)
        sorted_sources = sorted(report.errors_by_source.items(), key=lambda x: x[1], reverse=True)[:5]
        for source, count in sorted_sources:
            print(f"  {source:<30} {count:>5} errors")

    if report.top_errors:
        print("\nTop Issues:")
        print("-" * 70)
        for i, error in enumerate(report.top_errors[:5], 1):
            print(f"  {i}. [{error.severity_level.name}] {error.title}")
            print(f"     {error.description}")

    if report.recommendations:
        print("\nRecommendations:")
        print("-" * 70)
        for rec in report.recommendations:
            print(f"  • {rec}")

    print("=" * 70 + "\n")

    # Export to JSON if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Error analysis exported to {args.output}")
        print(f"Error analysis exported to {args.output}\n")


def _query_health(engine, query_builder, hours, args, logger):
    """Query overall system health"""
    logger.info("Analyzing system health")
    print("\n" + "=" * 70)
    print("SYSTEM HEALTH REPORT")
    print("=" * 70)

    report = engine.analyze_system_health(hours=hours)

    # Get system health snapshot using QueryBuilder
    health_snapshot = query_builder.system_health_snapshot()
    logger.debug(f"Latest system snapshot retrieved at {health_snapshot['timestamp']}")

    print(f"Time Window:       {hours} hours")
    print(f"Health Score:      {report.health_score}/100")
    print(f"Status:            {report.status.upper()}")

    print("\nComponent Scores:")
    print("-" * 70)
    print(f"  Security:        {report.security_score}/100")
    print(f"  Performance:     {report.performance_score}/100")
    print(f"  Errors:          {report.error_score}/100")
    print(f"  Network:         {report.network_score}/100")

    print("\nIssues Detected:")
    print("-" * 70)
    print(f"  Critical:        {report.critical_issues}")
    print(f"  High:            {report.high_issues}")
    print(f"  Medium:          {report.medium_issues}")
    print(f"  Low:             {report.low_issues}")
    print(f"  Total:           {report.total_issues}")

    if report.top_issues:
        print("\nTop Issues:")
        print("-" * 70)
        for i, issue in enumerate(report.top_issues, 1):
            print(f"  {i}. [{issue.severity_level.name}] {issue.title}")
            print(f"     {issue.description}")

    if report.recommendations:
        print("\nRecommendations:")
        print("-" * 70)
        for rec in report.recommendations:
            print(f"  • {rec}")

    print("=" * 70 + "\n")

    # Export to JSON if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Health report exported to {args.output}")
        print(f"Health report exported to {args.output}\n")


def _query_ips(detector, query_builder, args, logger):
    """Query IP threats"""
    logger.info("Analyzing IP threats")
    print("\n" + "=" * 70)
    print("IP THREAT ANALYSIS")
    print("=" * 70)

    # Get threat threshold
    threshold = args.threshold or 70

    # Find suspicious IPs using detector
    threats = detector.find_suspicious_ips(threat_threshold=threshold)

    # Also use QueryBuilder for IP queries
    high_threat_ips = query_builder.ips().high_threat().sort_by_threat()
    logger.debug(f"Found {len(high_threat_ips)} high-threat IPs via QueryBuilder")

    print(f"Threat Threshold:  {threshold}/100")
    print(f"High-Threat IPs:   {len(threats)}\n")

    if not threats:
        print("No high-threat IPs detected.")
    else:
        print("High-Threat IPs:")
        print("-" * 70)
        for threat in threats[:20]:  # Limit to top 20
            print(f"  {threat.ip_address:<16} "
                  f"Threat: {threat.threat_score:>3}/100  "
                  f"Failed Logins: {threat.failed_login_count:>3}  "
                  f"Bans: {threat.ban_count}")

    print("=" * 70 + "\n")

    # Export to JSON if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump([threat.to_dict() for threat in threats], f, indent=2)
        logger.info(f"IP threats exported to {args.output}")
        print(f"IP threats exported to {args.output}\n")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Logly - Log aggregation and system monitoring for AWS EC2',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '-c', '--config',
        help='Path to configuration file',
        default=None
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Start command
    start_parser = subparsers.add_parser('start', help='Start Logly daemon')
    start_parser.set_defaults(func=cmd_start)

    # Collect command (one-time collection)
    collect_parser = subparsers.add_parser('collect', help='Run collection once')
    collect_parser.set_defaults(func=cmd_collect)

    # Status command
    status_parser = subparsers.add_parser('status', help='Show database status')
    status_parser.set_defaults(func=cmd_status)

    # DB Size command
    db_size_parser = subparsers.add_parser('db-size', help='Show database size information')
    db_size_parser.set_defaults(func=cmd_db_size)

    # Export command
    export_parser = subparsers.add_parser('export', help='Export data')
    export_parser.add_argument('type', choices=['system', 'network', 'logs'],
                              help='Data type to export')
    export_parser.add_argument('output', help='Output file path')
    export_parser.add_argument('-f', '--format', choices=['csv', 'json'],
                              help='Export format')
    export_parser.add_argument('--hours', type=int,
                              help='Export data from last N hours')
    export_parser.add_argument('--days', type=int,
                              help='Export data from last N days')
    export_parser.add_argument('--source',
                              help='Filter logs by source')
    export_parser.add_argument('--level',
                              help='Filter logs by level')
    export_parser.set_defaults(func=cmd_export)

    # Report command
    report_parser = subparsers.add_parser('report', help='Generate summary report')
    report_parser.add_argument('output', help='Output file path')
    report_parser.add_argument('--hours', type=int,
                              help='Report on last N hours')
    report_parser.add_argument('--days', type=int,
                              help='Report on last N days')
    report_parser.add_argument('-p', '--print', action='store_true',
                              help='Print report to console')
    report_parser.set_defaults(func=cmd_report)

    # Query command
    query_parser = subparsers.add_parser('query', help='Query for issues and problems')
    query_parser.add_argument('query_type',
                             choices=['security', 'performance', 'errors', 'health', 'ips'],
                             help='Type of query to run')
    query_parser.add_argument('--hours', type=int,
                             help='Time window in hours (default: from config)')
    query_parser.add_argument('--threshold', type=int,
                             help='Threat score threshold for IP queries')
    query_parser.add_argument('-o', '--output',
                             help='Export results to JSON file')
    query_parser.set_defaults(func=cmd_query)

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    try:
        args.func(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
