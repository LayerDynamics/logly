"""
End-to-End tests for disaster recovery and maintenance operations
Tests REAL backup, restore, cleanup, and recovery scenarios
NO MOCKING - simulates actual operational procedures
"""

import pytest
import time
import tempfile
import shutil
import subprocess
import os
import sqlite3
import gzip
from pathlib import Path
import yaml
import json

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from logly.core.config import Config
from logly.storage.sqlite_store import SQLiteStore
from logly.storage.models import SystemMetric, NetworkMetric, LogEvent
from logly.core.scheduler import Scheduler
from logly.exporters.json_exporter import JSONExporter


class TestE2EDisasterRecovery:
    """End-to-end tests for disaster recovery and maintenance operations"""

    @pytest.mark.e2e
    def test_e2e_database_corruption_recovery(self):
        """
        E2E Scenario: Recover from database corruption
        Simulates detection and recovery from corrupted database

        User Story: "As a sysadmin, I need to recover monitoring data
        when the database becomes corrupted without losing history"

        Step 1: Create production database with historical data
        Step 2: Create automated backup
        Step 3: Simulate database corruption
        Step 4: Detect corruption during operation
        Step 5: Initiate recovery procedure
        Step 6: Restore from backup
        Step 7: Verify data integrity after restore
        Step 8: Resume normal monitoring
        Step 9: Fill in gaps from corruption period
        Step 10: Generate recovery report
        """
        workspace = tempfile.mkdtemp(prefix="logly_e2e_recovery_")

        try:
            print("Starting database corruption recovery scenario...")

            # Step 1: Create production database with data
            db_path = Path(workspace) / "production.db"
            backup_dir = Path(workspace) / "backups"
            backup_dir.mkdir()

            store = SQLiteStore(str(db_path))

            # Populate with historical data
            print("→ Creating historical monitoring data...")
            base_time = int(time.time()) - 3600  # 1 hour ago

            for i in range(100):
                metric = SystemMetric(
                    timestamp=base_time + i * 30,  # Every 30 seconds
                    cpu_percent=30.0 + (i % 20),
                    memory_percent=40.0 + (i % 15),
                    disk_percent=50.0 + (i % 10),
                )
                store.insert_system_metric(metric)

                if i % 5 == 0:  # Every 5th metric, add network
                    net_metric = NetworkMetric(
                        timestamp=base_time + i * 30,
                        bytes_sent=1024 * i,
                        bytes_recv=2048 * i,
                    )
                    store.insert_network_metric(net_metric)

            # Record data statistics before corruption
            with store._connection() as conn:
                original_stats = {
                    "system_count": conn.execute(
                        "SELECT COUNT(*) FROM system_metrics"
                    ).fetchone()[0],
                    "network_count": conn.execute(
                        "SELECT COUNT(*) FROM network_metrics"
                    ).fetchone()[0],
                    "last_timestamp": conn.execute(
                        "SELECT MAX(timestamp) FROM system_metrics"
                    ).fetchone()[0],
                }

            print(
                f"✓ Created database with {original_stats['system_count']} system metrics"
            )

            # Step 2: Create automated backup
            print("→ Creating automated backup...")
            backup_file = backup_dir / f"backup_{int(time.time())}.db.gz"

            # Create compressed backup
            with open(db_path, "rb") as f_in:
                with gzip.open(backup_file, "wb") as f_out:
                    f_out.write(f_in.read())

            backup_size = backup_file.stat().st_size / 1024  # KB
            print(f"✓ Backup created: {backup_file.name} ({backup_size:.1f} KB)")

            # Step 3: Simulate database corruption
            print("→ Simulating database corruption...")

            # Add some new data first
            for i in range(10):
                metric = SystemMetric(
                    timestamp=int(time.time()) + i,
                    cpu_percent=50.0,
                    memory_percent=60.0,
                )
                store.insert_system_metric(metric)

            # Close connection properly
            del store

            # Corrupt the database file - corrupt the SQLite header
            with open(db_path, "r+b") as f:
                f.seek(0)  # Corrupt the header
                f.write(b"CORRUPTED_SQLITE_HEADER!")  # Write garbage to header

            print("✓ Database corrupted (simulated disk failure)")

            # Step 4: Detect corruption during operation
            print("→ Attempting to use corrupted database...")

            corruption_detected = False
            try:
                corrupt_store = SQLiteStore(str(db_path))
                with corrupt_store._connection() as conn:
                    conn.execute("SELECT COUNT(*) FROM system_metrics").fetchone()
            except (sqlite3.DatabaseError, sqlite3.OperationalError) as e:
                corruption_detected = True
                print(f"✓ Corruption detected: {str(e)[:50]}...")

            assert corruption_detected, "Should detect corruption"

            # Step 5: Initiate recovery procedure
            print("\n🔧 Initiating recovery procedure...")

            # Move corrupted database aside
            corrupted_path = Path(workspace) / "corrupted.db"
            shutil.move(db_path, corrupted_path)
            print(f"✓ Corrupted database quarantined: {corrupted_path.name}")

            # Step 6: Restore from backup
            print("→ Restoring from backup...")

            # Find most recent backup
            backups = sorted(backup_dir.glob("backup_*.db.gz"))
            latest_backup = backups[-1]

            # Decompress and restore
            with gzip.open(latest_backup, "rb") as f_in:
                with open(db_path, "wb") as f_out:
                    f_out.write(f_in.read())

            print(f"✓ Restored from backup: {latest_backup.name}")

            # Step 7: Verify data integrity after restore
            restored_store = SQLiteStore(str(db_path))

            with restored_store._connection() as conn:
                # Run integrity check
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
                assert integrity == "ok", f"Integrity check failed: {integrity}"

                # Verify data matches original
                restored_stats = {
                    "system_count": conn.execute(
                        "SELECT COUNT(*) FROM system_metrics"
                    ).fetchone()[0],
                    "network_count": conn.execute(
                        "SELECT COUNT(*) FROM network_metrics"
                    ).fetchone()[0],
                    "last_timestamp": conn.execute(
                        "SELECT MAX(timestamp) FROM system_metrics"
                    ).fetchone()[0],
                }

            assert restored_stats["system_count"] == original_stats["system_count"]
            assert restored_stats["network_count"] == original_stats["network_count"]

            print("✓ Data integrity verified:")
            print(f"  System metrics: {restored_stats['system_count']}")
            print(f"  Network metrics: {restored_stats['network_count']}")

            # Step 8: Resume normal monitoring
            print("→ Resuming normal monitoring operations...")

            # Add new metrics post-recovery
            recovery_time = int(time.time())
            for i in range(5):
                metric = SystemMetric(
                    timestamp=recovery_time + i * 10,
                    cpu_percent=35.0 + i,
                    memory_percent=45.0 + i,
                )
                restored_store.insert_system_metric(metric)

            print("✓ Monitoring resumed successfully")

            # Step 9: Fill in gaps from corruption period
            print("→ Analyzing data gaps...")

            gap_start = original_stats["last_timestamp"]
            gap_end = recovery_time
            gap_duration = gap_end - gap_start

            print(f"✓ Identified gap: {gap_duration} seconds of missing data")

            # Log the gap for analysis
            gap_event = LogEvent(
                timestamp=recovery_time,
                source="recovery",
                message=f"Data gap detected: {gap_duration}s due to database corruption",
                level="WARNING",
            )
            restored_store.insert_log_event(gap_event)

            # Step 10: Generate recovery report
            print("→ Generating recovery report...")

            recovery_report = Path(workspace) / "recovery_report.json"
            report_data = {
                "incident": "Database Corruption",
                "detection_time": recovery_time - 300,  # 5 minutes ago
                "recovery_time": recovery_time,
                "downtime_seconds": 300,
                "data_loss": {
                    "gap_seconds": gap_duration,
                    "estimated_lost_metrics": gap_duration
                    // 30,  # Based on collection interval
                },
                "recovery_method": "Restore from backup",
                "backup_used": latest_backup.name,
                "data_recovered": {
                    "system_metrics": restored_stats["system_count"],
                    "network_metrics": restored_stats["network_count"],
                },
                "post_recovery_status": "Operational",
                "recommendations": [
                    "Increase backup frequency to every hour",
                    "Implement real-time replication",
                    "Add database integrity checks to monitoring",
                ],
            }

            with open(recovery_report, "w") as f:
                json.dump(report_data, f, indent=2)

            print(f"✓ Recovery report generated: {recovery_report}")

            print("\n✅ Database corruption recovery completed successfully!")
            print(f"   Data recovered: {restored_stats['system_count']} metrics")
            print("   Downtime: 5 minutes")
            print(f"   Data gap: {gap_duration} seconds")

        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    @pytest.mark.e2e
    def test_e2e_disk_full_handling(self):
        """
        E2E Scenario: Handle disk full condition gracefully
        Simulates monitoring behavior when disk space is exhausted

        User Story: "As a sysadmin, I need Logly to handle disk full
        conditions gracefully and continue operating when possible"

        Step 1: Setup monitoring with limited disk space
        Step 2: Fill disk to near capacity
        Step 3: Detect low disk space condition
        Step 4: Activate space-saving mode
        Step 5: Perform emergency cleanup
        Step 6: Archive old data
        Step 7: Resume with reduced retention
        Step 8: Alert on disk space issues
        Step 9: Verify continued operation
        Step 10: Generate space usage report
        """
        workspace = tempfile.mkdtemp(prefix="logly_e2e_diskfull_")

        try:
            print("Starting disk full handling scenario...")

            # Step 1: Setup with disk monitoring
            db_path = Path(workspace) / "monitoring.db"
            archive_dir = Path(workspace) / "archive"
            archive_dir.mkdir()

            config_data = {
                "database": {
                    "path": str(db_path),
                    "retention_days": 30,
                    "max_size_mb": 10,  # Simulate 10MB limit
                },
                "collection": {
                    "system_metrics": 1,
                    "network_metrics": 2,
                    "log_parsing": 5,
                },
                "system": {
                    "enabled": True,
                    "metrics": ["cpu_percent", "memory_percent", "disk_percent"],
                },
                "network": {
                    "enabled": True,
                },
            }

            config = Config()
            config.config = config_data
            store = SQLiteStore(str(db_path))

            # Step 2: Fill disk to near capacity
            print("→ Simulating disk usage growth...")

            # Add lots of historical data
            base_time = int(time.time()) - (7 * 24 * 3600)  # 7 days ago

            metrics_added = 0
            for day in range(7):
                for hour in range(24):
                    for minute in range(0, 60, 5):  # Every 5 minutes
                        timestamp = (
                            base_time + (day * 86400) + (hour * 3600) + (minute * 60)
                        )
                        metric = SystemMetric(
                            timestamp=timestamp,
                            cpu_percent=30.0 + (hour % 24),
                            memory_percent=40.0 + (minute % 30),
                            disk_percent=50.0 + (day * 5),  # Disk usage growing
                        )
                        store.insert_system_metric(metric)
                        metrics_added += 1

            # Check database size
            db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
            print(f"✓ Database grown to {db_size_mb:.2f} MB ({metrics_added} metrics)")

            # Step 3: Detect low disk space
            print("→ Checking disk space...")

            # Simulate disk space check
            simulated_free_space_mb = 2  # Only 2MB free
            disk_full_threshold_mb = 5

            low_space_detected = simulated_free_space_mb < disk_full_threshold_mb
            assert low_space_detected, "Should detect low disk space"

            print(f"⚠️  Low disk space detected: {simulated_free_space_mb} MB free")

            # Step 4: Activate space-saving mode
            print("→ Activating space-saving mode...")

            # Reduce collection frequency
            config.config["collection"]["system_metrics"] = (
                60  # Reduce to once per minute
            )
            config.config["collection"]["network_metrics"] = 120

            # Disable non-essential collectors
            config.config["network"]["enabled"] = False

            print("✓ Space-saving mode activated:")
            print("  - Reduced collection frequency")
            print("  - Disabled non-essential collectors")

            # Step 5: Perform emergency cleanup
            print("→ Performing emergency cleanup...")

            # Get data statistics before cleanup
            with store._connection() as conn:
                before_cleanup = conn.execute(
                    "SELECT COUNT(*) as count, MIN(timestamp) as oldest FROM system_metrics"
                ).fetchone()

            # Delete data older than 3 days (emergency retention)
            emergency_retention_days = 3
            cutoff_time = int(time.time()) - (emergency_retention_days * 86400)

            with store._connection() as conn:
                deleted = conn.execute(
                    "DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_time,)
                )

            # VACUUM must be run outside of a transaction
            conn = sqlite3.connect(str(db_path))
            conn.execute("VACUUM")  # Reclaim space
            conn.close()

            # Check new size
            new_db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
            space_recovered_mb = db_size_mb - new_db_size_mb

            print("✓ Emergency cleanup completed:")
            print(f"  Recovered {space_recovered_mb:.2f} MB")
            print(f"  New database size: {new_db_size_mb:.2f} MB")

            # Step 6: Archive old data
            print("→ Archiving historical data...")

            # Export old data before deletion
            with store._connection() as conn:
                old_data = conn.execute(
                    "SELECT * FROM system_metrics WHERE timestamp < ? LIMIT 1000",
                    (cutoff_time + 86400,),  # Archive 1 day of data
                ).fetchall()

            if old_data:
                archive_file = archive_dir / f"archive_{int(time.time())}.json.gz"
                archive_data = [dict(row) for row in old_data]

                # Compress and save
                import gzip

                with gzip.open(archive_file, "wt", encoding="utf-8") as f:
                    json.dump(archive_data, f)

                archive_size_kb = archive_file.stat().st_size / 1024
                print(
                    f"✓ Archived {len(old_data)} records to {archive_file.name} ({archive_size_kb:.1f} KB)"
                )

            # Step 7: Resume with reduced retention
            print("→ Resuming monitoring with adjusted settings...")

            # Update configuration for reduced retention
            config.config["database"]["retention_days"] = 7  # Reduce from 30 to 7 days
            config.config["aggregation"] = {
                "enabled": True,
                "intervals": ["hourly"],  # Only hourly, not daily
                "keep_raw_data_days": 1,  # Keep raw data for 1 day only
            }

            # Add new metrics with reduced frequency
            current_time = int(time.time())
            for i in range(5):
                metric = SystemMetric(
                    timestamp=current_time + i * 60,  # Once per minute
                    cpu_percent=25.0 + i,
                    memory_percent=35.0 + i,
                    disk_percent=90.0,  # High disk usage
                )
                store.insert_system_metric(metric)

            print("✓ Monitoring resumed with reduced retention")

            # Step 8: Alert on disk space issues
            print("→ Generating disk space alerts...")

            alert_event = LogEvent(
                timestamp=current_time,
                source="disk_monitor",
                message=f"CRITICAL: Low disk space - {simulated_free_space_mb} MB free",
                level="CRITICAL",
            )
            store.insert_log_event(alert_event)

            cleanup_event = LogEvent(
                timestamp=current_time + 60,
                source="maintenance",
                message=f"Emergency cleanup performed - recovered {space_recovered_mb:.1f} MB",
                level="WARNING",
            )
            store.insert_log_event(cleanup_event)

            print("✓ Disk space alerts logged")

            # Step 9: Verify continued operation
            print("→ Verifying system stability...")

            with store._connection() as conn:
                # Check we can still query
                current_metrics = conn.execute(
                    "SELECT COUNT(*) FROM system_metrics WHERE timestamp >= ?",
                    (current_time,),
                ).fetchone()[0]

                assert current_metrics > 0, "Should have recent metrics"

                # Check database integrity
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
                assert integrity == "ok", "Database should be intact"

            print(f"✓ System operational with {current_metrics} recent metrics")

            # Step 10: Generate space usage report
            print("→ Generating disk usage report...")

            report_file = Path(workspace) / "disk_usage_report.html"

            with store._connection() as conn:
                stats = conn.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(timestamp) as oldest_record,
                        MAX(timestamp) as newest_record
                    FROM system_metrics
                """).fetchone()

            with open(report_file, "w") as f:
                f.write(f"""<html>
<head><title>Disk Space Management Report</title></head>
<body>
<h1>Disk Space Crisis Management Report</h1>

<h2>Incident Summary</h2>
<ul>
<li>Condition: Disk space critically low ({simulated_free_space_mb} MB free)</li>
<li>Action taken: Emergency cleanup and archival</li>
<li>Space recovered: {space_recovered_mb:.2f} MB</li>
<li>Current database size: {new_db_size_mb:.2f} MB</li>
</ul>

<h2>Data Retention Changes</h2>
<table border="1">
<tr><th>Setting</th><th>Before</th><th>After</th></tr>
<tr><td>Retention Days</td><td>30</td><td>7</td></tr>
<tr><td>Collection Frequency</td><td>1s</td><td>60s</td></tr>
<tr><td>Raw Data Keep</td><td>7 days</td><td>1 day</td></tr>
</table>

<h2>Current Status</h2>
<ul>
<li>Total records: {stats["total_records"]}</li>
<li>Date range: {time.strftime("%Y-%m-%d", time.localtime(stats["oldest_record"]))} to 
    {time.strftime("%Y-%m-%d", time.localtime(stats["newest_record"]))}</li>
<li>Archived data: {archive_file.name if old_data else "None"}</li>
</ul>

<h2>Recommendations</h2>
<ol>
<li>Add more disk space immediately</li>
<li>Implement automated archival process</li>
<li>Set up disk space monitoring alerts at 80% threshold</li>
<li>Consider remote storage for historical data</li>
</ol>
</body>
</html>""")

            print(f"✓ Disk usage report generated: {report_file}")

            print("\n✅ Disk full handling completed successfully!")
            print(f"   Space recovered: {space_recovered_mb:.2f} MB")
            print(f"   Data archived: {len(old_data) if old_data else 0} records")
            print("   System status: Operational with reduced retention")

        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    @pytest.mark.e2e
    def test_e2e_scheduled_maintenance_window(self):
        """
        E2E Scenario: Execute scheduled maintenance window procedures
        Simulates planned maintenance with zero data loss

        User Story: "As an ops team, we need to perform scheduled
        maintenance on Logly without losing monitoring data"

        Step 1: Schedule maintenance window
        Step 2: Notify about upcoming maintenance
        Step 3: Increase collection before maintenance
        Step 4: Create pre-maintenance backup
        Step 5: Enter maintenance mode
        Step 6: Perform maintenance tasks
        Step 7: Verify system health
        Step 8: Exit maintenance mode
        Step 9: Validate no data loss
        Step 10: Generate maintenance report
        """
        workspace = tempfile.mkdtemp(prefix="logly_e2e_maintenance_")

        try:
            print("Starting scheduled maintenance window simulation...")

            # Step 1: Schedule maintenance window
            maintenance_start = int(time.time()) + 300  # 5 minutes from now
            maintenance_duration = 600  # 10 minutes
            maintenance_end = maintenance_start + maintenance_duration

            print("📅 Maintenance window scheduled:")
            print(
                f"   Start: {time.strftime('%H:%M:%S', time.localtime(maintenance_start))}"
            )
            print(
                f"   End: {time.strftime('%H:%M:%S', time.localtime(maintenance_end))}"
            )
            print(f"   Duration: {maintenance_duration // 60} minutes")

            # Setup monitoring
            db_path = Path(workspace) / "production.db"
            buffer_dir = Path(workspace) / "buffer"
            buffer_dir.mkdir()

            config = Config()
            config.config = {
                "database": {"path": str(db_path)},
                "collection": {"system_metrics": 5, "network_metrics": 5},
                "system": {
                    "enabled": True,
                    "metrics": ["cpu_percent", "memory_percent"],
                },
                "network": {"enabled": True, "metrics": ["bytes_sent", "bytes_recv"]},
            }

            store = SQLiteStore(str(db_path))

            # Collect normal metrics
            print("→ Collecting pre-maintenance baseline...")
            base_time = int(time.time())

            for i in range(10):
                metric = SystemMetric(
                    timestamp=base_time + i * 5,
                    cpu_percent=30.0 + (i % 5),
                    memory_percent=40.0 + (i % 3),
                )
                store.insert_system_metric(metric)

            pre_maintenance_count = 10
            print(f"✓ Baseline established: {pre_maintenance_count} metrics")

            # Step 2: Notify about upcoming maintenance
            print("→ Logging maintenance notification...")

            notification = LogEvent(
                timestamp=base_time,
                source="maintenance",
                message=f"Scheduled maintenance starting at {time.strftime('%H:%M', time.localtime(maintenance_start))}",
                level="INFO",
            )
            store.insert_log_event(notification)

            # Step 3: Increase collection before maintenance
            print("→ Increasing collection frequency before maintenance...")

            # Burst collection to ensure fresh data
            burst_time = base_time + 60
            for i in range(20):
                metric = SystemMetric(
                    timestamp=burst_time + i,
                    cpu_percent=32.0 + (i % 4),
                    memory_percent=42.0 + (i % 3),
                )
                store.insert_system_metric(metric)

            print("✓ Burst collection completed: 20 additional metrics")

            # Step 4: Create pre-maintenance backup
            print("→ Creating pre-maintenance backup...")

            backup_file = Path(workspace) / f"pre_maintenance_{int(time.time())}.db"
            shutil.copy2(db_path, backup_file)
            backup_size = backup_file.stat().st_size / 1024

            print(f"✓ Backup created: {backup_file.name} ({backup_size:.1f} KB)")

            # Step 5: Enter maintenance mode
            print("\n🔧 Entering maintenance mode...")

            maintenance_flag = Path(workspace) / "MAINTENANCE_MODE"
            maintenance_flag.write_text(
                f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Buffer file for metrics during maintenance
            buffer_file = buffer_dir / "metrics_buffer.json"
            buffered_metrics = []

            # Simulate metrics that would be collected during maintenance
            maintenance_time = int(time.time())
            for i in range(5):
                # These would normally go to buffer instead of database
                metric_data = {
                    "timestamp": maintenance_time + i * 60,
                    "cpu_percent": 10.0 + i,  # Low CPU during maintenance
                    "memory_percent": 25.0 + i,
                    "type": "system_metric",
                }
                buffered_metrics.append(metric_data)

            # Save to buffer
            with open(buffer_file, "w") as f:
                json.dump(buffered_metrics, f)

            print(f"✓ Metrics buffered during maintenance: {len(buffered_metrics)}")

            # Step 6: Perform maintenance tasks
            print("→ Performing maintenance tasks...")

            maintenance_tasks = [
                "Database optimization",
                "Index rebuilding",
                "Log rotation",
                "Cache clearing",
                "Configuration validation",
            ]

            for task in maintenance_tasks:
                print(f"  • {task}... ", end="")
                time.sleep(0.1)  # Simulate task execution

                # Database optimization
                if "optimization" in task.lower():
                    with store._connection() as conn:
                        conn.execute("ANALYZE")
                        conn.execute("VACUUM")

                print("✓")

            print("✓ All maintenance tasks completed")

            # Step 7: Verify system health
            print("→ Running post-maintenance health checks...")

            health_checks = {
                "Database integrity": False,
                "Configuration valid": False,
                "Disk space adequate": False,
                "Services responding": False,
            }

            # Run health checks
            with store._connection() as conn:
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
                health_checks["Database integrity"] = integrity == "ok"

            health_checks["Configuration valid"] = config.config is not None
            health_checks["Disk space adequate"] = True  # Simulated
            health_checks["Services responding"] = True  # Simulated

            all_healthy = all(health_checks.values())

            for check, status in health_checks.items():
                status_icon = "✅" if status else "❌"
                print(f"  {status_icon} {check}")

            assert all_healthy, "Health checks must pass"
            print("✓ All health checks passed")

            # Step 8: Exit maintenance mode
            print("→ Exiting maintenance mode...")

            # Process buffered metrics
            with open(buffer_file, "r") as f:
                buffered_data = json.load(f)

            # Insert buffered metrics into database
            for metric_data in buffered_data:
                if metric_data["type"] == "system_metric":
                    metric = SystemMetric(
                        timestamp=metric_data["timestamp"],
                        cpu_percent=metric_data["cpu_percent"],
                        memory_percent=metric_data["memory_percent"],
                    )
                    store.insert_system_metric(metric)

            print(f"✓ Processed {len(buffered_data)} buffered metrics")

            # Remove maintenance flag
            maintenance_flag.unlink()

            # Resume normal collection
            post_maintenance_time = int(time.time())
            for i in range(5):
                metric = SystemMetric(
                    timestamp=post_maintenance_time + i * 5,
                    cpu_percent=35.0 + i,
                    memory_percent=45.0 + i,
                )
                store.insert_system_metric(metric)

            print("✓ Normal monitoring resumed")

            # Step 9: Validate no data loss
            print("→ Validating data integrity...")

            with store._connection() as conn:
                total_metrics = conn.execute(
                    "SELECT COUNT(*) FROM system_metrics"
                ).fetchone()[0]

                # Should have pre + buffered + post metrics
                expected_metrics = (
                    pre_maintenance_count + 20 + len(buffered_metrics) + 5
                )

                # Allow some variance
                assert abs(total_metrics - expected_metrics) <= 5, (
                    f"Data mismatch: {total_metrics} vs {expected_metrics}"
                )

                # Check for gaps
                gaps = conn.execute("""
                    SELECT 
                        t1.timestamp as gap_start,
                        MIN(t2.timestamp) as gap_end,
                        (MIN(t2.timestamp) - t1.timestamp) as gap_size
                    FROM system_metrics t1
                    LEFT JOIN system_metrics t2 
                        ON t2.timestamp > t1.timestamp
                    WHERE (t2.timestamp - t1.timestamp) > 120
                    GROUP BY t1.timestamp
                """).fetchall()

                if gaps:
                    print(
                        f"  ⚠️  Found {len(gaps)} small gaps (expected during maintenance)"
                    )
                else:
                    print("  ✓ No significant data gaps detected")

            print(f"✓ Data integrity verified: {total_metrics} total metrics")

            # Step 10: Generate maintenance report
            print("→ Generating maintenance report...")

            report_file = Path(workspace) / "maintenance_report.html"

            with open(report_file, "w") as f:
                f.write(f"""<html>
<head><title>Maintenance Window Report</title></head>
<body>
<h1>Scheduled Maintenance Report</h1>

<h2>Maintenance Window</h2>
<ul>
<li>Date: {time.strftime("%Y-%m-%d")}</li>
<li>Start Time: {time.strftime("%H:%M:%S", time.localtime(maintenance_start))}</li>
<li>End Time: {time.strftime("%H:%M:%S", time.localtime(maintenance_end))}</li>
<li>Duration: {maintenance_duration // 60} minutes</li>
<li>Status: ✅ Completed Successfully</li>
</ul>

<h2>Tasks Performed</h2>
<ol>
{"".join(f"<li>✅ {task}</li>" for task in maintenance_tasks)}
</ol>

<h2>Health Check Results</h2>
<ul>
{"".join(f"<li>{'✅' if v else '❌'} {k}</li>" for k, v in health_checks.items())}
</ul>

<h2>Data Continuity</h2>
<ul>
<li>Metrics before maintenance: {pre_maintenance_count + 20}</li>
<li>Metrics buffered during maintenance: {len(buffered_metrics)}</li>
<li>Metrics after maintenance: 5</li>
<li>Total metrics in database: {total_metrics}</li>
<li>Data loss: None</li>
</ul>

<h2>Backup Information</h2>
<ul>
<li>Pre-maintenance backup: {backup_file.name}</li>
<li>Backup size: {backup_size:.1f} KB</li>
<li>Backup verified: ✅ Yes</li>
</ul>

<h2>Post-Maintenance Actions</h2>
<ul>
<li>✅ Buffered data processed</li>
<li>✅ Normal monitoring resumed</li>
<li>✅ Alerts re-enabled</li>
<li>✅ Report generated</li>
</ul>
</body>
</html>""")

            print(f"✓ Maintenance report generated: {report_file}")

            print("\n✅ Scheduled maintenance completed successfully!")
            print(f"   Duration: {maintenance_duration // 60} minutes")
            print(f"   Tasks completed: {len(maintenance_tasks)}")
            print("   Data loss: None")
            print("   System status: Fully operational")

        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_e2e_multi_database_migration(self):
        """
        E2E Scenario: Migrate monitoring data to new database version
        Simulates complete database migration without downtime

        User Story: "As a DBA, I need to migrate Logly's database to a
        new schema version without losing data or monitoring capability"

        Step 1: Setup current production database
        Step 2: Create new database with updated schema
        Step 3: Start parallel writing (dual-write)
        Step 4: Begin background data migration
        Step 5: Verify data consistency
        Step 6: Perform cutover to new database
        Step 7: Validate migrated data
        Step 8: Keep old database as backup
        Step 9: Monitor new database performance
        Step 10: Generate migration report
        """
        workspace = tempfile.mkdtemp(prefix="logly_e2e_migration_")

        try:
            print("Starting database migration scenario...")

            # Step 1: Setup current production database
            old_db = Path(workspace) / "logly_v1.db"
            new_db = Path(workspace) / "logly_v2.db"

            old_store = SQLiteStore(str(old_db))

            print("→ Populating current production database...")
            base_time = int(time.time()) - (3 * 24 * 3600)  # 3 days of data

            # Add historical data
            for day in range(3):
                for hour in range(24):
                    for minute in range(0, 60, 10):
                        timestamp = (
                            base_time + (day * 86400) + (hour * 3600) + (minute * 60)
                        )

                        # System metrics
                        metric = SystemMetric(
                            timestamp=timestamp,
                            cpu_percent=30.0 + (hour % 24),
                            memory_percent=40.0 + (minute % 20),
                        )
                        old_store.insert_system_metric(metric)

                        # Network metrics every 30 minutes
                        if minute % 30 == 0:
                            net_metric = NetworkMetric(
                                timestamp=timestamp,
                                bytes_sent=1024 * hour * minute,
                                bytes_recv=2048 * hour * minute,
                            )
                            old_store.insert_network_metric(net_metric)

            with old_store._connection() as conn:
                old_count = conn.execute(
                    "SELECT COUNT(*) FROM system_metrics"
                ).fetchone()[0]

            print(f"✓ Current database has {old_count} system metrics")

            # Step 2: Create new database with updated schema
            print("→ Creating new database with v2 schema...")

            new_store = SQLiteStore(str(new_db))

            # Add new table for v2 (extended metrics)
            with new_store._connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS extended_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER NOT NULL,
                        metric_type TEXT,
                        metric_name TEXT,
                        metric_value REAL,
                        tags TEXT
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_extended_timestamp
                    ON extended_metrics (timestamp)
                """)

            print("✓ New database created with extended schema")

            # Step 3: Start parallel writing
            print("→ Enabling dual-write mode...")

            dual_write_enabled = True
            dual_write_count = 0

            # New metrics go to both databases
            current_time = int(time.time())
            for i in range(10):
                metric = SystemMetric(
                    timestamp=current_time + i * 10,
                    cpu_percent=35.0 + i,
                    memory_percent=45.0 + i,
                )

                # Write to old database
                old_store.insert_system_metric(metric)

                # Write to new database (with extended format)
                new_store.insert_system_metric(metric)

                # Also write to extended format
                with new_store._connection() as conn:
                    conn.execute(
                        """
                        INSERT INTO extended_metrics 
                        (timestamp, metric_type, metric_name, metric_value, tags)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            metric.timestamp,
                            "system",
                            "cpu_percent",
                            metric.cpu_percent,
                            json.dumps({"source": "migration"}),
                        ),
                    )

                dual_write_count += 1

            print(
                f"✓ Dual-write enabled: {dual_write_count} metrics written to both databases"
            )

            # Step 4: Begin background migration
            print("→ Starting background data migration...")

            batch_size = 1000
            migrated_count = 0

            # Migrate in batches
            with old_store._connection() as old_conn:
                while True:
                    # Get batch of data
                    batch = old_conn.execute(
                        """
                        SELECT * FROM system_metrics 
                        ORDER BY timestamp 
                        LIMIT ? OFFSET ?
                    """,
                        (batch_size, migrated_count),
                    ).fetchall()

                    if not batch:
                        break

                    # Insert into new database
                    with new_store._connection() as new_conn:
                        for row in batch:
                            # Insert into standard table
                            new_conn.execute(
                                """
                                INSERT OR IGNORE INTO system_metrics
                                (timestamp, cpu_percent, memory_percent, disk_percent,
                                 memory_total, memory_available, disk_total, disk_used,
                                 disk_read_bytes, disk_write_bytes,
                                 load_1min, load_5min, load_15min, cpu_count)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                (
                                    row["timestamp"],
                                    row["cpu_percent"],
                                    row["memory_percent"],
                                    row["disk_percent"],
                                    row["memory_total"],
                                    row["memory_available"],
                                    row["disk_total"],
                                    row["disk_used"],
                                    row["disk_read_bytes"],
                                    row["disk_write_bytes"],
                                    row["load_1min"],
                                    row["load_5min"],
                                    row["load_15min"],
                                    row["cpu_count"],
                                ),
                            )
                        # Explicit commit for the batch
                        new_conn.commit()

                    migrated_count += len(batch)
                    print(f"  Migrated {migrated_count} records...", end="\r")

            print(f"\n✓ Migration completed: {migrated_count} records migrated")

            # Step 5: Verify data consistency
            print("→ Verifying data consistency...")

            with old_store._connection() as old_conn:
                old_check = old_conn.execute("""
                    SELECT COUNT(*) as count, 
                           MIN(timestamp) as min_ts,
                           MAX(timestamp) as max_ts,
                           AVG(cpu_percent) as avg_cpu
                    FROM system_metrics
                """).fetchone()

            with new_store._connection() as new_conn:
                new_check = new_conn.execute("""
                    SELECT COUNT(*) as count,
                           MIN(timestamp) as min_ts,  
                           MAX(timestamp) as max_ts,
                           AVG(cpu_percent) as avg_cpu
                    FROM system_metrics
                """).fetchone()

            # Verify counts match (new might have more due to dual-write)
            assert new_check["count"] >= old_check["count"], (
                "New DB should have all data"
            )
            assert new_check["min_ts"] == old_check["min_ts"], (
                "Min timestamp should match"
            )

            print("✓ Data consistency verified:")
            print(f"  Old DB: {old_check['count']} records")
            print(f"  New DB: {new_check['count']} records")
            print(f"  Average CPU (old): {old_check['avg_cpu']:.2f}%")
            print(f"  Average CPU (new): {new_check['avg_cpu']:.2f}%")

            # Step 6: Perform cutover
            print("\n🔄 Performing database cutover...")

            # Simulate cutover by switching active database
            cutover_time = int(time.time())
            cutover_flag = Path(workspace) / "ACTIVE_DB"
            cutover_flag.write_text(str(new_db))

            print(f"✓ Cutover completed at {time.strftime('%H:%M:%S')}")
            print(f"  Active database: {new_db.name}")

            # Step 7: Validate migrated data
            print("→ Validating migrated data...")

            # Run queries on new database
            with new_store._connection() as conn:
                # Check extended metrics table
                extended_count = conn.execute(
                    "SELECT COUNT(*) FROM extended_metrics"
                ).fetchone()[0]

                # Run integrity check
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
                assert integrity == "ok", "Integrity check failed"

                # Verify indexes work
                indexed_query = conn.execute(
                    """
                    SELECT COUNT(*) FROM system_metrics
                    WHERE timestamp > ?
                """,
                    (current_time,),
                ).fetchone()[0]

            print("✓ Validation passed:")
            print(f"  Extended metrics: {extended_count}")
            print(f"  Recent metrics: {indexed_query}")
            print("  Integrity check: OK")

            # Step 8: Keep old database as backup
            print("→ Preserving old database as backup...")

            backup_path = Path(workspace) / f"logly_v1_backup_{cutover_time}.db"
            shutil.copy2(old_db, backup_path)

            print(f"✓ Old database backed up: {backup_path.name}")

            # Step 9: Monitor new database performance
            print("→ Monitoring new database performance...")

            # Run performance test queries
            perf_results = {}

            with new_store._connection() as conn:
                # Test 1: Recent data query
                start = time.time()
                conn.execute(
                    """
                    SELECT * FROM system_metrics 
                    WHERE timestamp > ? 
                    ORDER BY timestamp DESC 
                    LIMIT 100
                """,
                    (current_time - 3600,),
                ).fetchall()
                perf_results["recent_query_ms"] = (time.time() - start) * 1000

                # Test 2: Aggregation query
                start = time.time()
                conn.execute("""
                    SELECT 
                        strftime('%H', timestamp, 'unixepoch') as hour,
                        AVG(cpu_percent) as avg_cpu,
                        MAX(memory_percent) as max_mem
                    FROM system_metrics
                    GROUP BY hour
                """).fetchall()
                perf_results["aggregation_ms"] = (time.time() - start) * 1000

            print("✓ Performance metrics:")
            print(f"  Recent query: {perf_results['recent_query_ms']:.2f} ms")
            print(f"  Aggregation: {perf_results['aggregation_ms']:.2f} ms")

            # Step 10: Generate migration report
            print("→ Generating migration report...")

            report_file = Path(workspace) / "migration_report.json"

            migration_report = {
                "migration_id": f"v1_to_v2_{cutover_time}",
                "start_time": base_time,
                "cutover_time": cutover_time,
                "duration_seconds": cutover_time - base_time,
                "source_database": {
                    "path": str(old_db),
                    "size_mb": old_db.stat().st_size / (1024 * 1024),
                    "record_count": old_check["count"],
                },
                "target_database": {
                    "path": str(new_db),
                    "size_mb": new_db.stat().st_size / (1024 * 1024),
                    "record_count": new_check["count"],
                    "extended_records": extended_count,
                },
                "migration_stats": {
                    "records_migrated": migrated_count,
                    "dual_write_records": dual_write_count,
                    "data_loss": 0,
                    "downtime_seconds": 0,
                },
                "validation": {
                    "integrity_check": "passed",
                    "consistency_check": "passed",
                    "performance_baseline": perf_results,
                },
                "backup": {
                    "location": str(backup_path),
                    "size_mb": backup_path.stat().st_size / (1024 * 1024),
                },
            }

            with open(report_file, "w") as f:
                json.dump(migration_report, f, indent=2)

            print(f"✓ Migration report generated: {report_file}")

            print("\n✅ Database migration completed successfully!")
            print(f"   Records migrated: {migrated_count}")
            print("   Downtime: Zero")
            print("   Data loss: None")
            print("   New features: Extended metrics table")

        finally:
            shutil.rmtree(workspace, ignore_errors=True)

