#!/usr/bin/env python3
"""
Quick test to verify database concurrency fixes
"""
import tempfile
import time
from pathlib import Path
import threading

from logly.storage.sqlite_store import SQLiteStore
from logly.storage.models import SystemMetric

def insert_metrics(store, thread_id, count=10):
    """Insert metrics from a thread"""
    for i in range(count):
        metric = SystemMetric.now(
            cpu_percent=10.0 + thread_id,
            memory_percent=50.0 + i,
            disk_percent=70.0
        )
        try:
            store.insert_system_metric(metric)
            print(f"Thread {thread_id}: Inserted metric {i+1}/{count}")
        except Exception as e:
            print(f"Thread {thread_id}: ERROR - {e}")
        time.sleep(0.05)  # Small delay

def main():
    print("Testing database concurrency...")

    # Create temporary database
    temp_dir = tempfile.mkdtemp(prefix="logly_concurrency_test_")
    db_path = Path(temp_dir) / "test.db"

    try:
        # Initialize store
        store = SQLiteStore(str(db_path))
        print(f"Database initialized at {db_path}")

        # Create multiple threads that write simultaneously
        threads = []
        num_threads = 5
        metrics_per_thread = 10

        print(f"\nStarting {num_threads} threads, each inserting {metrics_per_thread} metrics...")
        start_time = time.time()

        for i in range(num_threads):
            t = threading.Thread(target=insert_metrics, args=(store, i, metrics_per_thread))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        elapsed = time.time() - start_time
        print(f"\nAll threads completed in {elapsed:.2f} seconds")

        # Verify all metrics were inserted
        with store._connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM system_metrics").fetchone()[0]
            print(f"\nTotal metrics in database: {count}")
            expected = num_threads * metrics_per_thread

            if count == expected:
                print(f"✓ SUCCESS: All {expected} metrics inserted correctly")
                return 0
            else:
                print(f"✗ FAILURE: Expected {expected} metrics, got {count}")
                return 1

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    exit(main())
