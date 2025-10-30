"""
Unit tests for logly.utils.create_db module
Tests database creation and initialization functionality
"""

import pytest
import sqlite3
from unittest.mock import patch

from logly.utils.create_db import (
    db_exists,
    create_database,
    initialize_db_if_needed,
    get_db_info
)


class TestDatabaseCreation:
    """Test suite for database creation utilities"""

    @pytest.mark.unit
    def test_db_exists_returns_false_when_no_db(self, temp_dir):
        """Test db_exists returns False when database doesn't exist"""
        with patch('logly.utils.create_db.get_db_path', return_value=temp_dir / "nonexistent.db"):
            assert db_exists() is False

    @pytest.mark.unit
    def test_db_exists_returns_true_when_db_exists(self, temp_dir):
        """Test db_exists returns True when database exists"""
        db_path = temp_dir / "test.db"
        db_path.touch()  # Create empty file

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            assert db_exists() is True

    @pytest.mark.unit
    def test_db_exists_returns_false_for_directory(self, temp_dir):
        """Test db_exists returns False if path is a directory"""
        db_dir = temp_dir / "test_db_dir"
        db_dir.mkdir()

        with patch('logly.utils.create_db.get_db_path', return_value=db_dir):
            assert db_exists() is False

    @pytest.mark.unit
    def test_create_database_creates_db_file(self, temp_dir):
        """Test create_database creates the database file"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                result = create_database()

                assert result == db_path
                assert db_path.exists()
                assert db_path.is_file()

    @pytest.mark.unit
    def test_create_database_creates_parent_directory(self, temp_dir):
        """Test create_database creates parent directory if needed"""
        db_dir = temp_dir / "db"
        db_path = db_dir / "test.db"

        assert not db_dir.exists()

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=db_dir):
                create_database()

                assert db_dir.exists()
                assert db_path.exists()

    @pytest.mark.unit
    def test_create_database_executes_schema(self, temp_dir):
        """Test create_database executes schema.sql"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                create_database()

                # Verify tables were created
                conn = sqlite3.connect(db_path)
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
                tables = [row[0] for row in cursor.fetchall()]
                conn.close()

                # Check that key tables exist
                assert 'system_metrics' in tables
                assert 'network_metrics' in tables
                assert 'log_events' in tables
                assert 'hourly_aggregates' in tables
                assert 'daily_aggregates' in tables
                assert 'metadata' in tables
                assert 'event_traces' in tables
                assert 'process_traces' in tables
                assert 'network_traces' in tables
                assert 'error_traces' in tables
                assert 'ip_reputation' in tables
                assert 'trace_patterns' in tables

    @pytest.mark.unit
    def test_create_database_returns_existing_db_path(self, temp_dir):
        """Test create_database returns path when db exists and force=False"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Create database first time
                create_database()

                # Try to create again without force
                result = create_database(force=False)

                assert result == db_path

    @pytest.mark.unit
    def test_create_database_with_force_recreates_db(self, temp_dir):
        """Test create_database with force=True recreates database"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Create database first time
                create_database()

                # Add some data
                conn = sqlite3.connect(db_path)
                conn.execute("INSERT INTO metadata (key, value, updated_at) VALUES (?, ?, ?)",
                           ("test_key", "test_value", 123456))
                conn.commit()
                conn.close()

                # Recreate with force
                create_database(force=True)

                # Verify database was recreated (test data should be gone)
                conn = sqlite3.connect(db_path)
                cursor = conn.execute("SELECT * FROM metadata WHERE key = 'test_key'")
                result = cursor.fetchone()
                conn.close()

                assert result is None  # Test data should be gone

    @pytest.mark.unit
    def test_create_database_handles_missing_schema(self, temp_dir):
        """Test create_database raises error if schema.sql not found"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Create a temporary file for __file__ and make schema path not exist
                fake_module_dir = temp_dir / "utils"
                fake_module_dir.mkdir()

                # Patch __file__ within the module
                with patch('logly.utils.create_db.__file__', str(fake_module_dir / "create_db.py")):
                    # The schema should be at temp_dir/storage/schema.sql (which doesn't exist)
                    with pytest.raises(FileNotFoundError, match="Schema file not found"):
                        create_database()

    @pytest.mark.unit
    def test_create_database_cleans_up_on_failure(self, temp_dir):
        """Test create_database cleans up partial database on failure"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Mock schema read to raise an error
                with patch('builtins.open', side_effect=sqlite3.Error("SQL error")):
                    with pytest.raises(sqlite3.Error):
                        create_database()

                    # Verify database file was cleaned up
                    assert not db_path.exists()

    @pytest.mark.unit
    def test_create_database_verifies_table_creation(self, temp_dir):
        """Test create_database verifies tables were created"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                create_database()

                # Verify table count is logged
                conn = sqlite3.connect(db_path)
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                )
                table_count = cursor.fetchone()[0]
                conn.close()

                # Should have all expected tables
                assert table_count >= 12  # At least 12 main tables

    @pytest.mark.unit
    def test_initialize_db_if_needed_creates_when_missing(self, temp_dir):
        """Test initialize_db_if_needed creates database when it doesn't exist"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                result = initialize_db_if_needed()

                assert result == db_path
                assert db_path.exists()

    @pytest.mark.unit
    def test_initialize_db_if_needed_skips_when_exists(self, temp_dir):
        """Test initialize_db_if_needed doesn't recreate existing database"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Create database first
                create_database()

                # Add test data
                conn = sqlite3.connect(db_path)
                conn.execute("INSERT INTO metadata (key, value, updated_at) VALUES (?, ?, ?)",
                           ("test_marker", "exists", 123456))
                conn.commit()
                conn.close()

                # Call initialize again
                initialize_db_if_needed()

                # Verify test data still exists (db wasn't recreated)
                conn = sqlite3.connect(db_path)
                cursor = conn.execute("SELECT value FROM metadata WHERE key = 'test_marker'")
                result = cursor.fetchone()
                conn.close()

                assert result is not None
                assert result[0] == "exists"

    @pytest.mark.unit
    def test_get_db_info_returns_none_when_no_db(self, temp_dir):
        """Test get_db_info returns None when database doesn't exist"""
        with patch('logly.utils.create_db.get_db_path', return_value=temp_dir / "nonexistent.db"):
            result = get_db_info()
            assert result is None

    @pytest.mark.unit
    def test_get_db_info_returns_correct_info(self, temp_dir):
        """Test get_db_info returns correct database information"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Create database
                create_database()

                # Get info
                info = get_db_info()

                assert info is not None
                assert info['path'] == str(db_path)
                assert info['exists'] is True
                assert info['size_bytes'] > 0
                assert info['size_mb'] > 0
                assert info['table_count'] >= 12
                assert info['schema_version'] == '2.0'

    @pytest.mark.unit
    def test_get_db_info_calculates_size_correctly(self, temp_dir):
        """Test get_db_info calculates file size correctly"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                create_database()

                info = get_db_info()

                # Verify size calculations
                assert info is not None
                actual_size = db_path.stat().st_size
                assert info['size_bytes'] == actual_size
                assert info['size_mb'] == round(actual_size / (1024 * 1024), 2)

    @pytest.mark.unit
    def test_get_db_info_handles_missing_metadata(self, temp_dir):
        """Test get_db_info handles missing metadata gracefully"""
        db_path = temp_dir / "test.db"

        # Create empty database without schema
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE test_table (id INTEGER)")
        conn.commit()
        conn.close()

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.db_exists', return_value=True):
                info = get_db_info()

                assert info is not None
                assert info['schema_version'] == 'unknown'
                assert info['table_count'] == 1

    @pytest.mark.unit
    def test_get_db_info_handles_corrupted_db(self, temp_dir):
        """Test get_db_info handles corrupted database"""
        db_path = temp_dir / "corrupted.db"

        # Create corrupted database file
        with open(db_path, 'w') as f:
            f.write("This is not a valid SQLite database")

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            info = get_db_info()

            assert info is not None
            assert info['exists'] is True
            assert 'error' in info

    @pytest.mark.unit
    def test_create_database_sets_initial_metadata(self, temp_dir):
        """Test create_database sets initial metadata values"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                create_database()

                conn = sqlite3.connect(db_path)

                # Check schema_version
                cursor = conn.execute("SELECT value FROM metadata WHERE key = 'schema_version'")
                version = cursor.fetchone()
                assert version is not None
                assert version[0] == '2.0'

                # Check created_at exists
                cursor = conn.execute("SELECT value FROM metadata WHERE key = 'created_at'")
                created_at = cursor.fetchone()
                assert created_at is not None

                # Check hostname exists (may be empty)
                cursor = conn.execute("SELECT key FROM metadata WHERE key = 'hostname'")
                hostname = cursor.fetchone()
                assert hostname is not None

                conn.close()

    @pytest.mark.unit
    def test_create_database_creates_all_indexes(self, temp_dir):
        """Test create_database creates all required indexes"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                create_database()

                conn = sqlite3.connect(db_path)
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
                )
                indexes = [row[0] for row in cursor.fetchall()]
                conn.close()

                # Check for key indexes
                assert any('system_metrics_timestamp' in idx for idx in indexes)
                assert any('network_metrics_timestamp' in idx for idx in indexes)
                assert any('log_events_timestamp' in idx for idx in indexes)
                assert any('log_events_source' in idx for idx in indexes)
                assert any('event_traces_timestamp' in idx for idx in indexes)
                assert any('ip_reputation_threat' in idx for idx in indexes)

    @pytest.mark.unit
    def test_create_database_idempotent_without_force(self, temp_dir):
        """Test create_database is idempotent when called multiple times without force"""
        db_path = temp_dir / "test.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                # Create multiple times
                result1 = create_database()
                result2 = create_database()
                result3 = create_database()

                # All should return the same path
                assert result1 == result2 == result3 == db_path

                # Verify database is still valid
                info = get_db_info()
                assert info is not None
                assert info['table_count'] >= 12

    @pytest.mark.unit
    def test_db_exists_with_symlink(self, temp_dir):
        """Test db_exists handles symlinks correctly"""
        real_db = temp_dir / "real.db"
        real_db.touch()

        link_db = temp_dir / "link.db"
        link_db.symlink_to(real_db)

        with patch('logly.utils.create_db.get_db_path', return_value=link_db):
            assert db_exists() is True

    @pytest.mark.unit
    def test_create_database_with_read_only_directory(self, temp_dir):
        """Test create_database handles permission errors"""
        db_dir = temp_dir / "readonly"
        db_dir.mkdir()
        db_path = db_dir / "test.db"

        # Make directory read-only
        import os
        os.chmod(db_dir, 0o444)

        try:
            with patch('logly.utils.create_db.get_db_path', return_value=db_path):
                with patch('logly.utils.create_db.get_db_dir', return_value=db_dir):
                    with pytest.raises((sqlite3.Error, OSError, PermissionError)):
                        create_database()
        finally:
            # Restore permissions for cleanup
            os.chmod(db_dir, 0o755)

    @pytest.mark.unit
    def test_initialize_db_if_needed_thread_safe(self, temp_dir):
        """Test initialize_db_if_needed can be called from multiple threads"""
        import threading

        db_path = temp_dir / "test.db"
        results = []

        def init_db():
            with patch('logly.utils.create_db.get_db_path', return_value=db_path):
                with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                    try:
                        result = initialize_db_if_needed()
                        results.append(result)
                    except Exception as e:
                        results.append(e)

        # Create multiple threads
        threads = [threading.Thread(target=init_db) for _ in range(5)]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # All should succeed and return the same path
        assert len(results) == 5
        assert all(r == db_path for r in results)
        assert db_path.exists()

    @pytest.mark.unit
    def test_get_db_info_with_large_database(self, temp_dir):
        """Test get_db_info handles large databases correctly"""
        db_path = temp_dir / "large.db"

        with patch('logly.utils.create_db.get_db_path', return_value=db_path):
            with patch('logly.utils.create_db.get_db_dir', return_value=temp_dir):
                create_database()

                # Add some data to increase size
                conn = sqlite3.connect(db_path)
                for i in range(1000):
                    conn.execute(
                        "INSERT INTO metadata (key, value, updated_at) VALUES (?, ?, ?)",
                        (f"key_{i}", "x" * 100, i)
                    )
                conn.commit()
                conn.close()

                info = get_db_info()

                assert info is not None
                assert info['size_bytes'] > 10000  # Should be reasonably large
                assert info['size_mb'] > 0.0
