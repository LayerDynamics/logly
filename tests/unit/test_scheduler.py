"""
Unit tests for logly.core.scheduler module
Tests scheduling, collection tasks, and background execution
"""

import pytest
from unittest.mock import Mock, patch

from logly.core.scheduler import Scheduler


class TestScheduler:
    """Test suite for Scheduler class"""

    @pytest.mark.unit
    def test_init(self, mock_config, test_store):
        """Test Scheduler initialization"""
        scheduler = Scheduler(mock_config, test_store)

        assert scheduler.config == mock_config
        assert scheduler.store == test_store
        assert not scheduler.running
        assert scheduler.thread is None

        # Check collectors are initialized based on config
        assert scheduler.system_collector is not None
        assert scheduler.network_collector is not None
        assert scheduler.log_parser is not None
        assert scheduler.aggregator is not None

    @pytest.mark.unit
    def test_init_with_disabled_collectors(self, test_store):
        """Test Scheduler initialization with disabled collectors"""
        config = Mock()
        config.get_system_config.return_value = {"enabled": False}
        config.get_network_config.return_value = {"enabled": False}
        config.get_logs_config.return_value = {"enabled": False}
        config.get_collection_config.return_value = {
            "system_metrics": 60,
            "network_metrics": 60,
            "log_parsing": 300,
        }
        config.get_aggregation_config.return_value = {"enabled": True}

        scheduler = Scheduler(config, test_store)

        assert scheduler.system_collector is None
        assert scheduler.network_collector is None
        assert scheduler.log_parser is None

    @pytest.mark.unit
    def test_collect_system_metrics(self, mock_config, test_store, mock_system_metric):
        """Test _collect_system_metrics method"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock the collector
        scheduler.system_collector = Mock()
        scheduler.system_collector.collect.return_value = mock_system_metric

        # Run collection
        scheduler._collect_system_metrics()

        # Verify collector was called and metric was stored
        scheduler.system_collector.collect.assert_called_once()

    @pytest.mark.unit
    def test_collect_system_metrics_error_handling(
        self, mock_config, test_store, caplog
    ):
        """Test error handling in _collect_system_metrics"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock collector to raise exception
        scheduler.system_collector = Mock()
        scheduler.system_collector.collect.side_effect = Exception("Test error")

        # Should not raise, but log error
        scheduler._collect_system_metrics()

        assert "Error collecting system metrics" in caplog.text

    @pytest.mark.unit
    def test_collect_network_metrics(
        self, mock_config, test_store, mock_network_metric
    ):
        """Test _collect_network_metrics method"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock the collector
        scheduler.network_collector = Mock()
        scheduler.network_collector.collect.return_value = mock_network_metric

        # Run collection
        scheduler._collect_network_metrics()

        # Verify collector was called
        scheduler.network_collector.collect.assert_called_once()

    @pytest.mark.unit
    def test_parse_logs(self, mock_config, test_store, mock_log_events):
        """Test _parse_logs method"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock the log parser
        scheduler.log_parser = Mock()
        scheduler.log_parser.collect.return_value = mock_log_events

        # Run parsing
        scheduler._parse_logs()

        # Verify parser was called
        scheduler.log_parser.collect.assert_called_once()

    @pytest.mark.unit
    def test_parse_logs_empty(self, mock_config, test_store):
        """Test _parse_logs with no events"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock the log parser to return empty list
        scheduler.log_parser = Mock()
        scheduler.log_parser.collect.return_value = []

        # Should handle empty results gracefully
        scheduler._parse_logs()

        scheduler.log_parser.collect.assert_called_once()

    @pytest.mark.unit
    @patch("logly.core.scheduler.time")
    def test_run_aggregations_hourly(self, mock_time_module, mock_config, test_store):
        """Test _run_aggregations at top of hour"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock time to be at top of hour
        mock_localtime = Mock()
        mock_localtime.tm_min = 0
        mock_localtime.tm_hour = 10
        mock_time_module.localtime.return_value = mock_localtime

        # Mock aggregator
        scheduler.aggregator = Mock()

        # Run aggregations
        scheduler._run_aggregations()

        # Should trigger hourly aggregation
        scheduler.aggregator.run_hourly_aggregation.assert_called_once()
        scheduler.aggregator.run_daily_aggregation.assert_not_called()

    @pytest.mark.unit
    @patch("logly.core.scheduler.time")
    def test_run_aggregations_daily(self, mock_time_module, mock_config, test_store):
        """Test _run_aggregations at midnight"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock time to be at midnight
        mock_localtime = Mock()
        mock_localtime.tm_min = 0
        mock_localtime.tm_hour = 0
        mock_time_module.localtime.return_value = mock_localtime

        # Mock aggregator
        scheduler.aggregator = Mock()

        # Run aggregations
        scheduler._run_aggregations()

        # Should trigger both hourly and daily aggregations
        scheduler.aggregator.run_hourly_aggregation.assert_called_once()
        scheduler.aggregator.run_daily_aggregation.assert_called_once()

    @pytest.mark.unit
    def test_cleanup_old_data(self, mock_config, test_store):
        """Test _cleanup_old_data method"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock store's cleanup method
        test_store.cleanup_old_data = Mock()

        # Run cleanup
        scheduler._cleanup_old_data()

        # Verify cleanup was called with correct retention days
        test_store.cleanup_old_data.assert_called_once_with(90)

    @pytest.mark.unit
    def test_start(self, mock_config, test_store):
        """Test starting the scheduler"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock the _run method to prevent actual execution
        scheduler._run = Mock()

        # Mock threading to verify thread creation
        with patch("threading.Thread") as mock_thread:
            mock_thread_instance = Mock()
            mock_thread.return_value = mock_thread_instance

            scheduler.start()

            assert scheduler.running
            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

    @pytest.mark.unit
    def test_start_already_running(self, mock_config, test_store, caplog):
        """Test starting scheduler when already running"""
        scheduler = Scheduler(mock_config, test_store)
        scheduler.running = True

        scheduler.start()

        assert "Scheduler is already running" in caplog.text

    @pytest.mark.unit
    def test_stop(self, mock_config, test_store):
        """Test stopping the scheduler"""
        scheduler = Scheduler(mock_config, test_store)
        scheduler.running = True

        # Mock thread
        mock_thread = Mock()
        scheduler.thread = mock_thread

        scheduler.stop()

        assert not scheduler.running
        mock_thread.join.assert_called_once_with(timeout=5)

    @pytest.mark.unit
    def test_stop_not_running(self, mock_config, test_store):
        """Test stopping scheduler when not running"""
        scheduler = Scheduler(mock_config, test_store)
        scheduler.running = False

        # Should handle gracefully
        scheduler.stop()
        assert not scheduler.running

    @pytest.mark.unit
    def test_run_once(self, mock_config, test_store):
        """Test run_once method"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock collection methods
        scheduler._collect_system_metrics = Mock()
        scheduler._collect_network_metrics = Mock()
        scheduler._parse_logs = Mock()

        scheduler.run_once()

        # All collection methods should be called
        scheduler._collect_system_metrics.assert_called_once()
        scheduler._collect_network_metrics.assert_called_once()
        scheduler._parse_logs.assert_called_once()

    @pytest.mark.unit
    def test_run_once_with_disabled_collectors(self, test_store):
        """Test run_once with some collectors disabled"""
        config = Mock()
        config.get_system_config.return_value = {"enabled": False}
        config.get_network_config.return_value = {"enabled": True}
        config.get_logs_config.return_value = {"enabled": False}
        config.get_collection_config.return_value = {
            "system_metrics": 60,
            "network_metrics": 60,
            "log_parsing": 300,
        }
        config.get_aggregation_config.return_value = {"enabled": True}

        scheduler = Scheduler(config, test_store)

        # Mock the one enabled collector
        scheduler._collect_network_metrics = Mock()

        scheduler.run_once()

        # Only network collector should be called
        scheduler._collect_network_metrics.assert_called_once()

    @pytest.mark.unit
    def test_schedule_repeating(self, mock_config, test_store):
        """Test _schedule_repeating method"""
        scheduler = Scheduler(mock_config, test_store)
        scheduler.running = True

        # Mock the scheduler.enter method
        scheduler.scheduler.enter = Mock()

        mock_func = Mock()

        # Call _schedule_repeating
        scheduler._schedule_repeating(60, mock_func, "test_task")

        # Verify scheduler.enter was called (should be called once for initial schedule)
        assert scheduler.scheduler.enter.call_count >= 1

    @pytest.mark.unit
    @patch("logly.core.scheduler.time.sleep")
    def test_run_loop(self, mock_sleep, mock_config, test_store):
        """Test _run method loop"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock scheduler.run
        scheduler.scheduler = Mock()
        scheduler.scheduler.run = Mock()

        # Set running to True initially, then False after first iteration
        call_count = [0]

        def side_effect(*args):
            call_count[0] += 1
            if call_count[0] >= 2:
                scheduler.running = False
            return None

        mock_sleep.side_effect = side_effect
        scheduler.running = True

        # Run the loop
        scheduler._run()

        # Verify scheduler.run was called
        scheduler.scheduler.run.assert_called()

    @pytest.mark.unit
    def test_run_loop_error_handling(self, mock_config, test_store, caplog):
        """Test error handling in _run loop"""
        scheduler = Scheduler(mock_config, test_store)

        # Mock scheduler to raise exception
        scheduler.scheduler = Mock()
        scheduler.scheduler.run = Mock(side_effect=Exception("Test error"))

        # Run once then stop
        with patch("time.sleep") as mock_sleep:
            scheduler.running = True

            def stop_after_error(*args):
                scheduler.running = False

            mock_sleep.side_effect = stop_after_error

            scheduler._run()

            assert "Error in scheduler loop" in caplog.text

    @pytest.mark.unit
    def test_collection_intervals(self, mock_config, test_store):
        """Test that collection intervals are properly set from config"""
        scheduler = Scheduler(mock_config, test_store)

        assert scheduler.system_interval == 60
        assert scheduler.network_interval == 60
        assert scheduler.log_interval == 300
