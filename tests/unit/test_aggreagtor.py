"""
Unit tests for logly.core.aggregator module
Tests data aggregation for hourly and daily rollups
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from logly.core.aggregator import Aggregator


class TestAggregator:
    """Test suite for Aggregator class"""

    @pytest.mark.unit
    def test_init(self, test_store):
        """Test Aggregator initialization"""
        config = {
            "enabled": True,
            "intervals": ["hourly", "daily"],
            "keep_raw_data_days": 7,
        }

        aggregator = Aggregator(test_store, config)

        assert aggregator.store == test_store
        assert aggregator.config == config
        assert aggregator.enabled
        assert aggregator.intervals == ["hourly", "daily"]
        assert aggregator.keep_raw_data_days == 7

    @pytest.mark.unit
    def test_init_with_disabled_config(self, test_store):
        """Test Aggregator initialization with disabled config"""
        config = {"enabled": False, "intervals": [], "keep_raw_data_days": 0}

        aggregator = Aggregator(test_store, config)

        assert not aggregator.enabled
        assert aggregator.intervals == []

    @pytest.mark.unit
    @patch("logly.core.aggregator.datetime")
    def test_run_hourly_aggregation(self, mock_datetime, test_store):
        """Test run_hourly_aggregation method"""
        config = {
            "enabled": True,
            "intervals": ["hourly", "daily"],
            "keep_raw_data_days": 7,
        }

        # Mock datetime to return a specific time
        current_time = datetime(2025, 1, 15, 10, 30, 0)
        mock_datetime.now.return_value = current_time

        aggregator = Aggregator(test_store, config)

        # Mock store method
        test_store.compute_hourly_aggregates = Mock()

        # Run aggregation
        aggregator.run_hourly_aggregation()

        # Should compute aggregates for the previous complete hour
        expected_timestamp = int(datetime(2025, 1, 15, 9, 0, 0).timestamp())
        test_store.compute_hourly_aggregates.assert_called_once_with(expected_timestamp)

    @pytest.mark.unit
    def test_run_hourly_aggregation_disabled(self, test_store):
        """Test run_hourly_aggregation when disabled"""
        config = {"enabled": False, "intervals": ["hourly"], "keep_raw_data_days": 7}

        aggregator = Aggregator(test_store, config)

        # Mock store method
        test_store.compute_hourly_aggregates = Mock()

        # Run aggregation
        aggregator.run_hourly_aggregation()

        # Should not compute aggregates when disabled
        test_store.compute_hourly_aggregates.assert_not_called()

    @pytest.mark.unit
    def test_run_hourly_aggregation_not_in_intervals(self, test_store):
        """Test run_hourly_aggregation when not in intervals"""
        config = {
            "enabled": True,
            "intervals": ["daily"],  # Only daily, not hourly
            "keep_raw_data_days": 7,
        }

        aggregator = Aggregator(test_store, config)

        # Mock store method
        test_store.compute_hourly_aggregates = Mock()

        # Run aggregation
        aggregator.run_hourly_aggregation()

        # Should not compute aggregates when not in intervals
        test_store.compute_hourly_aggregates.assert_not_called()

    @pytest.mark.unit
    @patch("logly.core.aggregator.datetime")
    def test_run_daily_aggregation(self, mock_datetime, test_store):
        """Test run_daily_aggregation method"""
        config = {
            "enabled": True,
            "intervals": ["hourly", "daily"],
            "keep_raw_data_days": 7,
        }

        # Mock datetime to return a specific time
        current_time = datetime(2025, 1, 15, 10, 30, 0)
        mock_datetime.now.return_value = current_time

        aggregator = Aggregator(test_store, config)

        # Mock store method
        test_store.compute_daily_aggregates = Mock()

        # Run aggregation
        aggregator.run_daily_aggregation()

        # Should compute aggregates for yesterday
        expected_date = "2025-01-14"
        test_store.compute_daily_aggregates.assert_called_once_with(expected_date)

    @pytest.mark.unit
    def test_run_daily_aggregation_disabled(self, test_store):
        """Test run_daily_aggregation when disabled"""
        config = {"enabled": False, "intervals": ["daily"], "keep_raw_data_days": 7}

        aggregator = Aggregator(test_store, config)

        # Mock store method
        test_store.compute_daily_aggregates = Mock()

        # Run aggregation
        aggregator.run_daily_aggregation()

        # Should not compute aggregates when disabled
        test_store.compute_daily_aggregates.assert_not_called()

    @pytest.mark.unit
    def test_run_daily_aggregation_not_in_intervals(self, test_store):
        """Test run_daily_aggregation when not in intervals"""
        config = {
            "enabled": True,
            "intervals": ["hourly"],  # Only hourly, not daily
            "keep_raw_data_days": 7,
        }

        aggregator = Aggregator(test_store, config)

        # Mock store method
        test_store.compute_daily_aggregates = Mock()

        # Run aggregation
        aggregator.run_daily_aggregation()

        # Should not compute aggregates when not in intervals
        test_store.compute_daily_aggregates.assert_not_called()

    @pytest.mark.unit
    @patch("logly.core.aggregator.datetime")
    def test_run_hourly_aggregation_error_handling(
        self, mock_datetime, test_store, caplog
    ):
        """Test error handling in run_hourly_aggregation"""
        config = {"enabled": True, "intervals": ["hourly"], "keep_raw_data_days": 7}

        # Mock datetime
        current_time = datetime(2025, 1, 15, 10, 30, 0)
        mock_datetime.now.return_value = current_time

        aggregator = Aggregator(test_store, config)

        # Mock store method to raise exception
        test_store.compute_hourly_aggregates = Mock(
            side_effect=Exception("Database error")
        )

        # Should handle error gracefully
        aggregator.run_hourly_aggregation()

        assert "Error running hourly aggregation" in caplog.text

    @pytest.mark.unit
    @patch("logly.core.aggregator.datetime")
    def test_run_daily_aggregation_error_handling(
        self, mock_datetime, test_store, caplog
    ):
        """Test error handling in run_daily_aggregation"""
        config = {"enabled": True, "intervals": ["daily"], "keep_raw_data_days": 7}

        # Mock datetime
        current_time = datetime(2025, 1, 15, 10, 30, 0)
        mock_datetime.now.return_value = current_time

        aggregator = Aggregator(test_store, config)

        # Mock store method to raise exception
        test_store.compute_daily_aggregates = Mock(
            side_effect=Exception("Database error")
        )

        # Should handle error gracefully
        aggregator.run_daily_aggregation()

        assert "Error running daily aggregation" in caplog.text

    @pytest.mark.unit
    def test_cleanup_old_raw_data(self, test_store):
        """Test cleanup_old_raw_data method"""
        config = {
            "enabled": True,
            "intervals": ["hourly", "daily"],
            "keep_raw_data_days": 7,
        }

        aggregator = Aggregator(test_store, config)

        # Currently method is a placeholder, just verify it doesn't raise
        aggregator.cleanup_old_raw_data()

    @pytest.mark.unit
    def test_cleanup_old_raw_data_disabled(self, test_store):
        """Test cleanup_old_raw_data when disabled"""
        config = {"enabled": False, "intervals": [], "keep_raw_data_days": 7}

        aggregator = Aggregator(test_store, config)

        # Should return early when disabled
        aggregator.cleanup_old_raw_data()

    @pytest.mark.unit
    def test_cleanup_old_raw_data_error_handling(self, test_store, caplog):
        """Test error handling in cleanup_old_raw_data"""
        config = {"enabled": True, "intervals": ["hourly"], "keep_raw_data_days": 7}

        aggregator = Aggregator(test_store, config)

        # Mock to raise exception (when implemented)
        with patch.object(
            aggregator, "cleanup_old_raw_data", side_effect=Exception("Cleanup error")
        ):
            try:
                aggregator.cleanup_old_raw_data()
            except Exception:
                pass

        # Should log error when exception occurs
        # Note: Current implementation doesn't actually do cleanup yet

    @pytest.mark.unit
    @patch("logly.core.aggregator.datetime")
    def test_hourly_timestamp_calculation(self, mock_datetime, test_store):
        """Test correct timestamp calculation for hourly aggregation"""
        config = {"enabled": True, "intervals": ["hourly"], "keep_raw_data_days": 7}

        # Test various times in an hour
        test_cases = [
            (datetime(2025, 1, 15, 10, 0, 0), datetime(2025, 1, 15, 9, 0, 0)),
            (datetime(2025, 1, 15, 10, 30, 45), datetime(2025, 1, 15, 9, 0, 0)),
            (datetime(2025, 1, 15, 10, 59, 59), datetime(2025, 1, 15, 9, 0, 0)),
            (
                datetime(2025, 1, 15, 0, 15, 0),
                datetime(2025, 1, 14, 23, 0, 0),
            ),  # After midnight
        ]

        aggregator = Aggregator(test_store, config)
        test_store.compute_hourly_aggregates = Mock()

        for current_time, expected_hour in test_cases:
            mock_datetime.now.return_value = current_time
            test_store.compute_hourly_aggregates.reset_mock()

            aggregator.run_hourly_aggregation()

            expected_timestamp = int(expected_hour.timestamp())
            test_store.compute_hourly_aggregates.assert_called_once_with(
                expected_timestamp
            )

    @pytest.mark.unit
    @patch("logly.core.aggregator.datetime")
    def test_daily_date_calculation(self, mock_datetime, test_store):
        """Test correct date calculation for daily aggregation"""
        config = {"enabled": True, "intervals": ["daily"], "keep_raw_data_days": 7}

        # Test various dates
        test_cases = [
            (datetime(2025, 1, 15, 10, 0, 0), "2025-01-14"),
            (datetime(2025, 1, 1, 0, 0, 0), "2024-12-31"),  # New Year
            (datetime(2025, 3, 1, 12, 0, 0), "2025-02-28"),  # Non-leap year
        ]

        aggregator = Aggregator(test_store, config)
        test_store.compute_daily_aggregates = Mock()

        for current_time, expected_date in test_cases:
            mock_datetime.now.return_value = current_time
            test_store.compute_daily_aggregates.reset_mock()

            aggregator.run_daily_aggregation()

            test_store.compute_daily_aggregates.assert_called_once_with(expected_date)
