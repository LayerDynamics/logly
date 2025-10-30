"""
Unit tests for logly.cli module
Tests command-line interface functionality
"""

import pytest
import sys
from unittest.mock import patch, Mock, MagicMock

from logly import cli


class TestCLI:
    """Test suite for CLI commands"""

    @pytest.mark.unit
    @patch('logly.cli.Scheduler')
    @patch('logly.cli.SQLiteStore')
    @patch('logly.cli.Config')
    @patch('logly.cli.setup_logging')
    @patch('time.sleep', side_effect=KeyboardInterrupt)
    def test_cmd_start(self, mock_sleep, mock_setup_log, mock_config, mock_store, mock_scheduler):
        """Test start command"""
        mock_config_instance = Mock()
        mock_config_instance.get_database_config.return_value = {'path': '/tmp/test.db'}
        mock_config.return_value = mock_config_instance

        mock_store_instance = Mock()
        mock_store.return_value = mock_store_instance

        mock_scheduler_instance = Mock()
        mock_scheduler.return_value = mock_scheduler_instance

        args = Mock()
        args.config = None

        with pytest.raises(KeyboardInterrupt):
            cli.cmd_start(args)

        mock_scheduler_instance.start.assert_called_once()

    @pytest.mark.unit
    @patch('logly.cli.Scheduler')
    @patch('logly.cli.SQLiteStore')
    @patch('logly.cli.Config')
    @patch('logly.cli.setup_logging')
    def test_cmd_collect(self, mock_setup_log, mock_config, mock_store, mock_scheduler):
        """Test collect command"""
        mock_config_instance = Mock()
        mock_config_instance.get_database_config.return_value = {'path': '/tmp/test.db'}
        mock_config.return_value = mock_config_instance

        mock_store_instance = Mock()
        mock_store.return_value = mock_store_instance

        mock_scheduler_instance = Mock()
        mock_scheduler.return_value = mock_scheduler_instance

        args = Mock()
        args.config = None

        cli.cmd_collect(args)

        mock_scheduler_instance.run_once.assert_called_once()

    @pytest.mark.unit
    @patch('logly.cli.SQLiteStore')
    @patch('logly.cli.Config')
    @patch('builtins.print')
    def test_cmd_status(self, mock_print, mock_config, mock_store):
        """Test status command"""
        mock_config_instance = Mock()
        mock_config_instance.get_database_config.return_value = {'path': '/tmp/test.db'}
        mock_config.return_value = mock_config_instance

        mock_store_instance = Mock()
        mock_store_instance.get_stats.return_value = {
            'database_size_mb': 5.25,
            'system_metrics': 1000,
            'network_metrics': 500,
            'log_events': 2000,
            'hourly_aggregates': 24,
            'daily_aggregates': 7
        }
        mock_store.return_value = mock_store_instance

        args = Mock()
        args.config = None

        cli.cmd_status(args)

        assert mock_print.called
