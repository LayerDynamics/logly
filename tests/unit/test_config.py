"""
Unit tests for logly.core.config module
Tests configuration loading, merging, and access methods
"""

import pytest
from unittest.mock import patch, mock_open
from pathlib import Path
import yaml

from logly.core.config import Config


class TestConfig:
    """Test suite for Config class"""

    @pytest.mark.unit
    def test_init_with_default_config(self):
        """Test Config initialization with default configuration"""
        with patch("logly.core.config.Path.exists", return_value=False):
            config = Config()

            # Verify default configuration is loaded
            assert config.config is not None
            assert "database" in config.config
            assert "collection" in config.config
            assert config.config["database"]["retention_days"] == 90

    @pytest.mark.unit
    def test_init_with_custom_config_file(self, temp_config_file):
        """Test Config initialization with custom config file"""
        config = Config(config_path=temp_config_file)

        # Verify custom config is loaded
        assert config.config_path == temp_config_file
        assert config.config["database"]["retention_days"] == 7  # From test config

    @pytest.mark.unit
    def test_init_with_nonexistent_config_file(self, caplog):
        """Test Config initialization with non-existent config file"""
        fake_path = "/nonexistent/config.yaml"
        config = Config(config_path=fake_path)

        # Should fall back to default config
        assert "Config file not found" in caplog.text
        assert config.config["database"]["retention_days"] == 90  # Default value

    @pytest.mark.unit
    def test_load_config_with_yaml_error(self, temp_dir):
        """Test Config loading with invalid YAML"""
        bad_yaml_path = temp_dir / "bad.yaml"
        bad_yaml_path.write_text("invalid: yaml: content:")

        with patch(
            "logly.core.config.yaml.safe_load",
            side_effect=yaml.YAMLError("Invalid YAML"),
        ):
            config = Config(config_path=str(bad_yaml_path))

            # Should fall back to default config
            assert config.config == config.DEFAULT_CONFIG

    @pytest.mark.unit
    def test_hardcoded_paths_enforcement(self, temp_config_file):
        """Test that hardcoded paths cannot be overridden in production mode"""
        import os

        # Create config with different paths
        custom_config = """
database:
  path: "/custom/path/db.db"
logging:
  log_dir: "/custom/logs"
"""
        config_path = Path(temp_config_file).parent / "custom.yaml"
        config_path.write_text(custom_config)

        # Temporarily disable test mode to test production behavior
        original_test_mode = os.environ.get("LOGLY_TEST_MODE")
        if "LOGLY_TEST_MODE" in os.environ:
            del os.environ["LOGLY_TEST_MODE"]

        try:
            with patch(
                "logly.core.config.get_db_path", return_value=Path("/hardcoded/db.db")
            ):
                with patch(
                    "logly.core.config.get_logs_dir", return_value=Path("/hardcoded/logs")
                ):
                    config = Config(config_path=str(config_path))

                    # Hardcoded paths should override config file in production mode
                    assert config.config["database"]["path"] == str(
                        Path("/hardcoded/db.db")
                    )
                    assert config.config["logging"]["log_dir"] == str(
                        Path("/hardcoded/logs")
                    )
        finally:
            # Restore test mode
            if original_test_mode is not None:
                os.environ["LOGLY_TEST_MODE"] = original_test_mode

    @pytest.mark.unit
    def test_get_method(self):
        """Test Config.get() method with various key paths"""
        config = Config()

        # Test simple key
        assert config.get("database") is not None

        # Test nested key path
        assert config.get("database.retention_days") == 90

        # Test non-existent key with default
        assert config.get("nonexistent.key", "default") == "default"

        # Test deep nested path
        assert config.get("logs.sources.fail2ban.enabled", False)

    @pytest.mark.unit
    def test_get_database_config(self):
        """Test get_database_config method"""
        config = Config()
        db_config = config.get_database_config()

        assert "path" in db_config
        assert "retention_days" in db_config
        assert isinstance(db_config["retention_days"], int)

    @pytest.mark.unit
    def test_get_collection_config(self):
        """Test get_collection_config method"""
        config = Config()
        collection_config = config.get_collection_config()

        assert "system_metrics" in collection_config
        assert "network_metrics" in collection_config
        assert "log_parsing" in collection_config
        assert collection_config["system_metrics"] == 60

    @pytest.mark.unit
    def test_get_system_config(self):
        """Test get_system_config method"""
        config = Config()
        system_config = config.get_system_config()

        assert "enabled" in system_config
        assert "metrics" in system_config
        assert isinstance(system_config["metrics"], list)
        assert "cpu_percent" in system_config["metrics"]

    @pytest.mark.unit
    def test_get_network_config(self):
        """Test get_network_config method"""
        config = Config()
        network_config = config.get_network_config()

        assert "enabled" in network_config
        assert "metrics" in network_config
        assert "bytes_sent" in network_config["metrics"]

    @pytest.mark.unit
    def test_get_logs_config(self):
        """Test get_logs_config method"""
        config = Config()
        logs_config = config.get_logs_config()

        assert "enabled" in logs_config
        assert "sources" in logs_config
        assert "fail2ban" in logs_config["sources"]

    @pytest.mark.unit
    def test_get_aggregation_config(self):
        """Test get_aggregation_config method"""
        config = Config()
        agg_config = config.get_aggregation_config()

        assert "enabled" in agg_config
        assert "intervals" in agg_config
        assert "hourly" in agg_config["intervals"]
        assert "keep_raw_data_days" in agg_config

    @pytest.mark.unit
    def test_get_export_config(self):
        """Test get_export_config method"""
        config = Config()
        export_config = config.get_export_config()

        assert "default_format" in export_config
        assert "timestamp_format" in export_config
        assert export_config["default_format"] == "csv"

    @pytest.mark.unit
    def test_get_logging_config(self):
        """Test get_logging_config method"""
        config = Config()
        logging_config = config.get_logging_config()

        assert "log_dir" in logging_config

    @pytest.mark.unit
    def test_deep_merge(self):
        """Test _deep_merge method"""
        config = Config()

        base = {"a": 1, "b": {"c": 2, "d": 3}, "e": [1, 2, 3]}

        override = {"a": 10, "b": {"c": 20, "f": 4}, "g": 5}

        result = config._deep_merge(base, override)

        assert result["a"] == 10  # Overridden
        assert result["b"]["c"] == 20  # Nested override
        assert result["b"]["d"] == 3  # Original preserved
        assert result["b"]["f"] == 4  # New nested key
        assert result["e"] == [1, 2, 3]  # Original list preserved
        assert result["g"] == 5  # New key

    @pytest.mark.unit
    def test_default_config_paths(self):
        """Test that default config paths are searched correctly"""
        with patch("logly.core.config.get_project_root", return_value=Path("/project")):
            with patch("logly.core.config.Path.exists") as mock_exists:
                # Simulate first path exists
                mock_exists.side_effect = [True, False]

                with patch(
                    "builtins.open",
                    mock_open(read_data="database:\n  retention_days: 30"),
                ):
                    Config()

                    # Should use first existing path
                    assert mock_exists.call_count >= 1

    @pytest.mark.unit
    def test_config_file_loading_priority(self, temp_dir):
        """Test that loaded config takes priority over defaults"""
        config_path = temp_dir / "priority.yaml"
        config_content = """
database:
  retention_days: 30
collection:
  system_metrics: 120
new_section:
  new_key: new_value
"""
        config_path.write_text(config_content)

        config = Config(config_path=str(config_path))

        # Loaded values should override defaults
        assert config.config["database"]["retention_days"] == 30
        assert config.config["collection"]["system_metrics"] == 120

        # New sections should be added
        assert config.config.get("new_section", {}).get("new_key") == "new_value"

        # Other defaults should remain
        assert config.config["collection"]["network_metrics"] == 60
