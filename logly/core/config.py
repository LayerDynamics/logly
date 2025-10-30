"""
Configuration management for Logly
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from logly.utils.logger import get_logger
from logly.utils.paths import get_db_path, get_logs_dir, get_project_root


logger = get_logger(__name__)


class Config:
    """Configuration manager for Logly"""

    # HARDCODED: Only look for config in the project's config directory
    DEFAULT_CONFIG_PATHS = [
        str(get_project_root() / 'config' / 'logly.yaml'),
    ]

    DEFAULT_CONFIG = {
        'database': {
            'path': str(get_db_path()),  # HARDCODED - cannot be changed
            'retention_days': 90,
        },
        'collection': {
            'system_metrics': 60,
            'network_metrics': 60,
            'log_parsing': 300,
        },
        'system': {
            'enabled': True,
            'metrics': [
                'cpu_percent', 'cpu_count', 'memory_total',
                'memory_available', 'memory_percent', 'disk_usage',
                'disk_io', 'load_average'
            ],
        },
        'network': {
            'enabled': True,
            'metrics': [
                'bytes_sent', 'bytes_recv', 'packets_sent',
                'packets_recv', 'connections', 'listening_ports'
            ],
        },
        'logs': {
            'enabled': True,
            'sources': {
                'fail2ban': {
                    'path': '/var/log/fail2ban.log',
                    'enabled': True,
                },
                'syslog': {
                    'path': '/var/log/syslog',
                    'enabled': True,
                },
                'auth': {
                    'path': '/var/log/auth.log',
                    'enabled': True,
                },
            },
        },
        'aggregation': {
            'enabled': True,
            'intervals': ['hourly', 'daily'],
            'keep_raw_data_days': 7,
        },
        'export': {
            'default_format': 'csv',
            'timestamp_format': '%Y-%m-%d %H:%M:%S',
        },
        'logging': {
            'log_dir': str(get_logs_dir()),  # HARDCODED - cannot be changed
        },
    }

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration

        Args:
            config_path: Optional path to config file. If None, searches default paths.
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or use defaults"""
        config_file = None

        if self.config_path:
            # Use specified config path
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.warning(f"Config file not found: {self.config_path}")
                config_file = None
        else:
            # Search default paths
            for path in self.DEFAULT_CONFIG_PATHS:
                expanded = Path(path).expanduser()
                if expanded.exists():
                    config_file = expanded
                    logger.info(f"Using config file: {config_file}")
                    break

        if config_file:
            try:
                with open(config_file, 'r') as f:
                    loaded_config = yaml.safe_load(f)
                    # Merge with defaults (loaded config takes precedence)
                    config = self._deep_merge(self.DEFAULT_CONFIG.copy(), loaded_config)

                    # ENFORCE HARDCODED PATHS - cannot be overridden by config file
                    # Exception: In test mode, allow custom paths from config
                    if os.environ.get("LOGLY_TEST_MODE") != "1":
                        config['database']['path'] = str(get_db_path())
                        config['logging']['log_dir'] = str(get_logs_dir())

                    return config
            except Exception as e:
                logger.error(f"Error loading config file {config_file}: {e}")
                logger.info("Using default configuration")

        # Use default config
        logger.info("Using default configuration")
        return self.DEFAULT_CONFIG.copy()

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """
        Deep merge two dictionaries

        Args:
            base: Base dictionary
            override: Override dictionary

        Returns:
            Merged dictionary
        """
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated path

        Args:
            key_path: Dot-separated path (e.g., 'database.path')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self.config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.config.get('database', {})

    def get_collection_config(self) -> Dict[str, Any]:
        """Get collection intervals configuration"""
        return self.config.get('collection', {})

    def get_system_config(self) -> Dict[str, Any]:
        """Get system metrics configuration"""
        return self.config.get('system', {})

    def get_network_config(self) -> Dict[str, Any]:
        """Get network metrics configuration"""
        return self.config.get('network', {})

    def get_logs_config(self) -> Dict[str, Any]:
        """Get log parsing configuration"""
        return self.config.get('logs', {})

    def get_aggregation_config(self) -> Dict[str, Any]:
        """Get aggregation configuration"""
        return self.config.get('aggregation', {})

    def get_export_config(self) -> Dict[str, Any]:
        """Get export configuration"""
        return self.config.get('export', {})

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.config.get('logging', {})
