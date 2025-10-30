"""
Base collector abstract class
"""

from abc import ABC, abstractmethod
from typing import Any

from logly.utils.logger import get_logger


logger = get_logger(__name__)


class BaseCollector(ABC):
    """Abstract base class for all collectors"""

    def __init__(self, config: dict):
        """
        Initialize collector with configuration

        Args:
            config: Configuration dictionary for this collector
        """
        self.config = config
        self.enabled = config.get("enabled", True)

    @abstractmethod
    def collect(self) -> Any:
        """
        Collect data - must be implemented by subclasses

        Returns:
            Collected data (type depends on collector)
        """
        pass

    def is_enabled(self) -> bool:
        """Check if collector is enabled"""
        return self.enabled

    def validate(self) -> bool:
        """
        Validate collector can run (check permissions, file access, etc.)

        Returns:
            True if collector can run, False otherwise
        """
        return True
