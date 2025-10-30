"""
Unit tests for logly.collectors.base_collector module
Tests the abstract base collector class
"""

import pytest
from unittest.mock import Mock
from abc import ABC, abstractmethod

from logly.collectors.base_collector import BaseCollector


class TestBaseCollector:
    """Test suite for BaseCollector abstract class"""

    @pytest.mark.unit
    def test_base_collector_is_abstract(self):
        """Test that BaseCollector is abstract and cannot be instantiated directly"""
        config = {"enabled": True}

        # Should not be able to instantiate abstract class
        with pytest.raises(TypeError):
            BaseCollector(config)  # type: ignore[abstract]  # type: ignore[abstract]

    @pytest.mark.unit
    def test_concrete_implementation(self):
        """Test concrete implementation of BaseCollector"""

        # Create a concrete implementation for testing
        class TestCollector(BaseCollector):
            def collect(self):
                return "test_data"

        config = {"enabled": True, "test_param": "value"}
        collector = TestCollector(config)

        assert collector.config == config
        assert collector.enabled
        assert collector.collect() == "test_data"

    @pytest.mark.unit
    def test_init_with_disabled_config(self):
        """Test initialization with disabled configuration"""

        class TestCollector(BaseCollector):
            def collect(self):
                return None

        config = {"enabled": False}
        collector = TestCollector(config)

        assert not collector.enabled

    @pytest.mark.unit
    def test_init_with_missing_enabled_key(self):
        """Test initialization when 'enabled' key is missing"""

        class TestCollector(BaseCollector):
            def collect(self):
                return None

        config = {"other_param": "value"}  # No 'enabled' key
        collector = TestCollector(config)

        # Should default to True
        assert collector.enabled

    @pytest.mark.unit
    def test_is_enabled_method(self):
        """Test is_enabled method"""

        class TestCollector(BaseCollector):
            def collect(self):
                return None

        # Test with enabled=True
        config_enabled = {"enabled": True}
        collector_enabled = TestCollector(config_enabled)
        assert collector_enabled.is_enabled()

        # Test with enabled=False
        config_disabled = {"enabled": False}
        collector_disabled = TestCollector(config_disabled)
        assert not collector_disabled.is_enabled()

    @pytest.mark.unit
    def test_validate_method_default(self):
        """Test default validate method returns True"""

        class TestCollector(BaseCollector):
            def collect(self):
                return None

        config = {"enabled": True}
        collector = TestCollector(config)

        # Default implementation should return True
        assert collector.validate()

    @pytest.mark.unit
    def test_validate_method_override(self):
        """Test overriding validate method"""

        class TestCollector(BaseCollector):
            def collect(self):
                return None

            def validate(self):
                # Custom validation logic
                return self.config.get("valid", False)

        # Test with valid=True
        config_valid = {"enabled": True, "valid": True}
        collector_valid = TestCollector(config_valid)
        assert collector_valid.validate()

        # Test with valid=False
        config_invalid = {"enabled": True, "valid": False}
        collector_invalid = TestCollector(config_invalid)
        assert not collector_invalid.validate()

    @pytest.mark.unit
    def test_collect_method_must_be_implemented(self):
        """Test that collect method must be implemented by subclasses"""

        class IncompleteCollector(BaseCollector):
            # Intentionally not implementing collect method
            pass

        # Should not be able to instantiate without implementing collect
        with pytest.raises(TypeError):
            IncompleteCollector({"enabled": True})  # type: ignore[abstract]

    @pytest.mark.unit
    def test_multiple_inheritance(self):
        """Test that collector can be used with multiple inheritance"""

        class MixinClass:
            def mixin_method(self):
                return "mixin"

        class MultiCollector(BaseCollector, MixinClass):
            def collect(self):
                return f"collected_{self.mixin_method()}"

        config = {"enabled": True}
        collector = MultiCollector(config)

        assert collector.collect() == "collected_mixin"
        assert collector.mixin_method() == "mixin"

    @pytest.mark.unit
    def test_config_access_in_subclass(self):
        """Test that config is accessible in subclass methods"""

        class ConfigCollector(BaseCollector):
            def collect(self):
                return self.config.get("data_source", "default")

            def get_custom_param(self):
                return self.config.get("custom_param", None)

        config = {"enabled": True, "data_source": "test_source", "custom_param": 42}

        collector = ConfigCollector(config)

        assert collector.collect() == "test_source"
        assert collector.get_custom_param() == 42

    @pytest.mark.unit
    def test_state_management_in_collector(self):
        """Test that collectors can maintain internal state"""

        class StatefulCollector(BaseCollector):
            def __init__(self, config):
                super().__init__(config)
                self.collection_count = 0

            def collect(self):
                self.collection_count += 1
                return f"collection_{self.collection_count}"

        config = {"enabled": True}
        collector = StatefulCollector(config)

        assert collector.collect() == "collection_1"
        assert collector.collect() == "collection_2"
        assert collector.collection_count == 2

    @pytest.mark.unit
    def test_error_handling_in_collect(self):
        """Test error handling in collect method"""

        class ErrorCollector(BaseCollector):
            def __init__(self, config):
                super().__init__(config)
                self.should_error = config.get("should_error", False)

            def collect(self):
                if self.should_error:
                    raise ValueError("Collection error")
                return "success"

        # Test normal operation
        config_normal = {"enabled": True, "should_error": False}
        collector_normal = ErrorCollector(config_normal)
        assert collector_normal.collect() == "success"

        # Test error condition
        config_error = {"enabled": True, "should_error": True}
        collector_error = ErrorCollector(config_error)

        with pytest.raises(ValueError):
            collector_error.collect()

    @pytest.mark.unit
    def test_async_collect_pattern(self):
        """Test that collector can be extended for async patterns"""
        import asyncio

        class AsyncCollector(BaseCollector):
            def collect(self):
                # Synchronous collect for compatibility
                return asyncio.run(self.async_collect())

            async def async_collect(self):
                # Simulate async operation
                await asyncio.sleep(0)
                return "async_data"

        config = {"enabled": True}
        collector = AsyncCollector(config)

        result = collector.collect()
        assert result == "async_data"
