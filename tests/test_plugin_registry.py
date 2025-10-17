"""Tests for PluginRegistry (Spec 13: Plugin Infrastructure).

Tests explicit plugin registration system with no dynamic discovery.
"""

import pytest

from cidstore.keys import E
from cidstore.plugins import PluginRegistry, create_default_registry
from cidstore.predicates import (
    CounterStore,
    MultiValueSetStore,
    SpecializedDataStructure,
)


class MockPlugin(SpecializedDataStructure):
    """Mock plugin for testing."""

    def __init__(self, predicate: E, custom_param: str = "default"):
        super().__init__(predicate, supports_osp=True, supports_pos=False)
        self.custom_param = custom_param

    async def insert(self, subject: E, obj):
        pass

    async def query_spo(self, subject: E):
        return None

    async def query_osp(self, obj):
        return set()

    async def query_pos(self, obj):
        return set()

    def audit_performance(self):
        return {"custom_param": self.custom_param}


def test_plugin_registry_creation():
    """Test creating empty PluginRegistry."""
    registry = PluginRegistry()
    assert len(registry.list_plugins()) == 0
    assert registry.plugins == {}


def test_register_plugin():
    """Test registering a plugin class."""
    registry = PluginRegistry()
    registry.register("mock", MockPlugin)

    assert "mock" in registry.list_plugins()
    assert registry.get_plugin_class("mock") == MockPlugin


def test_register_duplicate_plugin_raises():
    """Test that registering duplicate plugin name raises ValueError."""
    registry = PluginRegistry()
    registry.register("mock", MockPlugin)

    with pytest.raises(ValueError, match="already registered"):
        registry.register("mock", MockPlugin)


def test_register_non_plugin_raises():
    """Test that registering non-SpecializedDataStructure raises TypeError."""
    registry = PluginRegistry()

    class NotAPlugin:
        pass

    with pytest.raises(TypeError, match="must inherit from SpecializedDataStructure"):
        registry.register("bad", NotAPlugin)


def test_create_instance():
    """Test creating plugin instance with config."""
    registry = PluginRegistry()
    registry.register("mock", MockPlugin)

    predicate = E(1, 2)
    config = {"custom_param": "test_value"}

    instance = registry.create_instance("mock", predicate, config)

    assert isinstance(instance, MockPlugin)
    assert instance.predicate == predicate
    assert instance.custom_param == "test_value"


def test_create_instance_unknown_plugin_raises():
    """Test that creating instance of unknown plugin raises ValueError."""
    registry = PluginRegistry()

    with pytest.raises(ValueError, match="Unknown plugin"):
        registry.create_instance("nonexistent", E(1, 2), {})


def test_create_instance_default_config():
    """Test creating instance with empty config uses defaults."""
    registry = PluginRegistry()
    registry.register("mock", MockPlugin)

    instance = registry.create_instance("mock", E(1, 2), {})

    assert instance.custom_param == "default"


def test_list_plugins():
    """Test listing all registered plugins."""
    registry = PluginRegistry()
    registry.register("plugin1", MockPlugin)
    registry.register("plugin2", MockPlugin)

    plugins = registry.list_plugins()

    assert len(plugins) == 2
    assert "plugin1" in plugins
    assert "plugin2" in plugins


def test_get_plugin_class():
    """Test retrieving plugin class by name."""
    registry = PluginRegistry()
    registry.register("mock", MockPlugin)

    plugin_class = registry.get_plugin_class("mock")

    assert plugin_class == MockPlugin


def test_get_plugin_class_unknown_raises():
    """Test that getting unknown plugin class raises ValueError."""
    registry = PluginRegistry()

    with pytest.raises(ValueError, match="Unknown plugin"):
        registry.get_plugin_class("nonexistent")


def test_create_default_registry():
    """Test create_default_registry includes built-in plugins."""
    registry = create_default_registry()

    plugins = registry.list_plugins()

    assert "counter" in plugins
    assert "multivalue_set" in plugins
    assert registry.get_plugin_class("counter") == CounterStore
    assert registry.get_plugin_class("multivalue_set") == MultiValueSetStore


def test_create_default_registry_counter_instance():
    """Test creating CounterStore instance from default registry."""
    registry = create_default_registry()
    predicate = E(10, 20)

    instance = registry.create_instance("counter", predicate, {})

    assert isinstance(instance, CounterStore)
    assert instance.predicate == predicate
    assert instance.supports_osp is True
    assert instance.supports_pos is True


def test_create_default_registry_multivalue_instance():
    """Test creating MultiValueSetStore instance from default registry."""
    registry = create_default_registry()
    predicate = E(30, 40)

    instance = registry.create_instance("multivalue_set", predicate, {})

    assert isinstance(instance, MultiValueSetStore)
    assert instance.predicate == predicate
    assert instance.supports_osp is True
    assert instance.supports_pos is True


def test_plugin_registry_isolation():
    """Test that separate PluginRegistry instances are isolated."""
    registry1 = PluginRegistry()
    registry2 = PluginRegistry()

    registry1.register("mock", MockPlugin)

    assert "mock" in registry1.list_plugins()
    assert "mock" not in registry2.list_plugins()


def test_register_preserves_order():
    """Test that list_plugins returns plugins in registration order."""
    registry = PluginRegistry()
    registry.register("plugin_a", MockPlugin)
    registry.register("plugin_b", MockPlugin)
    registry.register("plugin_c", MockPlugin)

    plugins = registry.list_plugins()

    assert plugins == ["plugin_a", "plugin_b", "plugin_c"]
