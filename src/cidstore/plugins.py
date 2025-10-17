"""Plugin infrastructure for predicate specialization.

Provides explicit plugin registration system for specialized data structures.
Per Spec 13: Plugin Infrastructure.
"""

from __future__ import annotations

from typing import Any, Dict, List, Type

from .keys import E
from .predicates import SpecializedDataStructure


class PluginRegistry:
    """Registry for predicate plugin implementations.

    Manages explicit registration and instantiation of specialized data structures.
    No dynamic discovery - all plugins must be explicitly registered.

    Per Spec 13: Plugin Infrastructure
    """

    def __init__(self):
        """Initialize empty plugin registry."""
        self.plugins: Dict[str, Type[SpecializedDataStructure]] = {}

    def register(self, name: str, plugin_class: Type[SpecializedDataStructure]) -> None:
        """Register a plugin implementation.

        Args:
            name: Unique plugin identifier (e.g., 'counter', 'multivalue_set')
            plugin_class: Class implementing SpecializedDataStructure interface

        Raises:
            ValueError: If plugin name already registered
            TypeError: If plugin_class doesn't inherit from SpecializedDataStructure
        """
        if name in self.plugins:
            raise ValueError(f"Plugin '{name}' already registered")

        if not issubclass(plugin_class, SpecializedDataStructure):
            raise TypeError(
                f"Plugin class must inherit from SpecializedDataStructure, "
                f"got {plugin_class.__name__}"
            )

        self.plugins[name] = plugin_class

    def create_instance(
        self, name: str, predicate: E, config: Dict[str, Any]
    ) -> SpecializedDataStructure:
        """Create plugin instance for a predicate.

        Args:
            name: Plugin name (must be registered)
            predicate: Predicate CID this instance will handle
            config: Plugin-specific configuration dict

        Returns:
            Initialized SpecializedDataStructure instance

        Raises:
            ValueError: If plugin name not registered
        """
        if name not in self.plugins:
            raise ValueError(
                f"Unknown plugin: '{name}'. Available: {self.list_plugins()}"
            )

        plugin_class = self.plugins[name]
        return plugin_class(predicate, **config)

    def list_plugins(self) -> List[str]:
        """List all registered plugin names.

        Returns:
            List of plugin names in registration order
        """
        return list(self.plugins.keys())

    def get_plugin_class(self, name: str) -> Type[SpecializedDataStructure]:
        """Get plugin class by name.

        Args:
            name: Plugin name

        Returns:
            Plugin class

        Raises:
            ValueError: If plugin not registered
        """
        if name not in self.plugins:
            raise ValueError(f"Unknown plugin: '{name}'")
        return self.plugins[name]


def create_default_registry() -> PluginRegistry:
    """Create PluginRegistry with built-in plugins registered.

    Registers:
        - counter: CounterStore for integer counters
        - multivalue_set: MultiValueSetStore for deduplicated sets

    Returns:
        PluginRegistry with built-in plugins
    """
    from .predicates import CounterStore, MultiValueSetStore

    registry = PluginRegistry()
    registry.register("counter", CounterStore)
    registry.register("multivalue_set", MultiValueSetStore)

    # Future plugins can be registered here when implemented:
    # registry.register("timeseries", TimeSeriesStore)
    # registry.register("geospatial", GeospatialStore)
    # registry.register("fulltext", FullTextStore)

    return registry
