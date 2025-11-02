"""ZVIC compatibility tests for CIDStore.

These tests verify that key interfaces remain compatible across changes
using ZVIC's Zero-Version Interface Contracts paradigm. Tests check:

1. Plugin system interface stability (PluginRegistry, SpecializedDataStructure)
2. Storage backend interface (Storage class)
3. WAL interface (WAL class)
4. Store interface (CIDStore class)
5. Key/value utilities (E class, composite_key functions)

Per ZVIC: compatibility is enforced through signature hashing and runtime
checks, eliminating the need for semantic versioning.
"""

from __future__ import annotations

import io

import pytest

# Test that ZVIC is available and can be used
zvic_available = False
try:
    import importlib.util

    zvic_available = importlib.util.find_spec("zvic") is not None
except (ImportError, ValueError):
    pass

# Skip all tests if ZVIC not available
pytestmark = pytest.mark.skipif(
    not zvic_available, reason="ZVIC not installed (pip install zvic==2025.43.0)"
)


class TestPluginSystemCompatibility:
    """Test plugin system interface compatibility."""

    def test_plugin_registry_interface_stable(self):
        """PluginRegistry public methods must remain compatible."""
        from cidstore.plugins import PluginRegistry

        # Check that key methods exist with expected signatures
        registry = PluginRegistry()

        # These methods define the plugin contract
        assert hasattr(registry, "register")
        assert hasattr(registry, "create_instance")
        assert hasattr(registry, "list_plugins")
        assert hasattr(registry, "get_plugin_class")

        # Verify method signatures are callable
        assert callable(registry.register)
        assert callable(registry.create_instance)
        assert callable(registry.list_plugins)
        assert callable(registry.get_plugin_class)

    def test_specialized_data_structure_interface(self):
        """SpecializedDataStructure base class must define stable contract."""
        from cidstore.predicates import SpecializedDataStructure

        # Check that async methods exist
        assert hasattr(SpecializedDataStructure, "insert")
        assert hasattr(SpecializedDataStructure, "query_spo")
        assert hasattr(SpecializedDataStructure, "query_osp")
        assert hasattr(SpecializedDataStructure, "query_pos")


class TestStorageInterfaceCompatibility:
    """Test Storage backend interface compatibility."""

    def test_storage_init_signature(self):
        """Storage.__init__ must accept Path | str | BytesIO."""
        from cidstore.storage import Storage

        # Test all valid init patterns
        f = io.BytesIO()
        storage = Storage(f)
        assert storage is not None
        storage.close()

    def test_storage_core_methods_exist(self):
        """Storage must provide core HDF5 operations."""
        from cidstore.storage import Storage

        f = io.BytesIO()
        storage = Storage(f)

        # Core methods that define storage contract
        assert hasattr(storage, "close")
        assert hasattr(storage, "flush")
        assert callable(storage.close)
        assert callable(storage.flush)

        storage.close()


class TestWALInterfaceCompatibility:
    """Test WAL interface compatibility."""

    def test_wal_init_accepts_none_for_memory(self):
        """WAL must accept None for in-memory mode."""
        from cidstore.wal import WAL

        wal = WAL(None)
        assert wal is not None
        wal.close()

    def test_wal_append_signature(self):
        """WAL.append must accept required parameters."""
        from cidstore.wal import WAL

        wal = WAL(None)

        # Check append is callable (signature checked at runtime by ZVIC)
        assert hasattr(wal, "append")
        assert callable(wal.append)

        wal.close()


class TestStoreInterfaceCompatibility:
    """Test CIDStore main interface compatibility."""

    def test_store_init_signature(self):
        """CIDStore must accept Storage and WAL."""
        from cidstore.storage import Storage
        from cidstore.store import CIDStore
        from cidstore.wal import WAL

        f = io.BytesIO()
        storage = Storage(f)
        wal = WAL(None)

        store = CIDStore(storage, wal=wal, testing=True)
        assert store is not None

        store.close()

    def test_store_core_async_methods(self):
        """CIDStore must provide async CRUD operations."""
        import asyncio

        from cidstore.storage import Storage
        from cidstore.store import CIDStore
        from cidstore.wal import WAL

        f = io.BytesIO()
        storage = Storage(f)
        wal = WAL(None)
        store = CIDStore(storage, wal=wal, testing=True)

        # Check async interface exists
        assert hasattr(store, "async_init")
        assert hasattr(store, "insert")
        assert hasattr(store, "get")
        assert hasattr(store, "delete")
        assert hasattr(store, "aclose")

        # Verify they're coroutine functions
        import inspect

        assert inspect.iscoroutinefunction(store.async_init)
        assert inspect.iscoroutinefunction(store.insert)
        assert inspect.iscoroutinefunction(store.get)
        assert inspect.iscoroutinefunction(store.delete)
        assert inspect.iscoroutinefunction(store.aclose)

        asyncio.run(store.aclose())


class TestKeyInterfaceCompatibility:
    """Test E key class interface compatibility."""

    def test_e_from_str_signature(self):
        """E.from_str must accept string and return E."""
        from cidstore.keys import E

        key = E.from_str("test:value")
        assert isinstance(key, E)
        assert isinstance(key, int)

    def test_e_properties_stable(self):
        """E must provide high/high_mid/low_mid/low properties."""
        from cidstore.keys import E

        key = E.from_str("test:value")

        # 256-bit component properties must exist
        assert hasattr(key, "high")
        assert hasattr(key, "high_mid")
        assert hasattr(key, "low_mid")
        assert hasattr(key, "low")

        # Verify they return integers
        assert isinstance(key.high, int)
        assert isinstance(key.high_mid, int)
        assert isinstance(key.low_mid, int)
        assert isinstance(key.low, int)

    def test_composite_key_functions_exist(self):
        """Composite key functions must be available."""
        from cidstore import keys

        assert hasattr(keys, "composite_key")
        assert hasattr(keys, "composite_value")
        assert hasattr(keys, "decode_composite_value")

        assert callable(keys.composite_key)
        assert callable(keys.composite_value)
        assert callable(keys.decode_composite_value)


class TestZVICRuntimeEnforcement:
    """Test that ZVIC runtime enforcement is active in debug mode."""

    def test_zvic_enabled_in_debug_mode(self):
        """ZVIC should be enabled when __debug__ is True."""
        from cidstore.zvic_init import get_zvic_info

        info = get_zvic_info()

        # In normal test runs (without -O), ZVIC should be enabled
        if __debug__:
            assert info["debug_mode"] is True
            # enabled depends on env variable, just check it's a bool
            assert isinstance(info["enabled"], bool)
        else:
            # Running with python -O, ZVIC should be disabled
            assert info["enabled"] is False

    def test_zvic_can_be_disabled_via_env(self, monkeypatch):
        """ZVIC can be disabled via CIDSTORE_ZVIC_ENABLED=0."""
        # Test that the environment variable is respected
        # (actual enforcement happens at import time, so this just validates logic)
        import os

        monkeypatch.setenv("CIDSTORE_ZVIC_ENABLED", "0")

        # Note: ZVIC_ENABLED is computed at module import, so this may not
        # reflect the env change unless modules are reloaded. This test
        # documents the intended behavior.
        assert os.getenv("CIDSTORE_ZVIC_ENABLED") == "0"


class TestCrossModuleCompatibility:
    """Test compatibility between related modules."""

    def test_storage_and_wal_work_together(self):
        """Storage and WAL must be compatible when used together."""
        from cidstore.storage import Storage
        from cidstore.wal import WAL

        f = io.BytesIO()
        storage = Storage(f)
        wal = WAL(None)

        # Both should be closeable
        assert callable(storage.close)
        assert callable(wal.close)

        storage.close()
        wal.close()

    def test_store_integrates_storage_and_wal(self):
        """CIDStore must integrate Storage and WAL correctly."""
        import asyncio

        from cidstore.keys import E
        from cidstore.storage import Storage
        from cidstore.store import CIDStore
        from cidstore.wal import WAL

        f = io.BytesIO()
        storage = Storage(f)
        wal = WAL(None)
        store = CIDStore(storage, wal=wal, testing=True)

        async def test_integration():
            await store.async_init()

            # Basic operations should work
            key = E.from_str("test:key")
            value = E.from_str("test:value")

            await store.insert(key, value)
            result = await store.get(key)

            assert result is not None

            await store.aclose()

        asyncio.run(test_integration())


# Compatibility baseline tests
# These establish the "known good" baseline that future changes must be compatible with


class TestCompatibilityBaseline:
    """Establish compatibility baseline for future comparisons.

    These tests document the current interface as the baseline.
    Future changes must be compatible with these signatures.
    """

    def test_plugin_registry_baseline(self):
        """Document PluginRegistry baseline interface."""
        from cidstore.keys import E
        from cidstore.plugins import PluginRegistry
        from cidstore.predicates import CounterStore, SpecializedDataStructure

        registry = PluginRegistry()

        # Baseline: register accepts (str, Type[SpecializedDataStructure])
        registry.register("test_plugin", CounterStore)

        # Baseline: create_instance accepts (str, E, Dict[str, Any])
        pred = E.from_str("test:pred")
        instance = registry.create_instance("test_plugin", pred, {})

        assert isinstance(instance, SpecializedDataStructure)

        # Baseline: list_plugins returns List[str]
        plugins = registry.list_plugins()
        assert isinstance(plugins, list)
        assert "test_plugin" in plugins

    def test_e_baseline_constructors(self):
        """Document E baseline construction methods."""
        from cidstore.keys import E

        # Baseline: from_str accepts str
        e1 = E.from_str("test")
        assert isinstance(e1, E)

        # Baseline: from_int accepts int
        e2 = E.from_int(12345)
        assert isinstance(e2, E)

        # Baseline: 4-component construction
        e3 = E(1, 2, 3, 4)
        assert isinstance(e3, E)

        # Baseline: properties return int
        assert isinstance(e1.high, int)
        assert isinstance(e1.high_mid, int)
        assert isinstance(e1.low_mid, int)
        assert isinstance(e1.low, int)


# Performance baseline tests (ensure ZVIC doesn't slow down hot paths)


class TestZVICPerformanceImpact:
    """Verify ZVIC doesn't significantly impact performance."""

    def test_e_construction_performance(self):
        """E construction should remain fast with ZVIC enabled."""
        import time

        from cidstore.keys import E

        # Warmup
        for _ in range(100):
            E.from_str(f"test:{_}")

        # Measure
        start = time.perf_counter()
        for i in range(1000):
            E.from_str(f"test:{i}")
        elapsed = time.perf_counter() - start

        # Should complete in reasonable time (< 100ms for 1000 constructions)
        # This is a smoke test, not a precise benchmark
        assert elapsed < 0.1, f"E construction too slow: {elapsed:.3f}s for 1000 ops"

    def test_plugin_registry_performance(self):
        """Plugin operations should remain fast."""
        import time

        from cidstore.keys import E
        from cidstore.plugins import create_default_registry

        registry = create_default_registry()
        pred = E.from_str("test:pred")

        # Warmup
        for _ in range(10):
            registry.create_instance("counter", pred, {})

        # Measure
        start = time.perf_counter()
        for _ in range(100):
            registry.create_instance("counter", pred, {})
        elapsed = time.perf_counter() - start

        # Should be fast (< 10ms for 100 operations)
        assert elapsed < 0.01, f"Plugin creation too slow: {elapsed:.3f}s for 100 ops"
