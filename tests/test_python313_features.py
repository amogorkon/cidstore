"""Test Python 3.13 features in CIDStore.

This module tests Python 3.13 specific features including free-threading and JIT.
Python 3.13+ is REQUIRED - no backward compatibility.
"""

import sys

import pytest

from cidstore.maintenance import MaintenanceConfig

# Verify Python 3.13+ requirement
if sys.version_info < (3, 13):
    pytest.skip("Python 3.13+ required", allow_module_level=True)


def test_copy_replace_feature():
    """Test copy.replace() for dataclass modification (Python 3.13 native feature)."""
    # Create a default config
    config = MaintenanceConfig()

    # Verify defaults
    assert config.gc_interval == 60
    assert config.maintenance_interval == 30
    assert config.thread_timeout == 1.0

    # Use the with_testing_timeouts method (which uses replace internally)
    test_config = config.with_testing_timeouts()

    # Verify the new config has modified values
    assert test_config.gc_interval == 5
    assert test_config.maintenance_interval == 5
    assert test_config.thread_timeout == 0.5
    assert test_config.wal_analysis_interval == 10

    # Verify original config is unchanged (immutability)
    assert config.gc_interval == 60
    assert config.maintenance_interval == 30
    assert config.thread_timeout == 1.0

    print("✅ copy.replace() works correctly for immutable dataclass updates")


def test_maintenance_config_replace():
    """Test manual replace usage on MaintenanceConfig."""
    from copy import replace  # Python 3.13 native - no fallback

    config = MaintenanceConfig(
        gc_interval=120, maintenance_interval=60, sort_threshold=32
    )

    # Create a modified config
    modified = replace(config, gc_interval=180, merge_threshold=16)

    # Verify changes
    assert modified.gc_interval == 180
    assert modified.merge_threshold == 16

    # Verify unchanged fields
    assert modified.maintenance_interval == 60
    assert modified.sort_threshold == 32

    # Verify original unchanged
    assert config.gc_interval == 120
    assert config.merge_threshold == 8  # default value

    print("✅ Manual replace() works correctly")


def test_python_version_check():
    """Verify Python version and runtime configuration."""
    version = sys.version_info
    print(f"\nPython version: {version.major}.{version.minor}.{version.micro}")

    # Verify Python 3.13 features are available
    from copy import replace

    assert replace is not None
    print("✅ copy.replace() available")

    # Check runtime configuration
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    jit_enabled = hasattr(sys, "_is_jit_enabled") and sys._is_jit_enabled()

    print(f"  - GIL enabled: {gil_enabled}")
    print(f"  - JIT enabled: {jit_enabled}")

    if not gil_enabled:
        print("  ✅ Free-threading mode active!")
    else:
        print("  ℹ️ Free-threading disabled (run with -X gil=0)")

    if jit_enabled:
        print("  ✅ JIT compiler active!")
    else:
        print("  ℹ️ JIT disabled (run with -X jit=1)")

    # Verify we can import and use the features
    config = MaintenanceConfig()
    test_config = config.with_testing_timeouts()
    assert test_config.gc_interval == 5


def test_python313_native_features():
    """Test features that use Python 3.13 native implementations."""
    # Verify it's the real copy.replace from copy module
    import copy
    from copy import replace

    assert hasattr(copy, "replace")

    # Test with MaintenanceConfig
    config = MaintenanceConfig(gc_interval=100)
    new_config = replace(config, gc_interval=200)

    assert new_config.gc_interval == 200
    assert config.gc_interval == 100

    print("✅ Native Python 3.13 copy.replace() confirmed")


def test_finalization_error_handling():
    """Test that PythonFinalizationError is handled gracefully."""
    # This test verifies the code doesn't crash when importing
    # modules that reference PythonFinalizationError
    from cidstore.store import CIDStore
    from cidstore.wal import WAL

    # These modules should import without error regardless of Python version
    assert WAL is not None
    assert CIDStore is not None

    print("✅ Finalization error handling imports successfully")


if __name__ == "__main__":
    # Run tests
    print("Testing Python 3.13 features in CIDStore\n")
    print("=" * 60)

    test_python_version_check()
    print("\n" + "=" * 60)

    test_copy_replace_feature()
    test_maintenance_config_replace()
    test_finalization_error_handling()
    test_python313_native_features()

    print("\n" + "=" * 60)
    print("All tests passed! ✅")
