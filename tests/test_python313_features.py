"""Test Python 3.13 features in CIDStore.

This module tests Python 3.13 specific features and their backward compatibility.
"""

import sys

import pytest

from cidstore.maintenance import MaintenanceConfig


def test_copy_replace_feature():
    """Test copy.replace() for dataclass modification (Python 3.13 feature)."""
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
    # Import replace (with fallback for Python < 3.13)
    try:
        from copy import replace
    except ImportError:
        from dataclasses import replace

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
    """Verify Python version and print feature availability."""
    version = sys.version_info
    print(f"\nPython version: {version.major}.{version.minor}.{version.micro}")

    # Check for Python 3.13+ features
    has_copy_replace = False
    try:
        from copy import replace

        has_copy_replace = True
    except ImportError:
        pass

    has_finalization_error = hasattr(__builtins__, "PythonFinalizationError")

    print("Python 3.13 features:")
    print(
        f"  - copy.replace(): {'✅' if has_copy_replace else '❌ (using dataclasses.replace fallback)'}"
    )
    print(f"  - PythonFinalizationError: {'✅' if has_finalization_error else '❌'}")

    # Verify we can import and use the features
    from cidstore.maintenance import MaintenanceConfig

    config = MaintenanceConfig()
    test_config = config.with_testing_timeouts()
    assert test_config.gc_interval == 5


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires Python 3.13+")
def test_python313_specific_features():
    """Test features that only work on Python 3.13+."""
    # Verify it's the real copy.replace, not dataclasses.replace
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

    if sys.version_info >= (3, 13):
        test_python313_specific_features()
    else:
        print("\n⚠️  Skipping Python 3.13-specific tests (Python 3.13+ required)")

    print("\n" + "=" * 60)
    print("All tests passed! ✅")
