#!/usr/bin/env python3
"""Test background merge/sort functionality - simplified version."""

import asyncio
import threading
import time

import pytest

from cidstore.maintenance import BackgroundMaintenance, MaintenanceConfig


class MockStore:
    """Mock store for testing BackgroundMaintenance."""

    def __init__(self):
        self.sort_calls = 0
        self.merge_calls = 0
        self.adaptive_calls = 0

    async def run_adaptive_maintenance(self):
        self.adaptive_calls += 1
        await asyncio.sleep(0.001)  # Simulate work

    def _fake_hdf_context(self):
        """Mock HDF5 context that doesn't trigger actual file operations."""
        return self


def test_background_maintenance_class():
    """Test BackgroundMaintenance class initialization and basic operations."""
    print("🔧 Testing BackgroundMaintenance class...")

    # Import the class

    # Create mock store
    mock_store = MockStore()

    # Test initialization
    config = MaintenanceConfig(
        maintenance_interval=1, sort_threshold=5, merge_threshold=3
    )
    maintenance = BackgroundMaintenance(mock_store, config)

    # Test configuration
    assert maintenance.config.maintenance_interval == 1
    assert maintenance.config.sort_threshold == 5
    assert maintenance.config.merge_threshold == 3
    assert maintenance.store == mock_store
    assert isinstance(maintenance.stop_event, threading.Event)

    print("✅ BackgroundMaintenance initialization working")

    # Test stats
    stats = maintenance.get_stats()
    expected_keys = {
        "running",
        "last_run",
        "interval",
        "sort_threshold",
        "merge_threshold",
    }
    assert set(stats.keys()) == expected_keys
    assert stats["running"]  # Should be running initially
    assert stats["last_run"] == 0  # Not run yet

    print("✅ Statistics tracking working")

    # Test stop
    maintenance.stop()
    time.sleep(0.1)  # Give time to stop
    stats_after_stop = maintenance.get_stats()
    assert not stats_after_stop["running"]

    print("✅ Stop functionality working")


@pytest.mark.asyncio
async def test_maintenance_cycle_components():
    """Test individual maintenance cycle components."""
    print("🔄 Testing maintenance cycle components...")

    # Create maintenance instance
    mock_store = MockStore()
    config = MaintenanceConfig(
        maintenance_interval=1, sort_threshold=1, merge_threshold=1
    )
    maintenance = BackgroundMaintenance(mock_store, config)

    # Stop the background thread
    maintenance.stop()

    # Test _run_maintenance_cycle components
    # This should call adaptive maintenance
    await maintenance._run_maintenance_cycle()

    # Verify adaptive maintenance was called
    assert mock_store.adaptive_calls == 1
    print("✅ Adaptive maintenance integration working")


def test_background_thread_lifecycle():
    """Test the background thread starts and stops properly."""
    print("🧵 Testing background thread lifecycle...")

    mock_store = MockStore()
    config = MaintenanceConfig(maintenance_interval=1)
    maintenance = BackgroundMaintenance(mock_store, config)  # Fast interval for testing

    # Start the thread manually
    maintenance.start()
    time.sleep(0.1)  # Give it time to start

    # Should be running now
    assert maintenance.is_alive()
    print("✅ Background thread starts properly")

    # Let it run briefly
    time.sleep(0.2)

    # Stop it
    maintenance.stop()

    # Give it time to stop
    start_time = time.time()
    stopped = False
    while (time.time() - start_time) < 1.0:
        if not maintenance.is_alive():
            stopped = True
            break
        time.sleep(0.1)

    assert stopped, "Background thread took longer to stop"
    print("✅ Background thread stops cleanly")


def test_configuration_parameters():
    """Test different configuration parameters."""
    print("⚙️ Testing configuration parameters...")

    mock_store = MockStore()

    # Test custom parameters
    config = MaintenanceConfig(
        maintenance_interval=60, sort_threshold=32, merge_threshold=16
    )
    maintenance = BackgroundMaintenance(mock_store, config)

    stats = maintenance.get_stats()
    assert stats["interval"] == 60
    assert stats["sort_threshold"] == 32
    assert stats["merge_threshold"] == 16

    print("✅ Custom configuration parameters working")

    # Test default parameters
    maintenance_default = BackgroundMaintenance(mock_store, MaintenanceConfig())
    stats_default = maintenance_default.get_stats()
    assert stats_default["interval"] == 30
    assert stats_default["sort_threshold"] == 16
    assert stats_default["merge_threshold"] == 8

    print("✅ Default configuration parameters working")

    # Cleanup
    maintenance.stop()
    maintenance_default.stop()
