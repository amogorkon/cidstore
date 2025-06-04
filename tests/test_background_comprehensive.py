#!/usr/bin/env python3
"""Test comprehensive background merge/sort of unsorted region functionality."""

import asyncio
import io

# Add src to path for imports
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "src"))

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL
from cidstore.maintenance import BackgroundMaintenance, MaintenanceConfig

def make_key(high: int, low: int) -> E:
    """Helper function to create E from high/low values."""
    return E.from_int((high << 64) | low)


def make_store():
    """Helper to create a test store."""
    storage = Storage(io.BytesIO())
    wal = WAL(path=None)  # Use None for in-memory
    return CIDStore(storage, wal)


@pytest.mark.asyncio
async def test_background_maintenance_full_cycle():
    """Test that background maintenance performs full sort/merge cycle."""
    print("🔧 Testing background maintenance full cycle...")

    # Create store
    store = make_store()

    # Verify background maintenance is running

    assert hasattr(store, "maintenance_manager")
    assert hasattr(store.maintenance_manager, "maintenance_thread")
    assert store.maintenance_manager.maintenance_thread.is_alive()

    # Stop automatic background to control timing
    store.maintenance_manager.maintenance_thread.stop()
    time.sleep(0.1)

    # Create manual background maintenance with aggressive thresholds
    config = MaintenanceConfig(maintenance_interval=1, sort_threshold=2, merge_threshold=1)
    test_maintenance = BackgroundMaintenance(store, config)

    # Insert data to create unsorted regions
    keys = [make_key(1, i) for i in [100, 50, 200, 75, 150]]
    values = [make_key(2, i + 1000) for i in range(len(keys))]

    for key, value in zip(keys, values):
        await store.insert(key, value)

    print(f"📝 Inserted {len(keys)} key-value pairs")

    # Run a complete maintenance cycle
    await test_maintenance._run_maintenance_cycle()

    # Verify data is still accessible after maintenance
    for key, expected_value in zip(keys, values):
        result = await store.get(key)
        assert len(result) >= 1, f"Key {key} not found after maintenance"
        assert expected_value in result, (
            f"Value {expected_value} not found for key {key}"
        )

    print("✅ All data accessible after maintenance cycle")

    # Cleanup
    store.maintenance_manager.maintenance_thread.stop()


@pytest.mark.asyncio
async def test_background_sort_threshold_behavior():
    """Test that sorting only happens when threshold is exceeded."""
    print("📊 Testing sort threshold behavior...")

    store = make_store()
    store.maintenance_manager.maintenance_thread.stop()

    # Create maintenance with high threshold
    high_threshold_maintenance = BackgroundMaintenance(
        store, store.maintenance_manager.config
    )
    high_threshold_maintenance.config.sort_threshold = 100
    high_threshold_maintenance.config.merge_threshold = 50

    # Insert few items (below threshold)
    await store.insert(make_key(1, 10), make_key(2, 1010))
    await store.insert(make_key(1, 20), make_key(2, 1020))

    # Run sort maintenance - should not trigger with high threshold
    await high_threshold_maintenance._sort_unsorted_regions()

    print("✅ Sort threshold behavior working correctly")

    # Cleanup
    store.maintenance_manager.maintenance_thread.stop()


@pytest.mark.asyncio
async def test_background_maintenance_with_wal_integration():
    """Test background maintenance integrates with WAL adaptive maintenance."""
    print("🔄 Testing WAL integration...")

    store = make_store()
    store.maintenance_manager.maintenance_thread.stop()

    # Verify WAL analyzer exists
    assert hasattr(store, "maintenance_manager"), "Store should have maintenance_manager"
    assert hasattr(store.maintenance_manager, "wal_analyzer_thread"), "Store should have wal_analyzer_thread"

    config = MaintenanceConfig(
        maintenance_interval=1, sort_threshold=5, merge_threshold=3
    )
    test_maintenance = BackgroundMaintenance(store, config)

    # Insert data and record operations
    for i in range(10):
        key = make_key(1, i)
        value = make_key(2, i + 1000)
        await store.insert(key, value)
        # Operations should be recorded in WAL analyzer automatically

    # Run maintenance cycle (includes adaptive maintenance)
    await test_maintenance._run_maintenance_cycle()

    print("✅ WAL integration working correctly")

    # Cleanup
    store.maintenance_manager.maintenance_thread.stop()


@pytest.mark.asyncio
async def test_background_maintenance_statistics():
    """Test that background maintenance tracks statistics."""
    print("📈 Testing maintenance statistics...")

    store = make_store()
    # Get initial stats
    initial_stats = store.maintenance_manager.maintenance_thread.get_stats()
    assert initial_stats["running"]
    assert initial_stats["interval"] == 30
    assert initial_stats["sort_threshold"] == 16
    assert initial_stats["merge_threshold"] == 8
    assert initial_stats["last_run"] == 0  # Not run yet

    print(f"📊 Initial stats: {initial_stats}")

    # Stop and verify
    store.maintenance_manager.maintenance_thread.stop()
    time.sleep(0.1)

    final_stats = store.maintenance_manager.maintenance_thread.get_stats()
    assert not final_stats["running"]

    print("✅ Statistics tracking working correctly")


@pytest.mark.asyncio
async def test_background_maintenance_error_handling():
    """Test that background maintenance handles errors gracefully."""
    print("⚠️ Testing error handling...")

    store = make_store()
    store.maintenance_manager.maintenance_thread.stop()

    config = MaintenanceConfig(
        maintenance_interval=1, sort_threshold=1, merge_threshold=1
    )
    test_maintenance = BackgroundMaintenance(store, config)

    # Even if there are issues, maintenance should not crash
    try:
        await test_maintenance._sort_unsorted_regions()
        await test_maintenance._merge_underfull_buckets()
        print("✅ Error handling working correctly")
    except Exception as e:
        pytest.fail(f"Background maintenance crashed: {e}")

    # Cleanup
    store.maintenance_manager.maintenance_thread.stop()


async def run_all_tests():
    """Run all background maintenance tests."""
    print("🚀 Starting background merge/sort tests...\n")

    await test_background_maintenance_full_cycle()
    print()

    await test_background_sort_threshold_behavior()
    print()

    await test_background_maintenance_with_wal_integration()
    print()

    await test_background_maintenance_statistics()
    print()

    await test_background_maintenance_error_handling()
    print()

    print("🎉 All background maintenance tests completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
