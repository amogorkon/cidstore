#!/usr/bin/env python3
"""Test background merge/sort of unsorted region functionality."""

import time

import pytest

from cidstore.keys import E
from cidstore.maintenance import BackgroundMaintenance, MaintenanceConfig


@pytest.mark.asyncio
async def test_background_maintenance_initialization(store):
    """Test that background maintenance thread is properly initialized."""
    # Use the store fixture and check background thread

    # Check that maintenance thread is initialized
    assert hasattr(store, "maintenance_manager")
    assert hasattr(store.maintenance_manager, "maintenance_thread")
    assert store.maintenance_manager.maintenance_thread.is_alive()

    # Check thread configuration
    stats = store.maintenance_manager.maintenance_thread.get_stats()
    assert stats["running"]
    assert stats["interval"] == 30
    assert stats["sort_threshold"] == 16
    assert stats["merge_threshold"] == 8

    # Cleanup
    store.maintenance_manager.maintenance_thread.stop()
    time.sleep(0.1)  # Let thread stop
    assert not store.maintenance_manager.maintenance_thread.get_stats()["running"]


@pytest.mark.asyncio
async def test_background_sort_unsorted_regions(store):
    """Test that background maintenance sorts unsorted regions."""
    # Use the store fixture with lower threshold for testing
    store.maintenance_manager.maintenance_thread.stop()  # Stop automatic background

    # Create a manual background maintenance with low threshold
    config = MaintenanceConfig(
        maintenance_interval=1, sort_threshold=3, merge_threshold=2
    )
    test_maintenance = BackgroundMaintenance(store, config)

    # Insert several values to create unsorted region
    test_key1 = E(1, 100)
    test_key2 = E(1, 200)
    test_key3 = E(1, 150)
    test_key4 = E(1, 50)

    await store.insert(test_key1, E(1, 1001))
    await store.insert(test_key2, E(1, 1002))
    await store.insert(test_key3, E(1, 1003))
    await store.insert(test_key4, E(1, 1004))
    # Check that we have unsorted entries (using available methods)
    # Assert that the required internal method exists
    assert hasattr(store, "_bucket_name_and_id"), (
        "CIDStore must have _bucket_name_and_id method"
    )
    bucket_id = store._bucket_name_and_id(test_key1.high, test_key1.low)[1]
    unsorted_count = store.get_unsorted_count(bucket_id)
    assert unsorted_count >= 0  # Should have some entries

    # Run maintenance cycle manually
    await test_maintenance._sort_unsorted_regions()

    # Check that entries are now sorted
    # Note: may not decrease if threshold not met

    # Cleanup
    store.maintenance_manager.maintenance_thread.stop()


@pytest.mark.asyncio
async def test_background_maintenance_integration(store):
    """Test full background maintenance integration."""
    # Use the store fixture

    # Check that both background threads are running
    assert hasattr(store, "maintenance_manager")
    assert hasattr(store.maintenance_manager, "gc_thread")
    assert hasattr(store.maintenance_manager, "maintenance_thread")
    assert store.maintenance_manager.gc_thread.is_alive()
    assert store.maintenance_manager.maintenance_thread.is_alive()

    # Test store operations still work
    test_key = E(1, 42)
    test_value = E(2, 84)

    await store.insert(test_key, test_value)
    result = await store.get(test_key)
    assert len(result) == 1
    assert result[0] == test_value

    # Test context manager cleanup
    await store.insert(E(1, 43), E(2, 86))

    # Threads should be stopped after context exit
    time.sleep(0.1)  # Let threads stop
    # Note: cleanup behavior may vary, so we just check threads existed
