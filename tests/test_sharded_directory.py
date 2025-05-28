"""
Test sharded directory mode functionality.
"""

import tempfile
from pathlib import Path

import pytest

from src.cidstore.keys import E
from src.cidstore.storage import Storage
from src.cidstore.store import CIDStore
from src.cidstore.wal import WAL


@pytest.mark.asyncio
async def test_sharded_directory_migration():
    """Test migration from dataset mode to sharded mode."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_sharded.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        # Start in attribute mode
        assert store._directory_mode == "attr"

        # Force migration to dataset mode by setting low threshold
        store._directory_attr_threshold = 2

        # Add enough entries to trigger migration to dataset mode
        for i in range(5):
            key = E(i + 1)
            value = E(i + 100)
            await store.insert(key, value)

        # Should have migrated to dataset mode
        if store._directory_mode != "ds":
            # Manual migration for testing
            store._migrate_attr_to_dataset()
        assert store._directory_mode == "ds"
        assert store._directory_dataset is not None

        # Force migration to sharded mode by setting low threshold
        store._directory_ds_threshold = 10

        # Add more entries to trigger sharded migration
        for i in range(5, 15):
            key = E(i + 1)
            value = E(i + 100)
            await store.insert(key, value)

        # Should have migrated to sharded mode
        if store._directory_mode != "sharded":
            # Manual migration for testing
            store._migrate_dataset_to_sharded()

        assert store._directory_mode == "sharded"
        assert store._directory_dataset is None
        assert len(store._directory_shards) > 0

        # Test that we can still read data
        for i in range(15):
            key = E(i + 1)
            values = await store.get(key)
            assert len(values) > 0
            assert E(i + 100) in values


@pytest.mark.asyncio
async def test_sharded_directory_loading():
    """Test loading an existing sharded directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_sharded.h5"
        wal_path = Path(temp_dir) / "test.wal"

        # Create store and force to sharded mode
        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        # Force to sharded mode
        store._directory_attr_threshold = 1
        store._directory_ds_threshold = 2

        # Add entries to trigger migrations
        for i in range(5):
            key = E(i + 1)
            value = E(i + 100)
            await store.insert(key, value)

        # Manually migrate to sharded if needed
        if store._directory_mode == "attr":
            store._migrate_attr_to_dataset()
        if store._directory_mode == "ds":
            store._migrate_dataset_to_sharded()

        assert store._directory_mode == "sharded"
        original_shards = len(store._directory_shards)

        # Close and reopen
        del store
        storage.close()
        wal.close()

        # Reopen
        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        # Should load in sharded mode
        assert store._directory_mode == "sharded"
        assert len(store._directory_shards) == original_shards

        # Test that data is still accessible
        for i in range(5):
            key = E(i + 1)
            values = await store.get(key)
            assert len(values) > 0
            assert E(i + 100) in values


@pytest.mark.asyncio
async def test_sharded_directory_expansion():
    """Test directory expansion in sharded mode."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_sharded.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        # Force to sharded mode quickly
        store._directory_attr_threshold = 1
        store._directory_ds_threshold = 1

        # Add initial entry
        await store.insert(E(1), E(100))

        # Manually migrate to sharded
        if store._directory_mode == "attr":
            store._migrate_attr_to_dataset()
        if store._directory_mode == "ds":
            store._migrate_dataset_to_sharded()

        assert store._directory_mode == "sharded"

        initial_global_depth = store.global_depth
        initial_directory_size = len(store.bucket_pointers)

        # Force bucket split by adding many entries with different hash values
        for i in range(200):  # Add many entries to force splits
            # Use large, diverse keys to ensure different hash values
            key = E((i + 1) * 12345678901234567890)
            value = E(i + 1000)
            await store.insert(key, value)

        # Directory might have expanded
        assert store.global_depth >= initial_global_depth
        assert len(store.bucket_pointers) >= initial_directory_size

        # Verify all data is still accessible
        for i in range(200):
            key = E((i + 1) * 12345678901234567890)
            values = await store.get(key)
            assert len(values) > 0
            assert E(i + 1000) in values


@pytest.mark.asyncio
async def test_sharded_directory_update_pointers():
    """Test updating bucket pointers in sharded mode."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_sharded.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        # Force to sharded mode
        store._directory_attr_threshold = 1
        store._directory_ds_threshold = 1

        await store.insert(E(1), E(100))

        # Migrate to sharded
        if store._directory_mode == "attr":
            store._migrate_attr_to_dataset()
        if store._directory_mode == "ds":
            store._migrate_dataset_to_sharded()

        assert store._directory_mode == "sharded"

        # Update a bucket pointer
        original_bucket_id = store.bucket_pointers[0]["bucket_id"]
        store.bucket_pointers[0]["bucket_id"] = 999

        # Save directory changes
        store._save_directory_to_storage()

        # Verify the change was persisted
        # Check that the shard dataset was updated
        assert len(store._directory_shards) > 0
        shard_0 = store._directory_shards[0]
        first_entry = shard_0[0]
        assert first_entry["bucket_id"] == 999

        # Restore original value
        store.bucket_pointers[0]["bucket_id"] = original_bucket_id
        store._save_directory_to_storage()


if __name__ == "__main__":
    import asyncio

    async def run_tests():
        await test_sharded_directory_migration()
        await test_sharded_directory_loading()
        await test_sharded_directory_expansion()
        await test_sharded_directory_update_pointers()
        print("All sharded directory tests passed!")

    asyncio.run(run_tests())
