"""
Test inline slots implementation (Spec 2 compliance).
"""

from pathlib import Path

import h5py
import pytest

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.utils import assumption
from cidstore.wal import WAL


@pytest.mark.asyncio
async def test_inline_slots_two_values(temp_dir):
    """Test that exactly 2 values can be stored inline."""
    hdf5_path = Path(temp_dir) / "test_slots.h5"
    wal_path = Path(temp_dir) / "test.wal"

    storage = Storage(str(hdf5_path))
    wal = WAL(wal_path)
    store = CIDStore(storage, wal)
    await store.async_init()

    key = E(123)
    value1 = E(456)
    value2 = E(789)

    try:
        # Insert first value
        await store.insert(key, value1)
        values = await store.get(key)
        print(f"[DEBUG] After first insert, values={values}")
        assert len(values) == 1
        assert value1 in values

        # Insert second value - should still be inline
        await store.insert(key, value2)
        values = await store.get(key)
        print(f"[DEBUG] After second insert, values={values}")
        assert len(values) == 2
        assert value1 in values
        assert value2 in values

        # Verify both values are stored inline (not spilled)
        bucket_id = store._bucket_name_and_id(key.high, key.low)[1]
        bucket_name = f"bucket_{bucket_id:04d}"

        bucket_group = store.hdf["/buckets"]
        assert assumption(bucket_group, h5py.Group)
        assert bucket_name in bucket_group.keys()
        bucket = bucket_group[bucket_name]
        assert assumption(bucket, h5py.Dataset)
        found = False
        for entry in bucket:
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                slots = entry["slots"].tolist()  # Fix: convert to list for comparison
                # Use tuple for value checks and correct non-zero slot logic
                non_zero_slots = [s for s in slots if s[0] != 0 or s[1] != 0]
                assert len(non_zero_slots) == 2
                assert (value1.high, value1.low) in slots
                assert (value2.high, value2.low) in slots
                found = True
                break
        assert found, "Entry not found in bucket"
    finally:
        if hasattr(store, "aclose"):
            await store.aclose()
        elif hasattr(store, "close"):
            store.close()
        if hasattr(wal, "close"):
            try:
                wal.close()
            except ValueError:
                pass
        if hasattr(storage, "close"):
            try:
                storage.close()
            except RuntimeError:
                pass


@pytest.mark.asyncio
async def test_inline_slots_promotion_to_spill(temp_dir):
    """Test that third value triggers promotion to spill ValueSet."""
    hdf5_path = Path(temp_dir) / "test_slots_spill.h5"
    wal_path = Path(temp_dir) / "test.wal"

    storage = Storage(str(hdf5_path))
    wal = WAL(wal_path)
    store = CIDStore(storage, wal)
    await store.async_init()

    key = E(123)
    value1 = E(456)
    value2 = E(789)
    value3 = E(999)

    try:
        # Insert two values - should be inline
        await store.insert(key, value1)
        await store.insert(key, value2)

        # Insert third value - should trigger spill
        await store.insert(key, value3)

        # All three values should be retrievable
        values = await store.get(key)
        print(f"[DEBUG] After third insert, values={values}")
        assert len(values) == 3
        assert value1 in values
        assert value2 in values
        assert value3 in values

        # Verify that values are now in a spilled ValueSet
        bucket_id = store._bucket_name_and_id(key.high, key.low)[1]
        bucket_name = f"bucket_{bucket_id:04d}"
        bucket_group = store.hdf["/buckets"]
        assert assumption(bucket_group, h5py.Group)
        assert bucket_name in bucket_group.keys()
        bucket = bucket_group[bucket_name]
        assert assumption(bucket, h5py.Dataset)
        entry_found = False
        for entry in bucket:
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                slots = entry["slots"].tolist()
                # Verify spilled ValueSet exists
                sp_group = store._get_valueset_group(store.hdf)
                assert assumption(sp_group, h5py.Group)
                ds_name = store._get_spill_ds_name(bucket_id, key)
                assert ds_name in sp_group.keys()
                spill_ds = sp_group[ds_name]
                assert assumption(spill_ds, h5py.Dataset)
                spilled_values = [E(v) for v in spill_ds[:] if v != 0]
                assert len(spilled_values) == 3
                assert value1 in spilled_values
                assert value2 in spilled_values
                assert value3 in spilled_values
                entry_found = True
                break
        assert entry_found, "Entry not found in bucket"
    finally:
        if hasattr(store, "aclose"):
            await store.aclose()
        elif hasattr(store, "close"):
            store.close()
        if hasattr(wal, "close"):
            try:
                wal.close()
            except ValueError:
                pass
        if hasattr(storage, "close"):
            try:
                storage.close()
            except RuntimeError:
                pass


@pytest.mark.asyncio
async def test_inline_slots_demotion_from_spill(temp_dir):
    """Test that removing values can demote back to inline storage."""
    hdf5_path = Path(temp_dir) / "test_slots_demote.h5"
    wal_path = Path(temp_dir) / "test.wal"

    storage = Storage(str(hdf5_path))
    wal = WAL(wal_path)
    store = CIDStore(storage, wal)
    await store.async_init()

    key = E(123)
    value1 = E(456)
    value2 = E(789)
    value3 = E(999)

    try:
        # Insert three values to trigger spill
        await store.insert(key, value1)
        await store.insert(key, value2)
        await store.insert(key, value3)

        # Verify spill occurred
        values = await store.get(key)
        print(f"[DEBUG] After three inserts, values={values}")
        assert len(values) == 3

        # Remove one value to go back to 2 values
        assert hasattr(store, "delete_value")
        await store.delete_value(key, value3)

        # Should now have 2 values
        values = await store.get(key)
        print(f"[DEBUG] After delete, values={values}")
        assert len(values) == 2
        assert value1 in values
        assert value2 in values
        assert value3 not in values

        # Verify demotion to inline
        bucket_id = store._bucket_name_and_id(key.high, key.low)[1]
        bucket_name = f"bucket_{bucket_id:04d}"
        bucket_group = store.hdf["/buckets"]
        assert assumption(bucket_group, h5py.Group)
        assert bucket_name in bucket_group.keys()
        bucket = bucket_group[bucket_name]
        assert assumption(bucket, h5py.Dataset)
        found = False
        for entry in bucket:
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                slots = entry["slots"].tolist()
                assert len(slots) == 2
                non_zero_slots = [s for s in slots if s[0] != 0 or s[1] != 0]
                assert len(non_zero_slots) == 1 or len(non_zero_slots) == 2
                assert (value1.high, value1.low) in slots
                assert (value2.high, value2.low) in slots
                found = True
                break
        assert found, "Entry not found in bucket"
    finally:
        if hasattr(store, "aclose"):
            await store.aclose()
        elif hasattr(store, "close"):
            store.close()
        if hasattr(wal, "close"):
            try:
                wal.close()
            except ValueError:
                pass
        if hasattr(storage, "close"):
            try:
                storage.close()
            except RuntimeError:
                pass


@pytest.mark.asyncio
async def test_inline_slots_empty_handling(temp_dir):
    """Test handling of empty slots."""
    hdf5_path = Path(temp_dir) / "test_slots_empty.h5"
    wal_path = Path(temp_dir) / "test.wal"

    storage = Storage(str(hdf5_path))
    wal = WAL(wal_path)
    store = CIDStore(storage, wal)
    await store.async_init()

    key = E(123)
    value1 = E(456)

    try:
        # Insert one value
        await store.insert(key, value1)
        values = await store.get(key)
        print(f"[DEBUG] After insert, values={values}")
        assert len(values) == 1
        assert value1 in values

        # Verify slot structure
        bucket_id = store._bucket_name_and_id(key.high, key.low)[1]
        bucket_name = f"bucket_{bucket_id:04d}"
        bucket_group = store.hdf["/buckets"]
        assert assumption(bucket_group, h5py.Group)
        assert bucket_name in bucket_group.keys()
        bucket = bucket_group[bucket_name]
        assert assumption(bucket, h5py.Dataset)
        found = False
        for entry in bucket:
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                slots = entry["slots"].tolist()
                assert len(slots) == 2
                non_zero_slots = [s for s in slots if s[0] != 0 or s[1] != 0]
                assert len(non_zero_slots) == 1
                assert (value1.high, value1.low) in slots
                found = True
                break
        assert found, "Entry not found in bucket"
    finally:
        if hasattr(store, "aclose"):
            await store.aclose()
        elif hasattr(store, "close"):
            store.close()
        if hasattr(wal, "close"):
            try:
                wal.close()
            except ValueError:
                pass
        if hasattr(storage, "close"):
            try:
                storage.close()
            except RuntimeError:
                pass


# Removed manual test runner block; use pytest to run these tests with fixtures.
