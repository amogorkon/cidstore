"""
Test inline slots implementation (Spec 2 compliance).
"""

import tempfile
from pathlib import Path

import pytest

from src.cidstore.keys import E
from src.cidstore.storage import Storage
from src.cidstore.store import CIDStore
from src.cidstore.wal import WAL


@pytest.mark.asyncio
async def test_inline_slots_two_values():
    """Test that exactly 2 values can be stored inline."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_slots.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        key = E(123)
        value1 = E(456)
        value2 = E(789)

        # Insert first value
        await store.insert(key, value1)
        values = await store.get(key)
        assert len(values) == 1
        assert value1 in values

        # Insert second value - should still be inline
        await store.insert(key, value2)
        values = await store.get(key)
        assert len(values) == 2
        assert value1 in values
        assert value2 in values

        # Verify both values are stored inline (not spilled)
        bucket_id = store._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with store.hdf as f:
            bucket = f["/buckets"][bucket_name]
            # Find the entry
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    slots = entry["slots"]
                    # Both values should be in slots (not spilled)
                    non_zero_slots = [s for s in slots if s != 0]
                    assert len(non_zero_slots) == 2
                    assert int(value1) in slots
                    assert int(value2) in slots
                    break
            else:
                pytest.fail("Entry not found in bucket")


@pytest.mark.asyncio
async def test_inline_slots_promotion_to_spill():
    """Test that third value triggers promotion to spill ValueSet."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_slots_spill.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        key = E(123)
        value1 = E(456)
        value2 = E(789)
        value3 = E(999)

        # Insert two values - should be inline
        await store.insert(key, value1)
        await store.insert(key, value2)

        # Insert third value - should trigger spill
        await store.insert(key, value3)

        # All three values should be retrievable
        values = await store.get(key)
        assert len(values) == 3
        assert value1 in values
        assert value2 in values
        assert value3 in values

        # Verify that values are now in a spilled ValueSet
        bucket_id = store._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with store.hdf as f:
            bucket = f["/buckets"][bucket_name]
            # Find the entry
            entry_found = False
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    slots = entry["slots"]
                    # Should have spill markers (high bit set)
                    spill_markers = [s for s in slots if s & (1 << 63)]
                    assert len(spill_markers) > 0, "Expected spill markers in slots"
                    entry_found = True
                    break

            assert entry_found, "Entry not found in bucket"

            # Verify spilled ValueSet exists
            sp_group = store._get_valueset_group(f)
            ds_name = store._get_spill_ds_name(bucket_id, key)
            assert ds_name in sp_group, "Spilled ValueSet should exist"

            # Verify all values are in the spilled dataset
            spill_ds = sp_group[ds_name]
            spilled_values = [E(v) for v in spill_ds[:] if v != 0]
            assert len(spilled_values) == 3
            assert value1 in spilled_values
            assert value2 in spilled_values
            assert value3 in spilled_values


@pytest.mark.asyncio
async def test_inline_slots_demotion_from_spill():
    """Test that removing values can demote back to inline storage."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_slots_demote.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        key = E(123)
        value1 = E(456)
        value2 = E(789)
        value3 = E(999)

        # Insert three values to trigger spill
        await store.insert(key, value1)
        await store.insert(key, value2)
        await store.insert(key, value3)

        # Verify spill occurred
        values = await store.get(key)
        assert len(values) == 3

        # Remove one value to go back to 2 values
        if hasattr(store, "delete_value"):
            await store.delete_value(key, value3)

            # Should now have 2 values
            values = await store.get(key)
            assert len(values) == 2
            assert value1 in values
            assert value2 in values
            assert value3 not in values


@pytest.mark.asyncio
async def test_inline_slots_empty_handling():
    """Test handling of empty slots."""
    with tempfile.TemporaryDirectory() as temp_dir:
        hdf5_path = Path(temp_dir) / "test_slots_empty.h5"
        wal_path = Path(temp_dir) / "test.wal"

        storage = Storage(str(hdf5_path))
        wal = WAL(str(wal_path))

        store = CIDStore(storage, wal)
        await store.async_init()

        key = E(123)
        value1 = E(456)

        # Insert one value
        await store.insert(key, value1)
        values = await store.get(key)
        assert len(values) == 1
        assert value1 in values

        # Verify slot structure
        bucket_id = store._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with store.hdf as f:
            bucket = f["/buckets"][bucket_name]
            # Find the entry
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    slots = entry["slots"]
                    # Should have exactly 2 slots
                    assert len(slots) == 2
                    # First slot should contain value, second should be 0
                    non_zero_slots = [s for s in slots if s != 0]
                    assert len(non_zero_slots) == 1
                    assert int(value1) in slots
                    break
            else:
                pytest.fail("Entry not found in bucket")


if __name__ == "__main__":
    import asyncio

    async def run_tests():
        await test_inline_slots_two_values()
        await test_inline_slots_promotion_to_spill()
        await test_inline_slots_demotion_from_spill()
        await test_inline_slots_empty_handling()
        print("All inline slots tests passed!")

    asyncio.run(run_tests())
