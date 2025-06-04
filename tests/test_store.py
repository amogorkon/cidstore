import asyncio

import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio

import pytest

pytestmark = pytest.mark.asyncio


@pytest.mark.xfail(
    reason="Sorted/unsorted region logic not implemented", raises=AttributeError
)
async def test_sorted_unsorted_region(directory):
    """Insert values and check sorted/unsorted region logic per spec 3."""
    key = E.from_str("sorttest")
    for i in range(1, 11):
        await directory.insert(key, E(i))
    assert await directory.get_sorted_count(key) >= 0
    # Optionally, check sorted region is sorted
    sorted_region = await directory.get_sorted_region(key)
    assert sorted_region == sorted(sorted_region)


@pytest.mark.xfail(reason="directory_type not implemented", raises=AttributeError)
async def test_directory_resize_and_migration(tree):
    # Insert enough keys to trigger directory resize/migration per spec 3
    for i in range(1, 10001):
        await tree.insert(E.from_str(f"dir{i}"), E(i))
    dtype = await tree.directory_type()
    assert dtype in ("attribute", "dataset")
    # If migration occurred, validate all keys are still present
    if dtype == "dataset":
        for i in range(10000):
            result = await tree.get(E.from_str(f"dir{i}"))
            assert len(result) == 1
            assert int(result[0]) == i
    # TODO: If migration/maintenance API is available, check atomic switch and data integrity


@pytest.mark.xfail(reason="has_spill_pointer not implemented", raises=AttributeError)
async def test_spill_pointer_and_large_valueset(tree):
    # Insert enough values to trigger ValueSet spill (external dataset)
    key = E.from_str("spill")
    for i in range(1, 1001):
        await tree.insert(key, E(i))
    assert await tree.has_spill_pointer(key)


@pytest.mark.xfail(reason="get_state_mask not implemented", raises=AttributeError)
async def test_state_mask_ecc(tree):
    key = E.from_str("ecc")
    for i in range(1, 5):
        await tree.insert(key, E(i))
    await tree.delete(key)
    mask = await tree.get_state_mask(key)
    assert isinstance(mask, int)
    # Optionally, inject bitflip and check correction
    await tree.check_and_correct_state_mask(key)
    # Spec 2: ECC-protected state mask must correct single-bit errors and detect double-bit errors
    mask = await tree.get_state_mask(key)
    orig_mask = mask
    for bit in range(8):
        corrupted = orig_mask ^ (1 << bit)
        await tree.set_state_mask(key, corrupted)
        await tree.check_and_correct_state_mask(key)
        assert await tree.get_state_mask(key) == orig_mask
    # Inject double-bit error and check detection (should not correct)
    corrupted = orig_mask ^ 0b11
    await tree.set_state_mask(key, corrupted)
    await tree.check_and_correct_state_mask(key)
    assert await tree.get_state_mask(key) != orig_mask


async def test_btree_initialization(tree):
    assert tree is not None


async def test_btree_insert(tree):
    await tree.insert(E.from_str("key"), E(123))
    result = await tree.lookup(E.from_str("key"))
    assert len(result) == 1
    assert int(result[0]) == 123


async def test_btree_delete(tree):
    await tree.insert(E.from_str("key"), E(123))
    await tree.delete(E.from_str("key"))
    result = await tree.lookup(E.from_str("key"))
    assert result == []


# Test cases for btree.py


# 3. Multi-value key promotion (mocked logic)
async def test_multi_value_promotion(tree):
    # Insert the same key many times to trigger promotion logic
    key = E.from_str("multi")
    for i in range(1, 201):
        await tree.insert(key, E(i))
    # Should still be able to look up all values (mocked logic)
    # (In real impl, would check value-list dataset)
    result = await tree.lookup(key)
    assert 200 in [int(x) for x in result]


@pytest.mark.xfail(reason="Split/merge logic not implemented", raises=AttributeError)
async def test_split_and_merge(directory):
    """Insert enough keys to trigger a split; merging should restore invariants (Spec 3, 6)."""
    for i in range(1, directory.SPLIT_THRESHOLD + 2):
        await directory.insert(E.from_str(f"split{i}"), E(i))
    new_dir, sep = await directory.split()
    assert await directory.validate()
    assert await new_dir.validate()
    assert await directory.size() <= directory.SPLIT_THRESHOLD
    assert await new_dir.size() <= directory.SPLIT_THRESHOLD


# ---
# TODOs for full Spec 3 compliance:
# - Add tests for cache stats, version attributes, and migration tool if APIs exist
# - Add tests for sharded directory and hybrid approach if/when implemented
# - Add tests for metrics/logging/tracing if exposed
# - Document any missing features or APIs as not covered


@pytest.mark.xfail(reason="compact not implemented", raises=AttributeError)
async def test_deletion_and_gc(directory):
    """Insert, delete, and check GC/compaction per spec 7."""
    for i in range(1, 11):
        await directory.insert(E.from_str(f"delgc{i}"), E(i))
    for i in range(1, 11):
        await directory.delete(E.from_str(f"delgc{i}"))
    for i in range(1, 11):
        result = await directory.lookup(E.from_str(f"delgc{i}"))
        assert result == []
    for i in range(1, 11):
        await directory.compact(E.from_str(f"delgc{i}"))


async def test_concurrent_insert(directory):
    """Simulate concurrent inserts and check for correct multi-value behavior (Spec 8)."""
    results = []

    async def writer():
        await directory.insert(E.from_str("swmr"), E(123))
        results.append(await directory.lookup(E.from_str("swmr")))

    await asyncio.gather(writer(), writer())
    for r in results:
        assert 123 in [int(x) for x in r]


# Test cases for btree.py


@pytest.mark.parametrize(
    "key,value",
    [
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("d", 4),
        ("e", 5),
    ],
)
@pytest.mark.asyncio
async def test_insert_and_lookup(tree, key, value):
    await tree.insert(E.from_str(key), E(value))
    result = await tree.lookup(E.from_str(key))
    assert len(result) == 1
    # Accept either E or int/str for test compatibility
    assert int(result[0]) == value


async def test_overwrite(tree):
    await tree.insert(E.from_str("dup"), E(1))
    await tree.insert(E.from_str("dup"), E(2))
    result = await tree.lookup(E.from_str("dup"))
    # Multi-value: both values should be present
    assert set(result) == {E(1), E(2)}


async def test_delete(tree):
    await tree.insert(E.from_str("x"), E(42))
    result = await tree.lookup(E.from_str("x"))
    assert len(result) == 1
    assert int(result[0]) == 42
    await tree.delete(E.from_str("x"))
    result = await tree.lookup(E.from_str("x"))
    assert result == []


async def test_multiple_inserts_and_deletes(directory):
    items = [(E.from_str(f"k{i}"), E(i)) for i in range(1, 11)]
    for k, v in items:
        await directory.insert(k, v)
    for k, v in items:
        result = await directory.lookup(k)
        assert len(result) == 1
        assert int(result[0]) == int(v)
    for k, _ in items:
        await directory.delete(k)
    for k, _ in items:
        result = await directory.lookup(k)
        assert result == []


async def test_nonexistent_key(directory):
    """Lookup for a key that was never inserted should return an empty result."""
    result = await directory.lookup(E.from_str("notfound"))
    assert result == []


async def test_bulk_insert(directory):
    for i in range(1, 101):
        await directory.insert(E.from_str(f"bulk{i}"), E(i))
    for i in range(1, 101):
        result = await directory.lookup(E.from_str(f"bulk{i}"))
        assert len(result) == 1
        assert int(result[0]) == i


async def test_bulk_delete(directory):
    for i in range(1, 101):
        await directory.insert(E.from_str(f"del{i}"), E(i))
    for i in range(1, 101):
        await directory.delete(E.from_str(f"del{i}"))
    for i in range(1, 101):
        result = await directory.lookup(E.from_str(f"del{i}"))
        assert result == []


@pytest.mark.asyncio
async def test_concurrent_writes(tmp_path):
    # Test concurrent inserts of distinct keys
    storage = Storage(path=Path(tmp_path) / "swmr.h5")
    wal = WAL(path=None)
    tree = CIDStore(storage, wal)

    async def worker(start, results):
        for i in range(start + 1, start + 51):
            k = E.from_str(f"key{i}")
            await tree.insert(k, E(i))
            # verify immediate lookup
            vals = await tree.get(k)
            results.append((k, list(vals)))

    results = []
    await asyncio.gather(
        worker(0, results),
        worker(50, results),
    )

    # Assert all inserted keys present with correct values
    for k, vals in results:
        i = int(str(k)[str(k).find("key") + 3 :]) if "key" in str(k) else None
        if i is not None:
            assert E(i) in vals
