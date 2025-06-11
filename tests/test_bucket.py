"""
Bucket structure and split/merge tests for the canonical hash directory model (Spec 2, 3, 6).
All tests are TDD-style and implementation-agnostic.
"""

import pytest

pytestmark = pytest.mark.asyncio


@pytest.mark.xfail(reason="Bucket split logic not implemented")
async def test_bucket_split(store):
    """Inserting enough entries should trigger a split; check split invariants."""
    from cidstore.keys import E

    for i in range(store.SPLIT_THRESHOLD + 1):
        await store.insert(E.from_str(f"k{i}"), E(i))
    new_bucket, sep = await store.split()
    assert await store.validate()
    assert await new_bucket.validate()
    assert await store.size() <= store.SPLIT_THRESHOLD
    assert await new_bucket.size() <= store.SPLIT_THRESHOLD


@pytest.mark.xfail(reason="Sorted/unsorted region logic not implemented")
async def test_sorted_unsorted_region_logic(store):
    """Test sorted/unsorted region logic per spec 3 (placeholder if not implemented)."""
    from cidstore.keys import E

    for i in range(10):
        await store.insert(E.from_str(f"srt{i}"), E(i))
    sorted_count = await store.get_sorted_count()
    assert 0 <= sorted_count <= await store.size()
    # Optionally, check that the sorted region is actually sorted
    sorted_region = await store.get_sorted_region()
    assert sorted_region == sorted(sorted_region)


async def test_bucket_structure_and_types(store):
    """Check that the bucket structure matches canonical data types (Spec 2)."""
    from cidstore.keys import E

    await store.insert(E.from_str("bigkey"), E(1 << 64))
    entry = await store.get_entry(E.from_str("bigkey"))
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "checksum" in entry
    assert isinstance(entry["key_high"], int)
    assert entry["key_high"] == 1


async def test_directory_entry_structure(directory):
    """Check that directory entries match canonical structure (Spec 2)."""
    from cidstore.keys import E

    await directory.insert(E.from_str("dirkey"), E(123))
    entry = await directory.get_entry(E.from_str("dirkey"))
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "checksum" in entry
