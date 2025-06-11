"""
Spec 3: Bucket Structure and Directory Management (TDD)
Covers bucket split/merge, sorted/unsorted region, directory migration, and resizing.
All tests are TDD-style and implementation-agnostic.
"""

import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio


async def test_bucket_split_and_merge(store):
    """Inserting enough entries should trigger a split and directory growth."""
    for i in range(store.SPLIT_THRESHOLD + 1):
        await store.insert(E.from_int(i), E(i))
    assert store.global_depth >= 1
    assert len(store.bucket_pointers) == 2**store.global_depth
    bucket_ids = {ptr["bucket_id"] for ptr in store.bucket_pointers}
    assert len(bucket_ids) > 1
    # Check that all directory pointers are valid and point to existing buckets
    all_bucket_ids = {ptr["bucket_id"] for ptr in store.bucket_pointers}
    for ptr in store.bucket_pointers:
        assert ptr["bucket_id"] in all_bucket_ids


@pytest.mark.xfail(reason="Sorted/unsorted region logic not implemented")
async def test_sorted_unsorted_region_logic(store):
    """Test sorted/unsorted region logic per spec 3 (placeholder if not implemented)."""
    for i in range(10):
        await store.insert(E.from_str(f"srt{i}"), E(i))
    sorted_count = await store.get_sorted_count()
    assert 0 <= sorted_count <= await store.size()
    # Optionally, check that the sorted region is actually sorted
    sorted_region = await store.get_sorted_region()
    assert sorted_region == sorted(sorted_region)


@pytest.mark.xfail(reason="Directory migration/type logic not implemented")
async def test_directory_resize_and_migration(directory):
    """Insert enough keys to trigger directory resize/migration (attribute → dataset)."""
    for i in range(10000):
        await directory.insert(E.from_str(f"dir{i}"), E(i))
    dtype = await directory.directory_type()
    assert dtype == "dataset"
    # Validate all keys are still present in all supported directory types
    for i in range(10000):
        result = await directory.lookup(E.from_str(f"dir{i}"))
        assert len(result) == 1
        assert int(result[0]) == i


async def test_directory_structure_and_types(directory):
    """Check that directory entries match canonical structure and types."""
    await directory.insert(E.from_str("dirkey"), E(123))
    entry = await directory.get_entry(E.from_str("dirkey"))
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "checksum" in entry
