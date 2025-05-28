"""
Spec 3: Bucket Structure and Directory Management (TDD)
Covers bucket split/merge, sorted/unsorted region, directory migration, and resizing.
All tests are TDD-style and implementation-agnostic.
"""

import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio


async def test_bucket_split_and_merge(bucket):
    """Inserting enough entries should trigger a split and directory growth."""
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        await bucket.insert(E.from_int(i), E(i))
    assert bucket.global_depth >= 1
    assert len(bucket.bucket_pointers) == 2**bucket.global_depth
    bucket_ids = set(ptr["bucket_id"] for ptr in bucket.bucket_pointers)
    assert len(bucket_ids) > 1


@pytest.mark.xfail(reason="Sorted/unsorted region logic not implemented")
async def test_sorted_unsorted_region_logic(bucket):
    """Test sorted/unsorted region logic per spec 3 (placeholder if not implemented)."""
    for i in range(10):
        await bucket.insert(f"srt{i}", i)
    sorted_count = await bucket.get_sorted_count()
    assert 0 <= sorted_count <= await bucket.size()
    # Optionally, check that the sorted region is actually sorted
    sorted_region = await bucket.get_sorted_region()
    assert sorted_region == sorted(sorted_region)


@pytest.mark.xfail(reason="Directory migration/type logic not implemented")
async def test_directory_resize_and_migration(directory):
    """Insert enough keys to trigger directory resize/migration (attribute → dataset)."""
    for i in range(10000):
        await directory.insert(f"dir{i}", i)
    dtype = await directory.directory_type()
    assert dtype in ("attribute", "dataset")
    # If migration occurred, validate all keys are still present
    if dtype == "dataset":
        for i in range(10000):
            result = await directory.lookup(f"dir{i}")
            assert len(result) == 1
            assert int(result[0]) == i


async def test_directory_structure_and_types(directory):
    """Check that directory entries match canonical structure and types."""
    await directory.insert("dirkey", 123)
    entry = await directory.get_entry("dirkey")
    assert "key_high" in entry
    assert "value" in entry
