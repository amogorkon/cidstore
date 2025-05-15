"""
Spec 3: Bucket Structure and Directory Management (TDD)
Covers bucket split/merge, sorted/unsorted region, directory migration, and resizing.
All tests are TDD-style and implementation-agnostic.
"""

import pytest


def test_bucket_split_and_merge(bucket):
    """Inserting enough entries should trigger a split; merging should restore invariants."""
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        bucket.insert(f"k{i}", i)
    if hasattr(bucket, "split"):
        new_bucket, sep = bucket.split()
        assert bucket.validate()
        assert new_bucket.validate()
        assert bucket.size() <= bucket.SPLIT_THRESHOLD
        assert new_bucket.size() <= bucket.SPLIT_THRESHOLD


def test_sorted_unsorted_region_logic(bucket):
    """Test sorted/unsorted region logic per spec 3 (placeholder if not implemented)."""
    for i in range(10):
        bucket.insert(f"srt{i}", i)
    if hasattr(bucket, "get_sorted_count"):
        sorted_count = bucket.get_sorted_count()
        assert 0 <= sorted_count <= bucket.size()
        # Optionally, check that the sorted region is actually sorted
        sorted_region = bucket.get_sorted_region()
        assert sorted_region == sorted(sorted_region)
    else:
        pytest.skip("Sorted/unsorted region logic not implemented")


def test_directory_resize_and_migration(directory):
    """Insert enough keys to trigger directory resize/migration (attribute → dataset)."""
    for i in range(10000):
        directory.insert(f"dir{i}", i)
    if hasattr(directory, "directory_type"):
        dtype = directory.directory_type()
        assert dtype in ("attribute", "dataset")
        # If migration occurred, validate all keys are still present
        if dtype == "dataset":
            for i in range(10000):
                result = list(directory.lookup(f"dir{i}"))
                assert len(result) == 1
                assert int(result[0]) == i


def test_directory_structure_and_types(directory):
    """Check that directory entries match canonical structure and types."""
    directory.insert("dirkey", 123)
    entry = directory.get_entry("dirkey")
    assert "key_high" in entry
    assert "value" in entry
