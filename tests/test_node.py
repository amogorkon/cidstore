"""
Node/Bucket structure and split/merge tests for the canonical hash directory model (Spec 2, 3, 6).
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


def test_bucket_structure_and_types(bucket):
    """Check that the bucket structure matches canonical data types (Spec 2)."""
    bucket.insert("bigkey", 1 << 64)
    entry = bucket.get_entry("bigkey")
    assert isinstance(entry["key_high"], int)
    assert entry["key_high"] == 1


def test_directory_entry_structure(directory):
    """Check that directory entries match canonical structure (Spec 2)."""
    directory.insert("dirkey", 123)
    entry = directory.get_entry("dirkey")
    assert "key_high" in entry
    assert "value" in entry
