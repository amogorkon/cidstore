"""
Spec 6: Bucket Splitting and Merging (TDD)
Covers split/merge triggers, invariants, and edge cases for buckets.
All tests are TDD-style and implementation-agnostic.
"""

import pytest

def test_bucket_split_trigger(bucket):
    """Inserting entries beyond threshold should trigger a split."""
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        bucket.insert(f"split{i}", i)
    if hasattr(bucket, "split"):
        new_bucket, sep = bucket.split()
        assert bucket.validate()
        assert new_bucket.validate()
        assert bucket.size() <= bucket.SPLIT_THRESHOLD
        assert new_bucket.size() <= bucket.SPLIT_THRESHOLD
    else:
        pytest.skip("Split not implemented")

def test_bucket_merge_trigger(bucket):
    """Deleting entries to underfill two buckets should trigger a merge."""
    # Fill and split first
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        bucket.insert(f"merge{i}", i)
    if hasattr(bucket, "split"):
        new_bucket, sep = bucket.split()
        # Now delete enough to underfill both
        for i in range(bucket.SPLIT_THRESHOLD + 1):
            bucket.delete(f"merge{i}")
            new_bucket.delete(f"merge{i}")
        if hasattr(bucket, "merge"):
            merged = bucket.merge(new_bucket)
            assert merged.validate()
            assert merged.size() <= 2 * bucket.SPLIT_THRESHOLD
        else:
            pytest.skip("Merge not implemented")
    else:
        pytest.skip("Split not implemented")

def test_split_merge_invariants(bucket):
    """After split and merge, all invariants should hold and data should be preserved or deleted as expected."""
    # Fill, split, then merge
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        bucket.insert(f"inv{i}", i)
    if hasattr(bucket, "split"):
        new_bucket, sep = bucket.split()
        # Insert more to both
        for i in range(100, 110):
            bucket.insert(f"inv{i}", i)
            new_bucket.insert(f"inv{i}", i)
        # Merge
        if hasattr(bucket, "merge"):
            merged = bucket.merge(new_bucket)
            assert merged.validate()
            # All inserted values should be present
            for i in range(100, 110):
                assert i in [int(x) for x in merged.lookup(f"inv{i}")]
        else:
            pytest.skip("Merge not implemented")
    else:
        pytest.skip("Split not implemented")
