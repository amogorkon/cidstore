"""
Spec 6: Bucket Splitting and Merging (TDD)
Covers split/merge triggers, invariants, and edge cases for buckets.
All tests are TDD-style and implementation-agnostic.
"""

import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio


async def test_bucket_split_trigger(bucket):
    """Inserting entries beyond threshold should trigger a split."""
    # Insert enough unique keys to trigger a split
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        await bucket.insert(E.from_int(i), E(i))
    # Check that global_depth increased or directory size doubled
    assert bucket.global_depth >= 1
    assert len(bucket.bucket_pointers) == 2**bucket.global_depth
    # Check that at least two buckets exist
    bucket_ids = set(ptr["bucket_id"] for ptr in bucket.bucket_pointers)
    assert len(bucket_ids) > 1


@pytest.mark.xfail(reason="get_bucket_values not implemented")
async def test_split_invariants(bucket):
    """After split, all invariants should hold and data should be preserved as expected."""
    for i in range(bucket.SPLIT_THRESHOLD + 1):
        await bucket.insert(E.from_int(i), E(i))
    # After split, all values should be present in some bucket
    all_values = set()
    for ptr in bucket.bucket_pointers:
        b_id = ptr["bucket_id"]
        vals = await bucket.get_bucket_values(b_id)
        all_values.update(vals)
    assert all(E(i) in all_values for i in range(bucket.SPLIT_THRESHOLD + 1))
