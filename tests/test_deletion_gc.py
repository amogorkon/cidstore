"""
Tests for deletion, GC, compaction, and related workflows in the canonical hash directory model (Spec 7).
All tests are TDD-style and implementation-agnostic.
"""

import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def test_delete_and_lookup(directory):
    from cidstore.keys import E

    key = E.from_str("key1")
    await directory.insert(key, E.from_int(1))
    await directory.delete(key)
    assert await directory.lookup(key) == []


@pytest.mark.xfail(reason="compact/get_tombstone_count not implemented")
async def test_multi_value_deletion_and_gc(directory):
    from cidstore.keys import E

    key = E.from_str("multi")
    for i in range(10):
        await directory.insert(key, E.from_int(i))
    await directory.delete(key, E.from_int(3))
    await directory.delete(key, E.from_int(7))
    values = await directory.lookup(key)
    assert E.from_int(3) not in values and E.from_int(7) not in values
    await directory.compact(key)
    values_after = await directory.lookup(key)
    assert E.from_int(3) not in values_after and E.from_int(7) not in values_after
    assert await directory.get_tombstone_count(key) == 0


@pytest.mark.xfail(reason="compact/valueset_exists not implemented")
async def test_gc_removes_empty_valueset(directory):
    from cidstore.keys import E

    key = E.from_str("to_gc")
    await directory.insert(key, E.from_int(42))
    await directory.delete(key, E.from_int(42))
    await directory.compact(key)
    assert await directory.lookup(key) == []
    assert not await directory.valueset_exists(key)


@pytest.mark.xfail(reason="recover/compact not implemented")
async def test_orphan_detection_and_reclamation(directory):
    from cidstore.keys import E

    key = E.from_str("orphan")
    await directory.insert(key, E.from_int(99))
    await directory.delete(key, E.from_int(99))
    await directory.recover()
    await directory.compact(key)
    assert await directory.lookup(key) == []


@pytest.mark.xfail(reason="compact not implemented")
async def test_concurrent_deletion_and_gc(directory):
    from cidstore.keys import E

    key = E.from_str("concurrent")
    for i in range(5):
        await directory.insert(key, E.from_int(i))

    async def delete_some():
        for i in range(5):
            await directory.delete(key, E.from_int(i))

    async def compact():
        await directory.compact(key)

    await asyncio.gather(delete_some(), compact())
    assert await directory.lookup(key) == []


@pytest.mark.xfail(reason="compact not implemented")
async def test_idempotent_gc(directory):
    key = "idempotent"
    await directory.insert(key, 1)
    await directory.delete(key, 1)
    await directory.compact(key)
    await directory.compact(key)
    assert await directory.lookup(key) == []


@pytest.mark.xfail(reason="recover not implemented")
async def test_wal_and_deletion_log_replay(directory):
    key = "replay"
    await directory.insert(key, 123)
    await directory.delete(key, 123)
    await directory.recover()
    assert await directory.lookup(key) == []


@pytest.mark.xfail(reason="rebalance_buckets not implemented")
async def test_underfilled_bucket_merge(directory):
    for i in range(20):
        await directory.insert(f"bucket{i}", i)
    for i in range(20):
        await directory.delete(f"bucket{i}", i)
    await directory.rebalance_buckets()
    for i in range(20):
        assert await directory.lookup(f"bucket{i}") == []
