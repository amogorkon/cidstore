"""
Spec 5: Multi-Value Keys (TDD)
Covers inline vs. spill (external ValueSet) promotion/demotion, compaction, and edge cases.
All tests are TDD-style and implementation-agnostic.
"""

import pytest

pytestmark = pytest.mark.asyncio


@pytest.mark.xfail(reason="No multi-value key promotion/demotion per Spec 5")
async def test_inline_to_spill_promotion(directory):
    """Inserting more than two values for a key should promote from inline to external ValueSet (SpillPointer)."""
    from cidstore.keys import E

    key = E.from_str("multi")
    await directory.insert(key, E(1))
    await directory.insert(key, E(2))
    # Should still be inline (slots length 2, both nonzero)
    entry = await directory.get_entry(key)
    slots = entry["slots"]
    assert isinstance(slots, (list, tuple))
    assert len(slots) == 2
    assert slots[0] != 0 and slots[1] != 0
    assert not await directory.is_spilled(key)
    await directory.insert(key, E(3))
    # Should now be promoted to ValueSet (spill): slot0 == 0, slot1 = spill pointer
    entry = await directory.get_entry(key)
    slots = entry["slots"]
    assert isinstance(slots, (list, tuple))
    assert len(slots) == 2
    assert slots[0] == 0
    assert slots[1] != 0  # spill pointer
    assert await directory.is_spilled(key)
    # All values should be present
    result = await directory.get(key)
    assert all(E(i) in result for i in range(1, 4))


@pytest.mark.xfail(reason="No multi-value key promotion/demotion per Spec 5")
async def test_spill_to_inline_demotion(directory):
    """After deleting values, ValueSet may be demoted back to inline if two or fewer values remain."""
    from cidstore.keys import E

    key = E.from_str("demote")
    for i in range(1, 5):
        await directory.insert(key, E(i))
    for i in range(1, 3):
        await directory.delete_value(key, E(i))
    # Should be eligible for demotion: slots should be inline (length 2, both nonzero)
    await directory.demote_if_possible(key)
    assert not await directory.is_spilled(key)
    entry = await directory.get_entry(key)
    slots = entry["slots"]
    assert isinstance(slots, (list, tuple))
    assert len(slots) == 2
    assert slots[0] != 0 and slots[1] != 0
    # Remaining values should be present
    result = await directory.get(key)
    assert all(E(i) in result for i in range(3, 5))


@pytest.mark.xfail(reason="No ValueSet compaction per Spec 5")
async def test_compaction_removes_tombstones(directory):
    """Compaction should remove tombstones and reclaim space in ValueSet."""
    key = "compact"
    for i in range(10):
        await directory.insert(key, i)
    for i in range(5):
        await directory.delete(key, i)
    await directory.compact(key)
    result = await directory.lookup(key)
    assert all(i in [int(x) for x in result] for i in range(5, 10))
    assert all(i not in [int(x) for x in result] for i in range(0, 5))


async def test_multivalue_edge_cases(directory):
    """Test edge cases: duplicate insert, delete non-existent, etc."""
    key = "edge"
    await directory.insert(key, 1)
    await directory.insert(key, 1)  # Duplicate
    result = await directory.lookup(key)
    assert result.count(1) >= 1  # At least one present
    await directory.delete(key, 2)  # Delete non-existent
    # Should not raise or remove existing value
    assert 1 in [int(x) for x in await directory.lookup(key)]


async def test_multivalue_get_entry_fields(directory):
    """get_entry should return all canonical fields for a multi-value key (no state_mask)."""
    key = "entryfields"
    for i in range(1, 4):
        await directory.insert(key, i)
    entry = await directory.get_entry(key)
    assert entry is not None
    assert "key" in entry
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    slots = entry["slots"]
    assert isinstance(slots, (list, tuple))
    assert len(slots) == 2
    assert "values" in entry
    assert "value" in entry
    # All inserted values should be present
    for i in range(1, 4):
        assert i in entry["values"]


@pytest.mark.xfail(
    reason="sorted/unsorted region API not implemented", raises=AttributeError
)
async def test_multivalue_sorted_unsorted_api(directory):
    """Test get_sorted_count, get_unsorted_count, get_bucket_sorted_region, get_bucket_unsorted_region."""
    key = "sortunsort"
    for i in range(1, 11):
        await directory.insert(key, i)
    bucket_id = directory.directory[key]
    sorted_count = await directory.get_sorted_count(bucket_id)
    unsorted_count = await directory.get_unsorted_count(bucket_id)
    assert sorted_count >= 0
    assert unsorted_count >= 0
    sorted_region = await directory.get_bucket_sorted_region(bucket_id)
    unsorted_region = await directory.get_bucket_unsorted_region(bucket_id)
    assert sorted_region is not None
    assert unsorted_region is not None
    assert sorted_region.shape[0] == sorted_count
    assert unsorted_region.shape[0] == unsorted_count


@pytest.mark.xfail(reason="No ValueSet compaction per Spec 5")
async def test_multivalue_delete_and_compact(directory):
    """Deleting all values and compacting should remove the key from the ValueSet."""
    key = "delcompact"
    for i in range(1, 6):
        await directory.insert(key, i)
    for i in range(1, 6):
        await directory.delete(key, i)
    await directory.compact(key)
    assert not await directory.valueset_exists(key)
    assert await directory.lookup(key) == []


@pytest.mark.xfail(reason="No multi-value key promotion/demotion per Spec 5")
async def test_multivalue_is_spilled_and_demote(directory):
    """Test is_spilled and demote_if_possible APIs."""
    key = "spilldemote"
    for i in range(1, 201):
        await directory.insert(key, i)
    assert await directory.is_spilled(key)
    for i in range(1, 198):
        await directory.delete(key, i)
    await directory.demote_if_possible(key)
    assert not await directory.is_spilled(key)
    # Remaining values should be present
    result = await directory.lookup(key)
    assert all(i in [int(x) for x in result] for i in range(198, 201))
