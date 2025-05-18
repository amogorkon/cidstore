"""
Spec 5: Multi-Value Keys (TDD)
Covers inline vs. spill (external ValueSet) promotion/demotion, compaction, and edge cases.
All tests are TDD-style and implementation-agnostic.
"""


def test_inline_to_spill_promotion(directory):
    """Inserting enough values for a key should promote from inline to external ValueSet."""
    key = "multi"
    for i in range(1, 201):
        directory.insert(key, i)
    if hasattr(directory, "is_spilled"):
        assert directory.is_spilled(key)
    # All values should be present
    result = list(directory.lookup(key))
    assert all(i in [int(x) for x in result] for i in range(1, 201))


def test_spill_to_inline_demotion(directory):
    """After deleting values, ValueSet may be demoted back to inline if below threshold."""
    key = "demote"
    for i in range(1, 201):
        directory.insert(key, i)
    for i in range(1, 190):
        directory.delete(key, i)
    if hasattr(directory, "is_spilled") and hasattr(directory, "demote_if_possible"):
        directory.demote_if_possible(key)
        assert not directory.is_spilled(key)
    # Remaining values should be present
    result = list(directory.lookup(key))
    assert all(i in [int(x) for x in result] for i in range(190, 201))


def test_compaction_removes_tombstones(directory):
    """Compaction should remove tombstones and reclaim space in ValueSet."""
    key = "compact"
    for i in range(10):
        directory.insert(key, i)
    for i in range(5):
        directory.delete(key, i)
    if hasattr(directory, "compact"):
        directory.compact(key)
    result = list(directory.lookup(key))
    assert all(i in [int(x) for x in result] for i in range(5, 10))
    assert all(i not in [int(x) for x in result] for i in range(0, 5))


def test_multivalue_edge_cases(directory):
    """Test edge cases: duplicate insert, delete non-existent, etc."""
    key = "edge"
    directory.insert(key, 1)
    directory.insert(key, 1)  # Duplicate
    result = list(directory.lookup(key))
    assert result.count(1) >= 1  # At least one present
    directory.delete(key, 2)  # Delete non-existent
    # Should not raise or remove existing value
    assert 1 in [int(x) for x in directory.lookup(key)]


def test_multivalue_get_entry_fields(directory):
    """get_entry should return all canonical fields for a multi-value key."""
    key = "entryfields"
    for i in range(1, 6):
        directory.insert(key, i)
    entry = directory.get_entry(key)
    assert entry is not None
    assert "key" in entry
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "state_mask" in entry
    assert "version" in entry
    assert "values" in entry
    assert "value" in entry
    # All inserted values should be present
    for i in range(1, 6):
        assert i in entry["values"]


def test_multivalue_sorted_unsorted_api(directory):
    """Test get_sorted_count, get_unsorted_count, get_bucket_sorted_region, get_bucket_unsorted_region."""
    key = "sortunsort"
    for i in range(1, 11):
        directory.insert(key, i)
    bucket_id = directory.directory[key]
    sorted_count = directory.get_sorted_count(bucket_id)
    unsorted_count = directory.get_unsorted_count(bucket_id)
    assert sorted_count >= 0
    assert unsorted_count >= 0
    sorted_region = directory.get_bucket_sorted_region(bucket_id)
    unsorted_region = directory.get_bucket_unsorted_region(bucket_id)
    assert sorted_region is not None
    assert unsorted_region is not None
    assert sorted_region.shape[0] == sorted_count
    assert unsorted_region.shape[0] == unsorted_count


def test_multivalue_delete_and_compact(directory):
    """Deleting all values and compacting should remove the key from the ValueSet."""
    key = "delcompact"
    for i in range(1, 6):
        directory.insert(key, i)
    for i in range(1, 6):
        directory.delete(key, i)
    if hasattr(directory, "compact"):
        directory.compact(key)
    assert not directory.valueset_exists(key)
    assert directory.lookup(key) == []


def test_multivalue_is_spilled_and_demote(directory):
    """Test is_spilled and demote_if_possible APIs."""
    key = "spilldemote"
    for i in range(1, 201):
        directory.insert(key, i)
    assert directory.is_spilled(key)
    for i in range(1, 198):
        directory.delete(key, i)
    if hasattr(directory, "demote_if_possible"):
        directory.demote_if_possible(key)
        assert not directory.is_spilled(key)
    # Remaining values should be present
    result = list(directory.lookup(key))
    assert all(i in [int(x) for x in result] for i in range(198, 201))
