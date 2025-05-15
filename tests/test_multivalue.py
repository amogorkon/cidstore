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
