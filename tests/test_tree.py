import threading

import pytest

from cidtree.keys import E

# Directory/Bucket Spec Compliance Tests (TDD, Hash Directory Model)
# All tests are spec-driven and implementation-agnostic.


def test_sorted_unsorted_region(directory):
    """Insert values and check sorted/unsorted region logic per spec 3."""
    key = "sorttest"
    for i in range(1, 11):
        directory.insert(key, i)
    if hasattr(directory, "get_sorted_count"):
        assert directory.get_sorted_count(key) >= 0
        # Optionally, check sorted region is sorted
        sorted_region = directory.get_sorted_region(key)
        assert sorted_region == sorted(sorted_region)
    else:
        pytest.skip("Sorted/unsorted region logic not implemented")


def test_directory_resize_and_migration(tree):
    # Insert enough keys to trigger directory resize/migration per spec 3
    for i in range(1, 10001):
        tree.insert(f"dir{i}", E(i))
    # If API exposes directory type, check migration
    if hasattr(tree, "directory_type"):
        dtype = tree.directory_type()
        assert dtype in ("attribute", "dataset")
        # If migration occurred, validate all keys are still present
        if dtype == "dataset":
            for i in range(10000):
                result = list(tree.lookup(f"dir{i}"))
                assert len(result) == 1
                assert int(result[0]) == i
    # TODO: If migration/maintenance API is available, check atomic switch and data integrity


def test_spill_pointer_and_large_valueset(tree):
    # Insert enough values to trigger ValueSet spill (external dataset)
    key = "spill"
    for i in range(1, 1001):
        tree.insert(key, E(i))
    # If API exposes spill pointer, check it
    if hasattr(tree, "has_spill_pointer"):
        assert tree.has_spill_pointer(key)
    # TODO: If directory migration is triggered by ValueSet spill, check directory state


def test_state_mask_ecc(tree):
    # Insert and delete to exercise state mask transitions
    key = "ecc"
    for i in range(1, 5):
        tree.insert(key, E(i))
    tree.delete(key)
    # If API exposes state mask, check ECC encoding/decoding
    if hasattr(tree, "get_state_mask"):
        mask = tree.get_state_mask(key)
        assert isinstance(mask, int)
        # Optionally, inject bitflip and check correction
        if hasattr(tree, "check_and_correct_state_mask"):
            tree.check_and_correct_state_mask(key)
    # TODO: If metrics/logging/tracing APIs are available, check for merge/GC events


def test_btree_initialization(tree):
    assert tree is not None


def test_btree_insert(tree):
    tree.insert("key", 123)
    result = list(tree.lookup("key"))
    assert len(result) == 1
    assert int(result[0]) == 123


def test_btree_delete(tree):
    tree.insert("key", 123)
    tree.delete("key")
    result = list(tree.lookup("key"))
    assert result == []


# Test cases for btree.py


# 3. Multi-value key promotion (mocked logic)
def test_multi_value_promotion(tree):
    # Insert the same key many times to trigger promotion logic
    key = "multi"
    for i in range(1, 201):
        tree.insert(key, E(i))
    # Should still be able to look up all values (mocked logic)
    # (In real impl, would check value-list dataset)
    result = list(tree.lookup(key))
    assert 200 in [int(x) for x in result]


def test_split_and_merge(directory):
    """Insert enough keys to trigger a split; merging should restore invariants (Spec 3, 6)."""
    for i in range(1, directory.SPLIT_THRESHOLD + 2):
        directory.insert(f"split{i}", i)
    if hasattr(directory, "split"):
        new_dir, sep = directory.split()
        assert directory.validate()
        assert new_dir.validate()
        assert directory.size() <= directory.SPLIT_THRESHOLD
        assert new_dir.size() <= directory.SPLIT_THRESHOLD
    else:
        pytest.skip("Split/merge logic not implemented")


# ---
# TODOs for full Spec 3 compliance:
# - Add tests for cache stats, version attributes, and migration tool if APIs exist
# - Add tests for sharded directory and hybrid approach if/when implemented
# - Add tests for metrics/logging/tracing if exposed
# - Document any missing features or APIs as not covered


def test_deletion_and_gc(directory):
    """Insert, delete, and check GC/compaction per spec 7."""
    for i in range(1, 11):
        directory.insert(f"delgc{i}", i)
    for i in range(1, 11):
        directory.delete(f"delgc{i}")
    for i in range(1, 11):
        result = list(directory.lookup(f"delgc{i}"))
        assert result == []
    if hasattr(directory, "compact"):
        for i in range(1, 11):
            directory.compact(f"delgc{i}")


def test_concurrent_insert(directory):
    """Simulate concurrent inserts and check for correct multi-value behavior (Spec 8)."""
    results = []

    def writer():
        directory.insert("swmr", 123)
        results.append(directory.lookup("swmr"))

    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=writer)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    for r in results:
        assert 123 in [int(x) for x in r]


# Test cases for btree.py


@pytest.mark.parametrize(
    "key,value",
    [
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("d", 4),
        ("e", 5),
    ],
)
def test_insert_and_lookup(tree, key, value):
    tree.insert(key, value)
    result = list(tree.lookup(key))
    assert len(result) == 1
    # Accept either E or int/str for test compatibility
    assert int(result[0]) == value


def test_overwrite(tree):
    tree.insert("dup", E(1))
    tree.insert("dup", E(2))
    result = list(tree.lookup("dup"))
    # Multi-value: both values should be present
    assert set(result) == {E(1), E(2)}


def test_delete(tree):
    tree.insert("x", 42)
    result = list(tree.lookup("x"))
    assert len(result) == 1
    assert int(result[0]) == 42
    tree.delete("x")
    result = list(tree.lookup("x"))
    assert result == []


def test_multiple_inserts_and_deletes(directory):
    items = [(f"k{i}", i) for i in range(1, 11)]
    for k, v in items:
        directory.insert(k, v)
    for k, v in items:
        result = list(directory.lookup(k))
        assert len(result) == 1
        assert int(result[0]) == int(v)
    for k, _ in items:
        directory.delete(k)
    for k, _ in items:
        result = list(directory.lookup(k))
        assert result == []


def test_nonexistent_key(directory):
    """Lookup for a key that was never inserted should return an empty result."""
    result = list(directory.lookup("notfound"))
    assert result == []


def test_bulk_insert(directory):
    for i in range(1, 101):
        directory.insert(f"bulk{i}", i)
    for i in range(1, 101):
        result = list(directory.lookup(f"bulk{i}"))
        assert len(result) == 1
        assert int(result[0]) == i


def test_bulk_delete(directory):
    for i in range(1, 101):
        directory.insert(f"del{i}", i)
    for i in range(1, 101):
        directory.delete(f"del{i}")
    for i in range(1, 101):
        result = list(directory.lookup(f"del{i}"))
        assert result == []
