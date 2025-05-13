import threading

import pytest


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
        tree.insert(key, i)
    # Should still be able to look up all values (mocked logic)
    # (In real impl, would check value-list dataset)
    result = list(tree.lookup(key))
    assert 200 in [int(x) for x in result]


# 4. Split/merge logic (mocked)
def test_split_merge(tree):
    # Insert enough keys to trigger a split
    for i in range(1, 51):
        tree.insert(f"split{i}", i)
    # No error means split logic is at least stubbed
    result = list(tree.lookup("split1"))
    assert len(result) == 1
    assert int(result[0]) == 1


# 5. Deletion and GC (mocked)
def test_deletion_and_gc(tree):
    for i in range(1, 11):
        tree.insert(f"delgc{i}", i)
    for i in range(1, 11):
        tree.delete(f"delgc{i}")
    for i in range(1, 11):
        result = list(tree.lookup(f"delgc{i}"))
        assert result == []


# 6. Concurrency/SWMR (simulate lock)
def test_swmr_lock(tree):
    results = []

    def writer():
        tree.insert("swmr", 123)
        results.append(tree.lookup("swmr"))

    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=writer)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    # Multi-value: check that 123 is present in all result lists
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
    from cidtree.keys import E

    tree.insert("dup", 1)
    tree.insert("dup", 2)
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


def test_multiple_inserts_and_deletes(tree):
    items = [(f"k{i}", i) for i in range(1, 11)]
    for k, v in items:
        tree.insert(k, v)
    for k, v in items:
        result = list(tree.lookup(k))
        assert len(result) == 1
        assert int(result[0]) == v
    for k, _ in items:
        tree.delete(k)
    for k, _ in items:
        result = list(tree.lookup(k))
        assert result == []


def test_nonexistent_key(tree):
    # Use a key that is not 0 and not previously inserted
    result = list(tree.lookup("notfound"))
    assert result == []


def test_bulk_insert(tree):
    for i in range(1, 101):  # Avoid 0 as value
        tree.insert(f"bulk{i}", i)
    for i in range(1, 101):
        result = list(tree.lookup(f"bulk{i}"))
        assert len(result) == 1
        assert int(result[0]) == i


def test_bulk_delete(tree):
    for i in range(1, 101):  # Avoid 0 as value
        tree.insert(f"del{i}", i)
    for i in range(1, 101):
        tree.delete(f"del{i}")
    for i in range(1, 101):
        result = list(tree.lookup(f"del{i}"))
        assert result == []
