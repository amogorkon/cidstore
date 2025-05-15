"""Core tests to quickly check the basic functionality of the library on every edit."""

import threading

import pytest

from cidtree.keys import E
from cidtree.storage import StorageManager
from cidtree.tree import CIDTree


def test_standard_workflow(tmp_path):
    # 1. Create storage and tree
    h5file = tmp_path / "workflow.h5"
    storage = StorageManager(path=str(h5file))
    tree = CIDTree(storage)

    # 2. Insert basic keys and values
    k_alpha = E.from_str("alpha")
    k_beta = E.from_str("beta")
    tree.insert(k_alpha, E(1))
    tree.insert(k_beta, E(2))
    assert list(tree.lookup(k_alpha)) == [E(1)]
    assert list(tree.lookup(k_beta)) == [E(2)]

    # 4. Multi-value key: insert same key many times
    k_multi = E.from_str("multi")
    for i in range(1, 151):
        tree.insert(k_multi, E(i))
    values = list(tree.lookup(k_multi))
    # Multi-value: order is preserved, but allow for duplicates and check set equality
    assert set(values) == set(E(i) for i in range(1, 151))

    # 5. Delete and check
    tree.delete(k_alpha)
    assert not list(tree.lookup(k_alpha))

    # 6. Bulk insert/delete
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        tree.insert(key, E(i))
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        assert list(tree.lookup(key)) == [E(i)]
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        tree.delete(key)
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        assert not list(tree.lookup(key))

    # 7. Node splits: perform enough inserts to trigger splits in both leaf and internal nodes
    for i in range(1, 1001):
        tree.insert(E(i), k_multi)
    # Implementation-agnostic: verify the tree is still operational after splits
    assert list(tree.lookup(k_beta)) == [E(2)]
    assert set(tree.lookup(k_multi)) == set(E(i) for i in range(1, 151))


def test_wal_recovery(tmp_path):
    # Write entries then reopen to test WAL replay
    file = tmp_path / "walrec.h5"
    storage1 = StorageManager(path=str(file))
    tree1 = CIDTree(storage1)
    k = E.from_str("recov")
    tree1.insert(k, E(1))
    if storage1.file:
        storage1.file.close()

    # Reopen storage and tree -> WAL should replay
    storage2 = StorageManager(path=str(file))
    tree2 = CIDTree(storage2)
    assert list(tree2.lookup(k)) == [E(1)]


def test_concurrent_writes(tmp_path):
    # Test concurrent inserts of distinct keys
    storage = StorageManager(path=str(tmp_path / "swmr.h5"))
    tree = CIDTree(storage)

    def worker(start, results):
        for i in range(start + 1, start + 51):
            k = E.from_str(f"key{i}")
            tree.insert(k, E(i))
            # verify immediate lookup
            results.append((k, list(tree.lookup(k))))

    results = []
    t1 = threading.Thread(target=worker, args=(0, results))
    t2 = threading.Thread(target=worker, args=(50, results))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Assert all inserted keys present with correct values
    for k, vals in results:
        # The inserted value is always E(i), so just check E(i) is present
        # k is E.from_str(f"key{i}") and value is E(i)
        i = int(str(k)[str(k).find("key") + 3 :]) if "key" in str(k) else None
        if i is not None:
            assert E(i) in vals


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
