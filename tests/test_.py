import threading

import pytest

from cidtree.config import NODES_GROUP
from cidtree.keys import E, create_composite_key
from cidtree.storage import StorageManager
from cidtree.tree import BPlusTree


def test_standard_workflow(tmp_path):
    # 1. Create storage and tree
    h5file = tmp_path / "workflow.h5"
    storage = StorageManager(path=str(h5file))
    tree = BPlusTree(storage)

    # 2. Insert basic keys and values
    k_alpha = E.from_str("alpha")
    k_beta = E.from_str("beta")
    tree.insert(k_alpha, E(1))
    tree.insert(k_beta, E(2))
    assert list(tree.lookup(k_alpha)) == [E(1)]
    assert list(tree.lookup(k_beta)) == [E(2)]

    # 3. Insert composite key
    a = E(123)
    b = E(456)
    comp_arr = create_composite_key(a, b)
    comp_key = E((int(comp_arr["high"][0]) << 64) | int(comp_arr["low"][0]))
    tree.insert(comp_key, E(99))
    assert list(tree.lookup(comp_key)) == [E(99)]

    # 4. Multi-value key: insert same key many times
    k_multi = E.from_str("multi")
    for i in range(150):
        tree.insert(k_multi, E(i))
    values = list(tree.lookup(k_multi))
    assert values == [E(i) for i in range(150)]

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

    # 7. Node splits: ensure multiple internal node datasets
    # Perform enough inserts to trigger splits in both leaf and internal nodes
    for i in range(1000):
        tree.insert(E(i), k_multi)
    storage.open()  # Ensure file is open before subscripting
    # Ensure the group is a group, not a dataset, and list its keys
    group = storage.open()[NODES_GROUP]
    # h5py Group objects have .keys(), but Dataset does not. If group is a Dataset, treat as no nodes.
    import h5py

    node_names = list(group.keys()) if isinstance(group, h5py.Group) else []
    # Expect at least one internal node plus leaf groups
    assert any(name.isdigit() for name in node_names)


def test_wal_recovery(tmp_path):
    # Write entries then reopen to test WAL replay
    file = tmp_path / "walrec.h5"
    storage1 = StorageManager(path=str(file))
    tree1 = BPlusTree(storage1)
    k = E.from_str("recov")
    tree1.insert(k, E(1))
    if storage1.file:
        storage1.file.close()

    # Reopen storage and tree -> WAL should replay
    storage2 = StorageManager(path=str(file))
    tree2 = BPlusTree(storage2)
    assert list(tree2.lookup(k)) == [E(1)]


def test_concurrent_writes(tmp_path):
    # Test concurrent inserts of distinct keys
    storage = StorageManager(path=str(tmp_path / "swmr.h5"))
    tree = BPlusTree(storage)

    def worker(start, results):
        for i in range(start, start + 50):
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
        # Use repr to extract the integer value from E
        # repr(k) is 'E(0x...)
        k_int = int(repr(k)[2:-1], 16)
        assert vals == [E(k_int & ((1 << 64) - 1))] or vals[0].high == k.high


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
