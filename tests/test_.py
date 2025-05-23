"""Core tests to quickly check the basic functionality of the library on every edit."""

import threading

import pytest
from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


async def test_standard_workflow(tmp_path):
    # 1. Create storage and tree
    from pathlib import Path

    h5file = Path(tmp_path) / "workflow.h5"
    storage = Storage(path=h5file)
    wal = WAL(path=":memory:")
    tree = CIDStore(storage, wal)

    # 2. Insert basic keys and values
    k_alpha = E.from_str("alpha")
    k_beta = E.from_str("beta")
    await tree.insert(k_alpha, E(1))
    await tree.insert(k_beta, E(2))
    assert await tree.get(k_alpha) == [E(1)]
    assert await tree.get(k_beta) == [E(2)]

    # 4. Multi-value key: insert same key many times
    k_multi = E.from_str("multi")
    for i in range(1, 151):
        await tree.insert(k_multi, E(i))
    values = await tree.get(k_multi)
    # Multi-value: values should be unique, no duplicates, order not guaranteed
    assert values == [E(i) for i in range(1, 151)]

    # 5. Delete and check
    await tree.delete(k_alpha)
    assert not await tree.get(k_alpha)

    # 6. Bulk insert/delete
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        await tree.insert(key, E(i))
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        assert await tree.get(key) == [E(i)]
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        await tree.delete(key)
    for i in range(10, 20):
        key = E.from_str(f"bulk{i}")
        assert not await tree.get(key)

    # 7. Node splits: perform enough inserts to trigger splits in both leaf and internal nodes
    for i in range(1, 1001):
        await tree.insert(E(i), k_multi)
    # Implementation-agnostic: verify the tree is still operational after splits
    assert await tree.get(k_beta) == [E(2)]
    assert set(await tree.get(k_multi)) == {E(i) for i in range(1, 151)}


def test_wal_recovery(tmp_path):
    # Write entries then reopen to test WAL replay
    from pathlib import Path

    file = Path(tmp_path) / "walrec.h5"
    storage1 = Storage(path=file)
    wal = WAL(path=":memory:")
    tree1 = CIDStore(storage1, wal)
    k = E.from_str("recov")
    tree1.insert(k, E(1))
    if storage1.file:
        storage1.file.close()

    # Reopen storage and tree -> WAL should replay
    storage2 = Storage(path=file)
    wal = WAL(path=":memory:")
    tree2 = CIDStore(storage2, wal)
    assert list(tree2.get(k)) == [E(1)]


def test_concurrent_writes(tmp_path):
    # Test concurrent inserts of distinct keys
    from pathlib import Path

    storage = Storage(path=Path(tmp_path) / "swmr.h5")
    wal = WAL(path=":memory:")
    tree = CIDStore(storage, wal)

    def worker(start, results):
        for i in range(start + 1, start + 51):
            k = E.from_str(f"key{i}")
            tree.insert(k, E(i))
            # verify immediate lookup
            results.append((k, list(tree.get(k))))

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
