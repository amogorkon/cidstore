"""
Spec 8: Concurrency & Crash Recovery (TDD)
Covers SWMR, concurrent access, crash recovery, and related edge cases.
All tests are TDD-style and implementation-agnostic.
"""

import threading

import pytest
from cidtree.keys import E


@pytest.mark.parametrize("concurrent_ops", [2, 4, 8])
def test_concurrent_inserts_and_lookups(tree, concurrent_ops):
    key = "concurrent_key"

    def inserter(idx):
        for i in range(10):
            tree.insert(f"{key}_{idx}", i)

    def get(idx):
        for i in range(10):
            list(tree.get(f"{key}_{idx}"))

    threads = []
    for i in range(concurrent_ops):
        t1 = threading.Thread(target=inserter, args=(i,))
        t2 = threading.Thread(target=get, args=(i,))
        threads.extend([t1, t2])
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # All keys should be present
    for i in range(concurrent_ops):
        assert set(tree.get(f"{key}_{i}")) == set(range(10))


def test_swmr_write_and_read(tmp_path):
    from cidtree.main import CIDTree
    from cidtree.storage import Storage
    from cidtree.tree import WAL

    path = tmp_path / "swmr_crash.h5"
    storage = Storage(str(path))
    wal = WAL(None)
    tree = CIDTree(storage, wal=wal)
    tree.insert(E.from_str("swmr"), E.from_int(1))
    # Simulate SWMR by opening file in two handles
    with Storage(None) as f1, Storage(None) as f2:
        buckets1 = f1["/buckets"]
        buckets2 = f2["/buckets"]
        assert "swmr" in list(buckets1)
        assert "swmr" in list(buckets2)


def test_crash_recovery_and_replay(tmp_path):
    from cidtree.main import CIDTree
    from cidtree.storage import Storage
    from cidtree.tree import WAL

    path = tmp_path / "crash_recover.h5"
    storage = Storage(str(path))
    wal = WAL(None)
    tree = CIDTree(storage, wal=wal)
    tree.insert(E.from_str("crash"), E.from_int(42))
    tree.insert(E.from_str("crash"), E.from_int(43))
    # Simulate crash by not closing file (if API allows)
    # tree.simulate_crash()
    # Reopen and recover
    storage2 = Storage(str(path))
    wal2 = WAL(None)
    tree2 = CIDTree(storage2, wal=wal2)
    # tree2.recover()
    assert set(tree2.get(E.from_str("crash"))) == {42, 43}
    storage2 = Storage(str(path))
    wal2 = WAL(None)
    tree2 = CIDTree(storage2, wal=wal2)
    # tree2.recover()
    assert set(tree2.get(E.from_str("crash"))) == {42, 43}
