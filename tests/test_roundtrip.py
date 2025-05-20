"""
Spec 10: Conclusion/Full Roundtrip (TDD)
Covers full roundtrip, persistence, and advanced edge cases.
All tests are TDD-style and implementation-agnostic.
"""

from cidtree.keys import E
from cidtree.main import CIDTree
from cidtree.storage import Storage
from cidtree.wal import WAL


def test_full_roundtrip(tmp_path):
    path = tmp_path / "roundtrip.h5"
    wal_path = tmp_path / "roundtrip.wal"
    hdf_storage = Storage(str(path))
    wal_storage = WAL(str(wal_path))
    tree = CIDTree(hdf_storage, wal=wal_storage)
    # Insert a variety of keys/values
    for i in range(100):
        tree.insert(E.from_str(f"key{i}"), E.from_int(i))
    # Delete some
    for i in range(0, 100, 3):
        tree.delete(E.from_str(f"key{i}"))
    # Close and reopen
    del tree
    from cidtree.main import CIDTree as CIDTree2

    hdf_storage2 = Storage(str(path))
    wal_storage2 = WAL(str(wal_path))
    tree2 = CIDTree2(hdf_storage2, wal=wal_storage2)
    # Check all values
    for i in range(100):
        expected = [] if i % 3 == 0 else [E.from_int(i)]
        assert list(tree2.get(E.from_str(f"key{i}"))) == expected


def test_persistence_and_recovery(tmp_path):
    from cidtree.keys import E
    from cidtree.main import CIDTree
    from cidtree.storage import Storage
    from cidtree.wal import WAL

    path = tmp_path / "persist.h5"
    wal_path = tmp_path / "persist.wal"
    hdf_storage = Storage(str(path))
    wal_storage = WAL(str(wal_path))
    tree = CIDTree(hdf_storage, wal=wal_storage)
    tree.insert(E.from_str("persist"), E.from_int(123))
    del tree
    hdf_storage2 = Storage(str(path))
    wal_storage2 = WAL(str(wal_path))
    tree2 = CIDTree(hdf_storage2, wal=wal_storage2)
    assert E.from_int(123) in list(tree2.get(E.from_str("persist")))
