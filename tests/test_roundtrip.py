"""
Spec 10: Conclusion/Full Roundtrip (TDD)
Covers full roundtrip, persistence, and advanced edge cases.
All tests are TDD-style and implementation-agnostic.
"""

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import WAL, CIDStore


def test_full_roundtrip(tmp_path):
    path = tmp_path / "roundtrip.h5"
    wal_path = tmp_path / "roundtrip.wal"
    storage = Storage(str(path))
    wal = WAL(None)
    store = CIDStore(storage, wal=wal)
    # Insert a variety of keys/values
    for i in range(100):
        store.insert(E.from_str(f"key{i}"), E.from_int(i))
    # Delete some
    for i in range(0, 100, 3):
        store.delete(E.from_str(f"key{i}"))
    # Close and reopen
    del store

    storage2 = Storage(str(path))
    wal2 = WAL(None)
    store2 = CIDStore(storage2, wal=wal2)
    # Check all values
    for i in range(100):
        expected = [] if i % 3 == 0 else [E.from_int(i)]
        assert list(store2.get(E.from_str(f"key{i}"))) == expected


def test_persistence_and_recovery(tmp_path):
    path = tmp_path / "persist.h5"
    wal_path = tmp_path / "persist.wal"
    storage = Storage(str(path))
    wal = WAL(None)
    store = CIDStore(storage, wal=wal)
    store.insert(E.from_str("persist"), E.from_int(123))
    del store
    storage2 = Storage(str(path))
    wal2 = WAL(None)
    store2 = CIDStore(storage2, wal=wal2)
    assert E.from_int(123) in list(store2.get(E.from_str("persist")))
