"""
Spec 8: Concurrency & Crash Recovery (TDD)
Covers SWMR, concurrent access, crash recovery, and related edge cases.
All tests are TDD-style and implementation-agnostic.
"""

import threading

import pytest

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.wal import WAL


@pytest.mark.parametrize("concurrent_ops", [2, 4, 8])
def test_concurrent_inserts_and_lookups(tree, concurrent_ops):
    key = "concurrent_key"
    writer_lock = threading.Lock()

    def inserter(idx):
        for i in range(10):
            # Serialize writers to respect the single-writer guarantee in Spec 8
            with writer_lock:
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
    # Debug: print worker-local mem-index snapshot to help diagnose failures
    try:
        # `tree` is the SyncCIDStoreWrapper which exposes `_store` attribute
        mem_index = getattr(tree._store.hdf, "_mem_index", None)
        if mem_index is not None:
            print(
                "DEBUG: mem_index keys after threads join:", list(mem_index.keys())[:32]
            )
            print("DEBUG: mem_index len:", len(mem_index))
            try:
                # Print detailed entries and wal_times for keys of interest
                for k in list(mem_index.keys())[:64]:
                    try:
                        entry = tree._store.hdf._mem_index.get(k)
                        wal_time = tree._store.hdf._mem_index_wal_times.get(k)
                        print(
                            f"DEBUG: mem_index[{k}] wal_time={wal_time} entry_slots={entry['slots']}"
                        )
                    except Exception:
                        print(f"DEBUG: mem_index[{k}] inspect failed")
            except Exception:
                pass
    except Exception as e:
        print("DEBUG: failed to inspect mem_index:", e)

    # All keys should be present
    for i in range(concurrent_ops):
        assert set(tree.get(f"{key}_{i}")) == set(range(10))


def test_swmr_write_and_read(tmp_path):
    from cidstore.main import CIDStore
    from cidstore.storage import Storage
    from cidstore.store import WAL

    path = tmp_path / "swmr_crash.h5"
    storage = Storage(str(path))
    wal = WAL(None)
    tree = CIDStore(storage, wal=wal)
    tree.insert(E.from_str("swmr"), E.from_int(1))
    # Simulate SWMR by opening file in two handles
    # Open two handles to the same underlying file to simulate SWMR readers
    # Open two handles to the same underlying file to simulate SWMR readers
    with Storage(str(path)) as f1, Storage(str(path)) as f2:
        buckets1 = f1["/buckets"]
        buckets2 = f2["/buckets"]
        # The store organizes keys inside bucket datasets named like
        # 'bucket_0000'. Ensure both reader handles can observe the
        # inserted key by scanning bucket datasets for the canonical
        # entry matching the inserted key's high/low parts.
        target = E.from_str("swmr")

        def _bucket_contains_key(buckets_group, target_e):
            for name in buckets_group:
                ds = buckets_group[name]
                try:
                    arr = ds[:]
                except Exception:
                    # If reading entire dataset fails, fall back to per-item
                    try:
                        total = ds.shape[0]
                        arr = [ds[i] for i in range(total)]
                    except Exception:
                        arr = []
                for e in arr:
                    if not hasattr(e, "__getitem__"):
                        continue
                    try:
                        kh = (
                            int(e["key_high"])
                            if "key_high" in e.dtype.names
                            else int(e["key_high"])
                            if hasattr(e, "__getitem__")
                            else 0
                        )
                        kl = (
                            int(e["key_low"])
                            if "key_low" in e.dtype.names
                            else int(e["key_low"])
                            if hasattr(e, "__getitem__")
                            else 0
                        )
                    except Exception:
                        # Structured array access may fail for unexpected types; skip
                        continue
                    if kh == int(target_e.high) and kl == int(target_e.low):
                        return True
            return False

        assert _bucket_contains_key(buckets1, target)
        assert _bucket_contains_key(buckets2, target)


def test_crash_recovery_and_replay(tmp_path):
    path = tmp_path / "crash_recover.h5"
    storage = Storage(str(path))
    wal = WAL(None)
    # Use the synchronous test-facing facade which runs the async store on
    # a background loop thread so test code can call `insert`/`get`
    # synchronously.
    from cidstore.main import CIDStore as SyncCIDStore

    tree = SyncCIDStore(storage, wal=wal)
    tree.insert(E.from_str("crash"), E.from_int(42))
    tree.insert(E.from_str("crash"), E.from_int(43))
    # Simulate crash by not closing file (if API allows)
    # tree.simulate_crash()
    # Reopen and recover
    storage2 = Storage(str(path))
    wal2 = WAL(None)
    tree2 = SyncCIDStore(storage2, wal=wal2)
    # tree2.recover()
    assert set(tree2.get(E.from_str("crash"))) == {42, 43}
    storage2 = Storage(str(path))
    wal2 = WAL(None)
    tree2 = SyncCIDStore(storage2, wal=wal2)
    # tree2.recover()
    assert set(tree2.get(E.from_str("crash"))) == {42, 43}
