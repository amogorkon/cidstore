"""
Spec 7: Deletion and GC (TDD)
Covers deletion, garbage collection, compaction, tombstone handling, and edge cases.
All tests are TDD-style and implementation-agnostic.
"""

def test_delete_and_lookup(directory):
    directory.insert("key1", 1)
    directory.delete("key1")
    assert list(directory.lookup("key1")) == []

def test_multi_value_deletion_and_gc(directory):
    key = "multi"
    for i in range(10):
        directory.insert(key, i)
    directory.delete(key, 3)
    directory.delete(key, 7)
    values = list(directory.lookup(key))
    assert 3 not in values and 7 not in values
    if hasattr(directory, "compact"):
        directory.compact(key)
        values_after = list(directory.lookup(key))
        assert 3 not in values_after and 7 not in values_after
    if hasattr(directory, "get_tombstone_count"):
        assert directory.get_tombstone_count(key) == 0

def test_gc_removes_empty_valueset(directory):
    key = "to_gc"
    directory.insert(key, 42)
    directory.delete(key, 42)
    if hasattr(directory, "compact"):
        directory.compact(key)
    assert list(directory.lookup(key)) == []
    if hasattr(directory, "valueset_exists"):
        assert not directory.valueset_exists(key)

def test_orphan_detection_and_reclamation(directory):
    key = "orphan"
    directory.insert(key, 99)
    directory.delete(key, 99)
    if hasattr(directory, "recover"):
        directory.recover()
    if hasattr(directory, "compact"):
        directory.compact(key)
    assert list(directory.lookup(key)) == []

def test_concurrent_deletion_and_gc(directory):
    import threading
    key = "concurrent"
    for i in range(5):
        directory.insert(key, i)
    def delete_some():
        for i in range(5):
            directory.delete(key, i)
    def compact():
        if hasattr(directory, "compact"):
            directory.compact(key)
    t1 = threading.Thread(target=delete_some)
    t2 = threading.Thread(target=compact)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert list(directory.lookup(key)) == []

def test_idempotent_gc(directory):
    key = "idempotent"
    directory.insert(key, 1)
    directory.delete(key, 1)
    if hasattr(directory, "compact"):
        directory.compact(key)
        directory.compact(key)
    assert list(directory.lookup(key)) == []

def test_wal_and_deletion_log_replay(directory):
    key = "replay"
    directory.insert(key, 123)
    directory.delete(key, 123)
    if hasattr(directory, "recover"):
        directory.recover()
    assert list(directory.lookup(key)) == []

def test_underfilled_bucket_merge(directory):
    for i in range(20):
        directory.insert(f"bucket{i}", i)
    for i in range(20):
        directory.delete(f"bucket{i}", i)
    if hasattr(directory, "rebalance_buckets"):
        directory.rebalance_buckets()
    for i in range(20):
        assert list(directory.lookup(f"bucket{i}")) == []
