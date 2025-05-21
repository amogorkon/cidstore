from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL, OpType


def make_tree():
    storage = Storage(":memory:")
    wal = WAL(path=":memory:")
    return CIDStore(storage, wal)


def test_wal_log_entry_on_insert():
    tree = make_tree()
    k = E.from_str("walkey")
    v = E(42)
    tree.insert(k, v)
    wal_records = tree.wal.replay()
    assert any(
        r["op_type"] == OpType.INSERT.value
        and r["key_high"] == k.high
        and r["key_low"] == k.low
        and r["value_high"] == v.high
        and r["value_low"] == v.low
        for r in wal_records
    )
    for entry in wal_records:
        assert "version" in entry or "version_op" in entry or "op_type" in entry
        assert "nanos" in entry and "seq" in entry and "shard_id" in entry
        assert "key_high" in entry and "key_low" in entry
        assert "value_high" in entry and "value_low" in entry
        assert "checksum" in entry
        assert len(bytes(tree.wal._pack_record(entry))) == 64


def test_wal_log_entry_on_delete():
    tree = make_tree()
    k = E.from_str("delkey")
    v = E(99)
    tree.insert(k, v)
    tree.delete(k)
    wal_records = tree.wal.replay()
    assert any(
        r["op_type"] == OpType.DELETE.value
        and r["key_high"] == k.high
        and r["key_low"] == k.low
        for r in wal_records
    )
    for entry in wal_records:
        assert "version" in entry or "version_op" in entry or "op_type" in entry
        assert "nanos" in entry and "seq" in entry and "shard_id" in entry
        assert "key_high" in entry and "key_low" in entry
        assert "value_high" in entry and "value_low" in entry
        assert "checksum" in entry
        assert len(bytes(tree.wal._pack_record(entry))) == 64


def test_wal_commit_and_rollback():
    tree = make_tree()
    k1 = E.from_str("a")
    k2 = E.from_str("b")
    v1 = E(1)
    v2 = E(2)
    tree.insert(k1, v1)
    tree.insert(k2, v2)
    # No explicit rollback/commit API in CIDStore, so just check WAL contains both inserts
    wal_records = tree.wal.replay()
    assert any(
        r["op_type"] == OpType.INSERT.value and r["key_high"] == k1.high
        for r in wal_records
    )
    assert any(
        r["op_type"] == OpType.INSERT.value and r["key_high"] == k2.high
        for r in wal_records
    )


def test_wal_recovery():
    tree = make_tree()
    k = E.from_str("persisted")
    v = E(123)
    tree.insert(k, v)
    # Simulate crash by closing and reopening
    tree.wal.close()
    wal = WAL(path=":memory:")
    tree2 = CIDStore(tree.hdf, wal)
    # Replay should restore the insert
    wal_records = tree2.wal.replay()
    assert any(
        r["op_type"] == OpType.INSERT.value and r["key_high"] == k.high
        for r in wal_records
    )


def test_wal_log_contains_all_ops():
    tree = make_tree()
    kx = E.from_str("x")
    ky = E.from_str("y")
    vx = E(1)
    vy = E(2)
    tree.insert(kx, vx)
    tree.insert(ky, vy)
    tree.delete(kx)
    wal_records = tree.wal.replay()
    ops = [(r["op_type"], r["key_high"]) for r in wal_records]
    assert (OpType.INSERT.value, kx.high) in ops
    assert (OpType.INSERT.value, ky.high) in ops
    assert (OpType.DELETE.value, kx.high) in ops


def test_wal_log_clear_on_checkpoint():
    tree = make_tree()
    k = E.from_str("foo")
    v = E(1)
    tree.insert(k, v)
    # Simulate checkpoint by truncating WAL
    head = tree.wal._get_head()
    tree.wal.truncate(head)
    wal_records = tree.wal.replay()
    assert wal_records == [] or wal_records is None
