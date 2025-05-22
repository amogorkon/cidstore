
import time
import pytest
import io
from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL, OpType, pack_record, unpack_record, OpVer


def make_tree():
    storage = Storage(io.BytesIO())
    wal = WAL(path=":memory:")
    return CIDStore(storage, wal)


import pytest

@pytest.mark.asyncio
async def test_wal_log_entry_on_insert():
    tree = make_tree()
    k = E.from_str("walkey")
    v = E(42)
    await tree.insert(k, v)
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


@pytest.mark.asyncio
async def test_wal_log_entry_on_delete():
    tree = make_tree()
    k = E.from_str("delkey")
    v = E(99)
    await tree.insert(k, v)
    await tree.delete(k)
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


@pytest.mark.asyncio
async def test_wal_commit_and_rollback():
    tree = make_tree()
    k1 = E.from_str("a")
    k2 = E.from_str("b")
    v1 = E(1)
    v2 = E(2)
    await tree.insert(k1, v1)
    await tree.insert(k2, v2)
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


@pytest.mark.asyncio
async def test_wal_recovery():
    tree = make_tree()
    k = E.from_str("persisted")
    v = E(123)
    await tree.insert(k, v)
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


@pytest.mark.asyncio
async def test_wal_log_contains_all_ops():
    tree = make_tree()
    kx = E.from_str("x")
    ky = E.from_str("y")
    vx = E(1)
    vy = E(2)
    await tree.insert(kx, vx)
    await tree.insert(ky, vy)
    await tree.delete(kx)
    wal_records = tree.wal.replay()
    ops = [(r["op_type"], r["key_high"]) for r in wal_records]
    assert (OpType.INSERT.value, kx.high) in ops
    assert (OpType.INSERT.value, ky.high) in ops
    assert (OpType.DELETE.value, kx.high) in ops


def test_wal_log_clear_on_checkpoint():

    pass





def test_pack_record_and_unpack_record_roundtrip():
    # Use fixed values for reproducibility
    version = OpVer.NOW
    op_type = OpType.INSERT
    time_tuple = (int(time.time_ns()), 123, 1)
    key_high = 0x1234567890ABCDEF
    key_low = 0x0FEDCBA098765432
    value_high = 0x1111222233334444
    value_low = 0x5555666677778888

    rec_bytes = pack_record(version, op_type, time_tuple, key_high, key_low, value_high, value_low)
    assert isinstance(rec_bytes, bytes)
    assert len(rec_bytes) == 64

    rec_dict = unpack_record(rec_bytes)
    assert rec_dict is not None
    assert rec_dict["version"] == version.value
    assert rec_dict["op_type"] == op_type.value
    assert rec_dict["nanos"] == time_tuple[0]
    assert rec_dict["seq"] == time_tuple[1]
    assert rec_dict["shard_id"] == time_tuple[2]
    assert rec_dict["key_high"] == key_high
    assert rec_dict["key_low"] == key_low
    assert rec_dict["value_high"] == value_high
    assert rec_dict["value_low"] == value_low
    assert isinstance(rec_dict["checksum"], int)

def test_unpack_record_invalid_length():
    # Too short
    bad_bytes = b"short"
    assert unpack_record(bad_bytes) is None
    # Too long
    bad_bytes = b"x" * 100
    assert unpack_record(bad_bytes) is None

def test_unpack_record_bad_checksum():
    version = OpVer.NOW
    op_type = OpType.INSERT
    time_tuple = (int(time.time_ns()), 123, 1)
    key_high = 1
    key_low = 2
    value_high = 3
    value_low = 4
    rec_bytes = bytearray(pack_record(version, op_type, time_tuple, key_high, key_low, value_high, value_low))
    # Corrupt the checksum
    rec_bytes[-4:] = b"\x00\x00\x00\x00"
    assert unpack_record(bytes(rec_bytes)) is None
