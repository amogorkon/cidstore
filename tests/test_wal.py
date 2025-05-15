def test_wal_log_clear_on_checkpoint(bucket):
    """WAL log should be cleared or truncated after a successful checkpoint or flush."""
    bucket.insert("foo", 1)
    bucket.checkpoint()
    wal = bucket.get_wal_log()
    assert wal == [] or wal is None
import pytest

# WAL (Write-Ahead Logging) Spec Compliance Tests (TDD, Hash Directory Model)
# These tests are written for the canonical hash directory model (Spec 2, 3, 4).
# They assume a bucket-based structure, with explicit WAL APIs and state tracking.
# All test logic is spec-driven and implementation-agnostic.

def test_wal_log_entry_on_insert(bucket):
    """Inserting a key/value must create a WAL log entry with correct op and data."""
    bucket.insert("walkey", 42)
    wal = bucket.get_wal_log()
    assert any(e["op"] == "insert" and e["key"] == "walkey" and e["value"] == 42 for e in wal)

def test_wal_log_entry_on_delete(bucket):
    """Deleting a key/value must create a WAL log entry with correct op and data."""
    bucket.insert("delkey", 99)
    bucket.delete("delkey")
    wal = bucket.get_wal_log()
    assert any(e["op"] == "delete" and e["key"] == "delkey" for e in wal)

def test_wal_commit_and_rollback(bucket):
    """WAL must support atomic commit and rollback of grouped operations."""
    bucket.begin_transaction()
    bucket.insert("a", 1)
    bucket.insert("b", 2)
    bucket.rollback()
    assert list(bucket.lookup("a")) == []
    assert list(bucket.lookup("b")) == []
    bucket.begin_transaction()
    bucket.insert("a", 1)
    bucket.insert("b", 2)
    bucket.commit()
    assert 1 in [int(x) for x in bucket.lookup("a")]
    assert 2 in [int(x) for x in bucket.lookup("b")]

def test_wal_recovery(bucket):
    """After a simulated crash, WAL recovery must restore all committed changes and discard uncommitted ones."""
    bucket.begin_transaction()
    bucket.insert("persisted", 123)
    bucket.commit()
    bucket.begin_transaction()
    bucket.insert("not_persisted", 456)
    # Simulate crash before commit
    bucket.simulate_crash_and_recover()
    # Only committed data should be present
    assert 123 in [int(x) for x in bucket.lookup("persisted")]
    assert list(bucket.lookup("not_persisted")) == []

def test_wal_log_contains_all_ops(bucket):
    """WAL log should contain all insert, delete, and update operations in order."""
    bucket.insert("x", 1)
    bucket.insert("y", 2)
    bucket.delete("x")
    wal = bucket.get_wal_log()
    ops = [(e["op"], e["key"]) for e in wal]
    assert ("insert", "x") in ops
    assert ("insert", "y") in ops
    assert ("delete", "x") in ops

def test_wal_log_clear_on_checkpoint(bucket):
    """WAL log should be cleared or truncated after a successful checkpoint or flush."""
    bucket.insert("foo", 1)
    bucket.checkpoint()
    wal = bucket.get_wal_log()
    assert wal == [] or wal is None
