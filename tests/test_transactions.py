"""
Tests for transaction support in CIDStore.

Tests transaction begin, commit, rollback, and transactional triple operations.
"""

import pytest

from cidstore.keys import E


def test_basic_transaction_commit(tree):
    """Test basic transaction with commit."""
    from cidstore.predicates import CounterStore

    # Register predicates as specialized stores so get_triple works
    s = E(1)
    p1 = E(2)
    p2 = E(3)

    tree.register_predicate(p1, CounterStore)
    tree.register_predicate(p2, CounterStore)

    # Begin transaction
    tree.begin_transaction()

    assert tree.in_transaction()

    # Insert triples within transaction
    tree.insert_triple_transactional(s, p1, 100)
    tree.insert_triple_transactional(s, p2, 200)

    # Commit transaction
    result = tree.commit()

    assert not tree.in_transaction()
    assert result["operations"] == 2

    # Verify triples were inserted
    assert tree.get_triple(s, p1) == 100
    assert tree.get_triple(s, p2) == 200


def test_transaction_rollback(tree):
    """Test transaction rollback logs abort and clears state."""
    s = E(10)
    p = E(20)

    # Begin transaction
    tree.begin_transaction()

    # Insert triple (applied immediately in current implementation)
    tree.insert_triple_transactional(s, p, 300)

    # Rollback
    tree.rollback()

    assert not tree.in_transaction()

    # Note: In current implementation, operations are applied immediately,
    # so triple still exists. True rollback would require undo log.
    # This test documents current behavior.


def test_transaction_with_delete(tree):
    """Test transaction with both insert and delete."""
    from cidstore.predicates import CounterStore

    s = E(30)
    p1 = E(40)
    p2 = E(50)

    # Register predicates
    tree.register_predicate(p1, CounterStore)
    tree.register_predicate(p2, CounterStore)

    # Insert initial triple outside transaction
    tree.insert_triple(s, p1, 400)

    # Begin transaction
    tree.begin_transaction()

    # Delete existing triple and insert new one
    tree.delete_triple_transactional(s, p1, 400)
    tree.insert_triple_transactional(s, p2, 500)

    # Commit
    result = tree.commit()

    assert result["operations"] == 2

    # Verify state
    assert tree.get_triple(s, p1) is None  # Deleted
    assert tree.get_triple(s, p2) == 500  # Inserted


def test_transaction_multiple_operations(tree):
    """Test transaction with many operations."""
    from cidstore.predicates import CounterStore

    subjects = [E(100 + i) for i in range(10)]
    predicate = E(200)

    tree.register_predicate(predicate, CounterStore)

    tree.begin_transaction()

    for i, s in enumerate(subjects):
        tree.insert_triple_transactional(s, predicate, 1000 + i)

    result = tree.commit()

    assert result["operations"] == 10

    # Verify all inserted
    for i, s in enumerate(subjects):
        assert tree.get_triple(s, predicate) == 1000 + i


def test_transaction_already_active_error(tree):
    """Test error when beginning transaction while one is active."""
    tree.begin_transaction()

    with pytest.raises(RuntimeError, match="Transaction already active"):
        tree.begin_transaction()

    tree.rollback()  # Clean up


def test_commit_without_transaction_error(tree):
    """Test error when committing without active transaction."""
    with pytest.raises(RuntimeError, match="No active transaction"):
        tree.commit()


def test_rollback_without_transaction_error(tree):
    """Test error when rolling back without active transaction."""
    with pytest.raises(RuntimeError, match="No active transaction"):
        tree.rollback()


def test_transactional_operations_outside_transaction(tree):
    """Test that transactional methods work outside transaction."""
    from cidstore.predicates import CounterStore

    s = E(300)
    p = E(400)

    tree.register_predicate(p, CounterStore)

    # Should execute immediately without transaction
    tree.insert_triple_transactional(s, p, 500)

    assert tree.get_triple(s, p) == 500

    # Delete should also work
    tree.delete_triple_transactional(s, p, 500)

    assert tree.get_triple(s, p) is None


def test_empty_transaction_commit(tree):
    """Test committing empty transaction."""
    tree.begin_transaction()

    result = tree.commit()

    assert result["operations"] == 0


def test_transaction_with_specialized_predicates(tree):
    """Test transactions work with specialized predicates (Counter, MultiValue)."""
    from cidstore.predicates import CounterStore, MultiValueSetStore

    # Register specialized predicates
    tree.register_predicate(E(1000), CounterStore)
    tree.register_predicate(E(2000), MultiValueSetStore)

    s1 = E(500)
    s2 = E(600)
    p_counter = E(1000)
    p_multi = E(2000)

    tree.begin_transaction()

    # Insert counter
    tree.insert_triple_transactional(s1, p_counter, 10)

    # Insert multivalue
    tree.insert_triple_transactional(s2, p_multi, 100)
    tree.insert_triple_transactional(s2, p_multi, 200)

    result = tree.commit()

    assert result["operations"] == 3

    # Verify specialized operations worked
    # Counter should have value 10
    counter_val = tree.get_triple(s1, p_counter)
    assert counter_val == 10

    # MultiValue should have both values
    multi_vals = tree.get_triple(s2, p_multi)
    assert 100 in multi_vals
    assert 200 in multi_vals


def test_transaction_isolation_from_queries(tree):
    """Test that operations are applied immediately (immediate-apply model)."""
    from cidstore.predicates import CounterStore

    s = E(700)
    p = E(800)

    tree.register_predicate(p, CounterStore)

    tree.begin_transaction()

    # Insert in transaction
    tree.insert_triple_transactional(s, p, 900)

    # Commit
    tree.commit()

    # Should now be visible
    assert tree.get_triple(s, p) == 900


def test_transaction_rollback_after_error(tree):
    """Test rollback cleans up state after operations."""
    from cidstore.predicates import CounterStore

    s = E(1000)
    p = E(1100)

    tree.register_predicate(p, CounterStore)
    tree.register_predicate(E(1300), CounterStore)

    tree.begin_transaction()

    tree.insert_triple_transactional(s, p, 1200)
    tree.insert_triple_transactional(s, E(1300), 1400)

    # Rollback
    tree.rollback()

    # State should be clean
    assert not tree.in_transaction()

    # Can start new transaction
    tree.begin_transaction()
    tree.insert_triple_transactional(s, p, 1500)
    tree.commit()

    assert tree.get_triple(s, p) == 1500


def test_transaction_batch_insert_and_delete(tree):
    """Test transaction combining many inserts and deletes."""
    from cidstore.predicates import CounterStore

    # Setup initial data
    subjects = [E(2000 + i) for i in range(5)]
    predicate = E(3000)

    tree.register_predicate(predicate, CounterStore)

    for i, s in enumerate(subjects):
        tree.insert_triple(s, predicate, 4000 + i)

    # Transaction: delete first 3, insert 5 new
    tree.begin_transaction()

    for s in subjects[:3]:
        val = tree.get_triple(s, predicate)
        tree.delete_triple_transactional(s, predicate, val)

    new_subjects = [E(5000 + i) for i in range(5)]
    for i, s in enumerate(new_subjects):
        tree.insert_triple_transactional(s, predicate, 6000 + i)

    result = tree.commit()

    assert result["operations"] == 8  # 3 deletes + 5 inserts

    # Verify first 3 deleted
    for s in subjects[:3]:
        assert tree.get_triple(s, predicate) is None

    # Verify last 2 still exist
    for i, s in enumerate(subjects[3:], start=3):
        assert tree.get_triple(s, predicate) == 4000 + i

    # Verify new ones inserted
    for i, s in enumerate(new_subjects):
        assert tree.get_triple(s, predicate) == 6000 + i
