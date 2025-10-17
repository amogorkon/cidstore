"""Tests for batch operations (Task 15)."""

import pytest

from cidstore.keys import E


@pytest.mark.asyncio
async def test_batch_insert_basic(store):
    """Basic batch insert functionality."""
    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)

    alice = E.from_str("alice")
    bob = E.from_str("bob")
    charlie = E.from_str("charlie")

    triples = [
        (alice, friends_pred, bob),
        (alice, friends_pred, charlie),
        (bob, friends_pred, alice),
    ]

    stats = await store.insert_triples_batch(triples)

    assert stats["inserted"] == 3
    assert stats["failed"] == 0
    assert stats["specialized"] == 3
    assert stats["composite"] == 0


@pytest.mark.asyncio
async def test_batch_insert_mixed_predicates(store):
    """Batch insert with multiple predicate types."""
    age_pred = E.from_str("age")
    friends_pred = E.from_str("friendsWith")

    store.predicate_registry.register_counter(age_pred)
    store.predicate_registry.register_multivalue(friends_pred)

    alice = E.from_str("alice")
    bob = E.from_str("bob")
    charlie = E.from_str("charlie")

    triples = [
        (alice, age_pred, 30),
        (alice, friends_pred, bob),
        (bob, age_pred, 25),
        (bob, friends_pred, charlie),
    ]

    stats = await store.insert_triples_batch(triples)

    assert stats["inserted"] == 4
    assert stats["specialized"] == 4

    # Verify data was inserted correctly
    age_alice = await store.query_triple(alice, age_pred, None)
    assert age_alice == 30

    friends_alice = await store.query_triple(alice, friends_pred, None)
    assert bob in friends_alice


@pytest.mark.asyncio
async def test_batch_insert_composite_keys(store):
    """Batch insert with composite key system."""
    custom_pred = E.from_str("customPredicate")
    another_pred = E.from_str("anotherPredicate")

    alice = E.from_str("alice")
    bob = E.from_str("bob")
    charlie = E.from_str("charlie")

    triples = [
        (alice, custom_pred, bob),
        (alice, another_pred, charlie),
        (bob, custom_pred, charlie),
    ]

    stats = await store.insert_triples_batch(triples)

    assert stats["inserted"] == 3
    assert stats["composite"] == 3
    assert stats["specialized"] == 0


@pytest.mark.asyncio
async def test_batch_insert_empty_list(store):
    """Batch insert with empty list."""
    stats = await store.insert_triples_batch([])

    assert stats["inserted"] == 0
    assert stats["failed"] == 0
    assert stats["specialized"] == 0
    assert stats["composite"] == 0


@pytest.mark.asyncio
async def test_batch_insert_atomic_success(store):
    """Atomic mode: all insertions succeed."""
    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)

    alice = E.from_str("alice")
    bob = E.from_str("bob")

    triples = [
        (alice, friends_pred, bob),
        (bob, friends_pred, alice),
    ]

    stats = await store.insert_triples_batch(triples, atomic=True)

    assert stats["inserted"] == 2
    assert stats["failed"] == 0


@pytest.mark.asyncio
async def test_batch_insert_atomic_failure(store):
    """Atomic mode: validation error causes all to fail."""
    from cidstore.exceptions import InvalidTripleError

    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)

    alice = E.from_str("alice")
    bob = E.from_str("bob")

    triples = [
        (alice, friends_pred, bob),
        ("not_an_E", friends_pred, bob),  # Invalid!
    ]

    with pytest.raises(InvalidTripleError):
        await store.insert_triples_batch(triples, atomic=True)


@pytest.mark.asyncio
async def test_batch_insert_non_atomic(store):
    """Non-atomic mode: continues on errors."""
    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)

    alice = E.from_str("alice")
    bob = E.from_str("bob")

    triples = [
        (alice, friends_pred, bob),
        ("not_an_E", friends_pred, bob),  # Invalid - will be skipped
        (bob, friends_pred, alice),
    ]

    stats = await store.insert_triples_batch(triples, atomic=False)

    assert stats["inserted"] == 2
    assert stats["failed"] == 1


@pytest.mark.asyncio
async def test_batch_insert_large_batch(store):
    """Batch insert with large number of triples."""
    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)

    triples = []
    for i in range(100):
        subject = E.from_str(f"person_{i}")
        for j in range(10):
            friend = E.from_str(f"person_{(i + j + 1) % 100}")
            triples.append((subject, friends_pred, friend))

    stats = await store.insert_triples_batch(triples)

    assert stats["inserted"] == 1000
    assert stats["failed"] == 0
    assert stats["specialized"] == 1000


@pytest.mark.asyncio
async def test_batch_insert_returns_statistics(store):
    """Verify batch insert returns accurate statistics."""
    age_pred = E.from_str("age")
    custom_pred = E.from_str("customPred")

    store.predicate_registry.register_counter(age_pred)

    alice = E.from_str("alice")
    bob = E.from_str("bob")

    triples = [
        (alice, age_pred, 30),
        (bob, age_pred, 25),
        (alice, custom_pred, bob),
    ]

    stats = await store.insert_triples_batch(triples)

    assert stats["inserted"] == 3
    assert stats["specialized"] == 2
    assert stats["composite"] == 1
    assert stats["failed"] == 0
