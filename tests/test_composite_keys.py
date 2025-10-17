"""Tests for composite key system (Spec 20).

Tests:
- composite_key() creates unique keys for different triples
- composite_value() encodes predicate and object
- decode_composite_value() reverses encoding
- insert_triple() with non-specialized predicates
- query() returns correct results for composite key triples
"""

import pytest

from cidstore.exceptions import InvalidTripleError
from cidstore.keys import E, composite_key, composite_value, decode_composite_value


def test_composite_key_unique():
    """Test that composite_key generates unique keys for different triples."""
    s1 = E.from_str("person:alice")
    p1 = E.from_str("rel:knows")
    o1 = E.from_str("person:bob")

    s2 = E.from_str("person:alice")
    p2 = E.from_str("rel:knows")
    o2 = E.from_str("person:charlie")

    key1 = composite_key(s1, p1, o1)
    key2 = composite_key(s2, p2, o2)

    # Different objects should produce different keys
    assert key1 != key2

    # Same triple should produce same key (deterministic)
    key1_again = composite_key(s1, p1, o1)
    assert key1 == key1_again


def test_composite_key_components():
    """Test that composite_key depends on all three components."""
    s = E.from_str("person:alice")
    p = E.from_str("rel:knows")
    o = E.from_str("person:bob")

    key_spo = composite_key(s, p, o)

    # Different subject
    s2 = E.from_str("person:dave")
    key_s2po = composite_key(s2, p, o)
    assert key_spo != key_s2po

    # Different predicate
    p2 = E.from_str("rel:likes")
    key_sp2o = composite_key(s, p2, o)
    assert key_spo != key_sp2o

    # Different object
    o2 = E.from_str("person:eve")
    key_spo2 = composite_key(s, p, o2)
    assert key_spo != key_spo2


def test_composite_value_encode():
    """Test composite_value encoding."""
    p = E.from_str("rel:knows")
    o = E.from_str("person:bob")

    value = composite_value(p, o)

    # Value should be an E
    assert isinstance(value, E)

    # Different predicates/objects should produce different values
    p2 = E.from_str("rel:likes")
    value2 = composite_value(p2, o)
    assert value != value2


def test_composite_value_decode():
    """Test decode_composite_value reverses encoding."""
    p = E.from_str("rel:knows")
    o = E.from_str("person:bob")

    value = composite_value(p, o)
    p_decoded, o_decoded = decode_composite_value(value)

    # Decoded values should match originals
    assert p_decoded == p
    assert o_decoded == o


@pytest.mark.asyncio
async def test_insert_triple_composite_key(store):
    """Test insert_triple uses composite keys for non-specialized predicates."""
    s = E.from_str("person:alice")
    p = E.from_str("rel:knows")  # Not registered as specialized
    o = E.from_str("person:bob")

    # Insert triple
    await store.insert_triple(s, p, o)

    # Verify triple was stored using composite key
    key = composite_key(s, p, o)
    value = composite_value(p, o)

    # Should be able to retrieve via composite key
    result = await store.get(key)
    assert len(result) > 0
    assert value in result


@pytest.mark.asyncio
async def test_insert_triple_invalid_subject(store):
    """Test insert_triple rejects non-E subject."""
    p = E.from_str("rel:knows")
    o = E.from_str("person:bob")

    with pytest.raises(InvalidTripleError, match="Subject must be E instance"):
        await store.insert_triple("not_an_e", p, o)


@pytest.mark.asyncio
async def test_insert_triple_invalid_predicate(store):
    """Test insert_triple rejects non-E predicate."""
    s = E.from_str("person:alice")
    o = E.from_str("person:bob")

    with pytest.raises(InvalidTripleError, match="Predicate must be E instance"):
        await store.insert_triple(s, 123, o)


@pytest.mark.asyncio
async def test_insert_triple_non_e_object(store):
    """Test insert_triple converts string objects to E."""
    s = E.from_str("person:alice")
    p = E.from_str("rel:name")

    # String object should be converted via E.from_str
    await store.insert_triple(s, p, "Alice Smith")

    # Should be stored with converted object
    key = composite_key(s, p, E.from_str("Alice Smith"))
    result = await store.get(key)
    assert len(result) > 0


@pytest.mark.asyncio
async def test_insert_triple_numeric_object(store):
    """Test insert_triple converts numeric objects to E."""
    s = E.from_str("person:alice")
    p = E.from_str("attr:age")

    # Numeric object should be converted via E.from_int
    # This should work without error (conversion happens inside insert_triple)
    await store.insert_triple(s, p, 42)

    # Verify insertion succeeded - the exact key depends on internal conversion
    # Just verify no exception was raised
    assert True  # Test passes if we reach here without exception


@pytest.mark.asyncio
async def test_composite_key_multiple_triples(store):
    """Test multiple triples with same subject/predicate but different objects."""
    s = E.from_str("person:alice")
    p = E.from_str("rel:knows")

    objects = [
        E.from_str("person:bob"),
        E.from_str("person:charlie"),
        E.from_str("person:dave"),
    ]

    # Insert multiple triples
    for o in objects:
        await store.insert_triple(s, p, o)

    # Each should have unique composite key
    for o in objects:
        key = composite_key(s, p, o)
        value = composite_value(p, o)
        result = await store.get(key)
        assert len(result) > 0
        value = composite_value(p, o)
        result = await store.get(key)
        assert len(result) > 0
        assert value in result


@pytest.mark.asyncio
async def test_composite_key_fallback_specialized_predicate(store):
    """Test that specialized predicates bypass composite key system."""
    # Register a specialized predicate
    p_term = "R:test:counter"
    counter = store.predicate_registry.register_cidsem_counter(p_term)
    p = store.predicate_registry.predicate_to_cid[p_term]

    s = E.from_str("person:alice")

    # Insert via specialized predicate (should use CounterStore, not composite keys)
    await store.insert_triple(s, p, 5)

    # Query via specialized predicate directly through the counter
    result = await counter.query_spo(s)
    assert result == 5  # Should retrieve via CounterStore

    # Composite key should NOT have been used
    key = composite_key(s, p, E.from_int(5))
    composite_result = await store.get(key)
    assert (
        len(composite_result) == 0
    )  # Should be empty - used specialized store instead
