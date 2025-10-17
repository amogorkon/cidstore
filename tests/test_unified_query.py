"""Tests for unified query interface (CIDStore.query method).

Tests all query patterns: SPO, OSP, POS, S??, ??O, and dispatch to plugins.
"""

import pytest

from cidstore.keys import E
from cidstore.predicates import PredicateRegistry


@pytest.mark.asyncio
async def test_query_spo_pattern(store):
    """Test SPO pattern: (subject, predicate, ?) lookup."""
    # Register a predicate
    pred = store.predicate_registry.register_cidsem_multivalue(
        "R:usr:friendsWith", "Social graph"
    )

    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)

    # Insert data
    await pred.insert(alice, bob)
    await pred.insert(alice, charlie)

    # Query SPO pattern
    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:friendsWith"]
    results = []
    async for s, p, o in store.query(alice, pred_cid, None):
        results.append((s, p, o))

    # Should find both bob and charlie
    assert len(results) == 2
    objects = {o for s, p, o in results}
    assert bob in objects
    assert charlie in objects


@pytest.mark.asyncio
async def test_query_osp_pattern(store):
    """Test OSP pattern: (?, predicate, object) lookup."""
    pred = store.predicate_registry.register_cidsem_multivalue("R:usr:likes")

    alice = E(1, 1)
    bob = E(2, 2)
    pizza = E(10, 10)

    await pred.insert(alice, pizza)
    await pred.insert(bob, pizza)

    # Query OSP pattern: who likes pizza?
    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:likes"]
    results = []
    async for s, p, o in store.query(None, pred_cid, pizza):
        results.append((s, p, o))

    assert len(results) == 2
    subjects = {s for s, p, o in results}
    assert alice in subjects
    assert bob in subjects


@pytest.mark.asyncio
async def test_query_counter_spo(store):
    """Test SPO query with CounterStore plugin."""
    counter = store.predicate_registry.register_cidsem_counter(
        "R:usr:score", "Score tracking"
    )

    alice = E(1, 1)
    bob = E(2, 2)

    await counter.insert(alice, 100)
    await counter.insert(bob, 200)

    # Query alice's score
    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:score"]
    results = []
    async for s, p, o in store.query(alice, pred_cid, None):
        results.append((s, p, o))

    assert len(results) == 1
    assert results[0] == (alice, pred_cid, 100)


@pytest.mark.asyncio
async def test_query_counter_osp(store):
    """Test OSP query with CounterStore plugin."""
    counter = store.predicate_registry.register_cidsem_counter("R:usr:age")

    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)

    await counter.insert(alice, 30)
    await counter.insert(bob, 25)
    await counter.insert(charlie, 30)

    # Query: who is 30 years old?
    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:age"]
    results = []
    async for s, p, o in store.query(None, pred_cid, 30):
        results.append((s, p, o))

    assert len(results) == 2
    subjects = {s for s, p, o in results}
    assert alice in subjects
    assert charlie in subjects


@pytest.mark.asyncio
async def test_query_fan_out_pattern(store):
    """Test fan-out pattern: (?, ?, object) across all predicates."""
    pred1 = store.predicate_registry.register_cidsem_multivalue("R:usr:friendsWith")
    pred2 = store.predicate_registry.register_cidsem_multivalue("R:usr:follows")

    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)

    await pred1.insert(alice, charlie)  # alice friends with charlie
    await pred2.insert(bob, charlie)  # bob follows charlie

    # Query: all relationships involving charlie
    results = []
    async for s, p, o in store.query(None, None, charlie):
        results.append((s, p, o))

    assert len(results) == 2
    subjects = {s for s, p, o in results}
    assert alice in subjects
    assert bob in subjects


@pytest.mark.asyncio
async def test_query_subject_all_pattern(store):
    """Test S?? pattern: (subject, ?, ?) - all predicates for subject."""
    pred1 = store.predicate_registry.register_cidsem_multivalue("R:usr:friendsWith")
    pred2 = store.predicate_registry.register_cidsem_counter("R:usr:age")

    alice = E(1, 1)
    bob = E(2, 2)

    await pred1.insert(alice, bob)
    await pred2.insert(alice, 30)

    # Query: all predicates for alice
    results = []
    async for s, p, o in store.query(alice, None, None):
        results.append((s, p, o))

    # Should find both friendsWith and age
    assert len(results) == 2
    predicates = {p for s, p, o in results}
    pred1_cid = store.predicate_registry.predicate_to_cid["R:usr:friendsWith"]
    pred2_cid = store.predicate_registry.predicate_to_cid["R:usr:age"]
    assert pred1_cid in predicates
    assert pred2_cid in predicates


@pytest.mark.asyncio
async def test_query_full_scan_not_implemented(store):
    """Test that full scan pattern (?, ?, ?) is not implemented."""
    # Full scan should return empty (not implemented)
    results = []
    async for s, p, o in store.query(None, None, None):
        results.append((s, p, o))

    # Should be empty - full scan not supported
    assert len(results) == 0


@pytest.mark.asyncio
async def test_query_unsupported_pattern_raises(store):
    """Test that unsupported query patterns raise ValueError."""
    alice = E(1, 1)
    pred_cid = E(5, 5)
    obj = E(10, 10)

    # Pattern: (subject, ?, object) is not supported
    with pytest.raises(ValueError, match="Unsupported query pattern"):
        async for _ in store.query(alice, None, obj):
            pass


@pytest.mark.asyncio
async def test_query_empty_results(store):
    """Test query returns empty when no matches found."""
    pred = store.predicate_registry.register_cidsem_multivalue("R:usr:knows")

    alice = E(1, 1)
    bob = E(2, 2)

    # No data inserted

    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:knows"]
    results = []
    async for s, p, o in store.query(alice, pred_cid, None):
        results.append((s, p, o))

    assert len(results) == 0


@pytest.mark.asyncio
async def test_query_multivalue_set_multiple_inserts(store):
    """Test querying multivalue set with multiple values."""
    pred = store.predicate_registry.register_cidsem_multivalue("R:usr:skills")

    alice = E(1, 1)
    python = E(10, 1)
    rust = E(10, 2)
    go = E(10, 3)

    await pred.insert(alice, python)
    await pred.insert(alice, rust)
    await pred.insert(alice, go)

    # Query all skills for alice
    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:skills"]
    results = []
    async for s, p, o in store.query(alice, pred_cid, None):
        results.append((s, p, o))

    assert len(results) == 3
    objects = {o for s, p, o in results}
    assert python in objects
    assert rust in objects
    assert go in objects


@pytest.mark.asyncio
async def test_query_respects_plugin_supports_flags(store):
    """Test that query respects plugin's supports_osp/supports_pos flags."""
    counter = store.predicate_registry.register_cidsem_counter("R:usr:count")

    alice = E(1, 1)
    await counter.insert(alice, 5)

    # Counter supports OSP
    assert counter.supports_osp is True

    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:count"]
    results = []
    async for s, p, o in store.query(None, pred_cid, 5):
        results.append((s, p, o))

    assert len(results) == 1
    assert results[0][0] == alice


@pytest.mark.asyncio
async def test_query_async_iteration(store):
    """Test that query properly yields results via async iteration."""
    pred = store.predicate_registry.register_cidsem_multivalue("R:usr:visited")

    alice = E(1, 1)
    cities = [E(i, i) for i in range(10, 20)]

    for city in cities:
        await pred.insert(alice, city)

    # Query using async for
    pred_cid = store.predicate_registry.predicate_to_cid["R:usr:visited"]
    count = 0
    async for s, p, o in store.query(alice, pred_cid, None):
        count += 1
        assert s == alice
        assert p == pred_cid
        assert o in cities

    assert count == 10


@pytest.mark.asyncio
async def test_query_mixed_predicates(store):
    """Test querying with both counter and multivalue predicates."""
    counter = store.predicate_registry.register_cidsem_counter("R:usr:points")
    multivalue = store.predicate_registry.register_cidsem_multivalue("R:usr:badges")

    alice = E(1, 1)
    badge1 = E(20, 1)
    badge2 = E(20, 2)

    await counter.insert(alice, 1000)
    await multivalue.insert(alice, badge1)
    await multivalue.insert(alice, badge2)

    # Query all predicates for alice
    results = []
    async for s, p, o in store.query(alice, None, None):
        results.append((s, p, o))

    # Should have 3 results: 1 counter + 2 badges
    assert len(results) == 3

    # Check we have both predicate types
    predicates = {p for s, p, o in results}
    points_cid = store.predicate_registry.predicate_to_cid["R:usr:points"]
    badges_cid = store.predicate_registry.predicate_to_cid["R:usr:badges"]
    assert points_cid in predicates
    assert badges_cid in predicates
