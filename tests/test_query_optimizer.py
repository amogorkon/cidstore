"""Tests for unified query interface with pattern matching (Task 14)."""
import pytest
from cidstore.keys import E


@pytest.mark.asyncio
async def test_query_spo_pattern(store):
    """Test (S, P, ?) query pattern."""
    from cidstore.predicates import CounterStore
    
    counter_pred = E.from_str("counter_pred")
    counter_ds = store.predicate_registry.register_counter(counter_pred)
    
    alice = E.from_str("alice")
    await store.insert_triple(alice, counter_pred, 5)
    
    results = []
    async for s, p, o in store.query(alice, counter_pred, None):
        results.append((s, p, o))
    
    assert len(results) == 1
    assert results[0] == (alice, counter_pred, 5)


@pytest.mark.asyncio
async def test_query_osp_pattern(store):
    """Test (?, P, O) query pattern (reverse lookup)."""
    from cidstore.predicates import MultiValueSetStore
    
    friends_pred = E.from_str("friendsWith")
    friends_ds = store.predicate_registry.register_multivalue(friends_pred)
    
    alice = E.from_str("alice")
    bob = E.from_str("bob")
    charlie = E.from_str("charlie")
    
    await store.insert_triple(alice, friends_pred, bob)
    await store.insert_triple(charlie, friends_pred, bob)
    
    results = []
    async for s, p, o in store.query(None, friends_pred, bob):
        results.append((s, p, o))
    
    assert len(results) == 2
    subjects = {s for s, _, _ in results}
    assert alice in subjects
    assert charlie in subjects


@pytest.mark.asyncio
async def test_query_fanout_pattern(store):
    """Test (?, ?, O) query pattern (fan-out to all predicates)."""
    from cidstore.predicates import MultiValueSetStore
    
    friends_pred = E.from_str("friendsWith")
    knows_pred = E.from_str("knows")
    
    store.predicate_registry.register_multivalue(friends_pred)
    store.predicate_registry.register_multivalue(knows_pred)
    
    alice = E.from_str("alice")
    charlie = E.from_str("charlie")
    bob = E.from_str("bob")
    
    await store.insert_triple(alice, friends_pred, bob)
    await store.insert_triple(charlie, knows_pred, bob)
    
    results = []
    async for s, p, o in store.query(None, None, bob):
        results.append((s, p, o))
    
    assert len(results) == 2
    predicates = {p for _, p, _ in results}
    assert friends_pred in predicates
    assert knows_pred in predicates


@pytest.mark.asyncio
async def test_query_subject_scan_pattern(store):
    """Test (S, ?, ?) query pattern (scan all predicates for subject)."""
    from cidstore.predicates import CounterStore, MultiValueSetStore
    
    age_pred = E.from_str("age")
    friends_pred = E.from_str("friendsWith")
    
    store.predicate_registry.register_counter(age_pred)
    store.predicate_registry.register_multivalue(friends_pred)
    
    alice = E.from_str("alice")
    bob = E.from_str("bob")
    
    await store.insert_triple(alice, age_pred, 30)
    await store.insert_triple(alice, friends_pred, bob)
    
    results = []
    async for s, p, o in store.query(alice, None, None):
        results.append((s, p, o))
    
    assert len(results) == 2
    predicates = {p for _, p, _ in results}
    assert age_pred in predicates
    assert friends_pred in predicates


@pytest.mark.asyncio
async def test_query_exact_triple(store):
    """Test (S, P, O) query pattern (exact triple check)."""
    from cidstore.predicates import MultiValueSetStore
    
    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)
    
    alice = E.from_str("alice")
    bob = E.from_str("bob")
    charlie = E.from_str("charlie")
    
    await store.insert_triple(alice, friends_pred, bob)
    await store.insert_triple(alice, friends_pred, charlie)
    
    # Query for exact triple that exists
    results = []
    async for s, p, o in store.query(alice, friends_pred, bob):
        results.append((s, p, o))
    
    assert len(results) == 1
    assert results[0] == (alice, friends_pred, bob)
    
    # Query for exact triple that doesn't exist
    dave = E.from_str("dave")
    results = []
    async for s, p, o in store.query(alice, friends_pred, dave):
        results.append((s, p, o))
    
    assert len(results) == 0


@pytest.mark.asyncio
async def test_query_full_scan_warning(store):
    """Test (?, ?, ?) query pattern warns about full scan."""
    import warnings
    results = []
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        async for triple in store.query(None, None, None):
            results.append(triple)
        
        assert len(w) == 1
        assert "expensive" in str(w[0].message).lower()


@pytest.mark.asyncio
async def test_query_no_results(store):
    """Test query with no matching results."""
    from cidstore.predicates import MultiValueSetStore
    
    friends_pred = E.from_str("friendsWith")
    store.predicate_registry.register_multivalue(friends_pred)
    
    alice = E.from_str("alice")
    bob = E.from_str("bob")
    
    results = []
    async for triple in store.query(alice, friends_pred, None):
        results.append(triple)
    
    assert len(results) == 0


@pytest.mark.asyncio
async def test_query_mixed_types(store):
    """Test query with mixed result types (int and E)."""
    from cidstore.predicates import CounterStore, MultiValueSetStore
    
    age_pred = E.from_str("age")
    friends_pred = E.from_str("friendsWith")
    
    store.predicate_registry.register_counter(age_pred)
    store.predicate_registry.register_multivalue(friends_pred)
    
    alice = E.from_str("alice")
    bob = E.from_str("bob")
    charlie = E.from_str("charlie")
    
    await store.insert_triple(alice, age_pred, 30)
    await store.insert_triple(alice, friends_pred, bob)
    await store.insert_triple(alice, friends_pred, charlie)
    
    results = []
    async for s, p, o in store.query(alice, None, None):
        results.append((s, p, o))
    
    assert len(results) == 3
    
    # Check counter result
    age_results = [o for s, p, o in results if p == age_pred]
    assert len(age_results) == 1
    assert isinstance(age_results[0], int)
    assert age_results[0] == 30
    
    # Check multivalue results
    friend_results = [o for s, p, o in results if p == friends_pred]
    assert len(friend_results) == 2
    assert all(isinstance(o, E) for o in friend_results)
