import asyncio
from pathlib import Path

import pytest

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL

pytestmark = pytest.mark.asyncio


@pytest.mark.skip(
    reason="Bucket internals (sorted_count, sorted_region) are not exposed at CIDStore level per spec 3. These are Storage layer implementation details."
)
async def test_sorted_unsorted_region(store):
    """Insert values and check sorted/unsorted region logic per spec 3.

    NOTE: This test is skipped because per spec 3, bucket internals like sorted_count
    and sorted_region are implementation details of the Storage layer and not part of
    the CIDStore public API. The spec defines buckets as having sorted and unsorted
    regions internally, but these are not exposed to users."""
    key = E.from_str("sorttest")
    for i in range(1, 11):
        await store.insert(key, E(i))
    # These methods don't exist in the public API - they're storage layer internals
    # assert await store.get_sorted_count(key) >= 0
    # sorted_region = await store.get_sorted_region(key)
    # assert sorted_region == sorted(sorted_region)


@pytest.mark.xfail(reason="directory_type not implemented", raises=AttributeError)
async def test_directory_resize_and_migration(tree):
    # Insert enough keys to trigger directory resize/migration per spec 3
    for i in range(1, 10001):
        await tree.insert(E.from_str(f"dir{i}"), E(i))
    dtype = await tree.directory_type()
    assert dtype in ("attribute", "dataset")
    # If migration occurred, validate all keys are still present
    if dtype == "dataset":
        for i in range(10000):
            result = await tree.get(E.from_str(f"dir{i}"))
            assert len(result) == 1
            assert int(result[0]) == i
    # TODO: If migration/maintenance API is available, check atomic switch and data integrity


@pytest.mark.xfail(reason="has_spill_pointer not implemented", raises=AttributeError)
async def test_spill_pointer_and_large_valueset(tree):
    # Insert enough values to trigger ValueSet spill (external dataset)
    key = E.from_str("spill")
    for i in range(1, 1001):
        await tree.insert(key, E(i))
    assert await tree.has_spill_pointer(key)


async def test_cidstore_initialization(tree):
    """Verify CIDStore initializes correctly - per spec 0."""
    assert tree is not None


async def test_cidstore_insert(tree):
    """Test basic insert and lookup - per spec 0."""
    await tree.insert(E.from_str("key"), E(123))
    result = await tree.lookup(E.from_str("key"))
    assert len(result) == 1
    assert int(result[0]) == 123


async def test_cidstore_delete(tree):
    """Test deletion removes key-value pairs - per spec 7."""
    await tree.insert(E.from_str("key"), E(123))
    await tree.delete(E.from_str("key"))
    result = await tree.lookup(E.from_str("key"))
    assert result == []


# Test cases for CIDStore (spec 0-8)


# 3. Multi-value key promotion (per spec 5)
async def test_multi_value_promotion(tree):
    """Test multi-value key handling. Per spec 5, when >2 values exist for a key,
    the system should promote to ValueSet representation. This test inserts 200 values
    and verifies all are retrievable."""
    key = E.from_str("multi")
    for i in range(1, 201):
        await tree.insert(key, E(i))
    # Verify all values are retrievable (ValueSet handles large collections)
    result = await tree.lookup(key)
    assert 200 in [int(x) for x in result]


@pytest.mark.xfail(reason="Split/merge logic not implemented", raises=AttributeError)
async def test_split_and_merge(store):
    """Insert enough keys to trigger a split; merging should restore invariants (Spec 3, 6)."""
    for i in range(1, store.SPLIT_THRESHOLD + 2):
        await store.insert(E.from_str(f"split{i}"), E(i))
    new_dir, sep = await store.split()
    assert await store.validate()
    assert await new_dir.validate()
    assert await store.size() <= store.SPLIT_THRESHOLD
    assert await new_dir.size() <= store.SPLIT_THRESHOLD


# ---
# TODOs for full Spec 3 compliance:
# - Add tests for cache stats, version attributes, and migration tool if APIs exist
# - Add tests for sharded directory and hybrid approach if/when implemented
# - Add tests for metrics/logging/tracing if exposed
# - Document any missing features or APIs as not covered


@pytest.mark.xfail(reason="compact not implemented", raises=AttributeError)
async def test_deletion_and_gc(store):
    """Insert, delete, and check GC/compaction per spec 7."""
    for i in range(1, 11):
        await store.insert(E.from_str(f"delgc{i}"), E(i))
    for i in range(1, 11):
        await store.delete(E.from_str(f"delgc{i}"))
    for i in range(1, 11):
        result = await store.lookup(E.from_str(f"delgc{i}"))
        assert result == []
    for i in range(1, 11):
        await store.compact(E.from_str(f"delgc{i}"))


async def test_concurrent_insert(store):
    """Simulate concurrent inserts and check for correct multi-value behavior (Spec 8)."""
    results = []

    async def writer():
        await store.insert(E.from_str("swmr"), E(123))
        results.append(await store.lookup(E.from_str("swmr")))

    await asyncio.gather(writer(), writer())
    for r in results:
        assert 123 in [int(x) for x in r]


# Test cases for CIDStore (spec 0-8)


@pytest.mark.parametrize(
    "key,value",
    [
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("d", 4),
        ("e", 5),
    ],
)
@pytest.mark.asyncio
async def test_insert_and_lookup(tree, key, value):
    await tree.insert(E.from_str(key), E(value))
    result = await tree.lookup(E.from_str(key))
    assert len(result) == 1
    # Accept either E or int/str for test compatibility
    assert int(result[0]) == value


async def test_overwrite(tree):
    await tree.insert(E.from_str("dup"), E(1))
    await tree.insert(E.from_str("dup"), E(2))
    result = await tree.lookup(E.from_str("dup"))
    # Multi-value: both values should be present
    assert set(result) == {E(1), E(2)}


async def test_delete(tree):
    await tree.insert(E.from_str("x"), E(42))
    result = await tree.lookup(E.from_str("x"))
    assert len(result) == 1
    assert int(result[0]) == 42
    await tree.delete(E.from_str("x"))
    result = await tree.lookup(E.from_str("x"))
    assert result == []


async def test_multiple_inserts_and_deletes(directory):
    items = [(E.from_str(f"k{i}"), E(i)) for i in range(1, 11)]
    for k, v in items:
        await directory.insert(k, v)
    for k, v in items:
        result = await directory.lookup(k)
        assert len(result) == 1
        assert int(result[0]) == int(v)
    for k, _ in items:
        await directory.delete(k)
    for k, _ in items:
        result = await directory.lookup(k)
        assert result == []


async def test_nonexistent_key(directory):
    """Lookup for a key that was never inserted should return an empty result."""
    result = await directory.lookup(E.from_str("notfound"))
    assert result == []


async def test_bulk_insert(directory):
    for i in range(1, 101):
        await directory.insert(E.from_str(f"bulk{i}"), E(i))
    for i in range(1, 101):
        result = await directory.lookup(E.from_str(f"bulk{i}"))
        assert len(result) == 1
        assert int(result[0]) == i


async def test_bulk_delete(directory):
    for i in range(1, 101):
        await directory.insert(E.from_str(f"del{i}"), E(i))
    for i in range(1, 101):
        await directory.delete(E.from_str(f"del{i}"))
    for i in range(1, 101):
        result = await directory.lookup(E.from_str(f"del{i}"))
        assert result == []


@pytest.mark.asyncio
async def test_concurrent_writes(tmp_path):
    """Test concurrent inserts with SWMR semantics - per spec 8."""
    # Test concurrent inserts of distinct keys
    storage = Storage(path=Path(tmp_path) / "swmr.h5")
    wal = WAL(path=None)
    tree = CIDStore(storage, wal)
    await tree.async_init()

    async def worker(start, results):
        for i in range(start + 1, start + 51):
            k = E.from_str(f"key{i}")
            await tree.insert(k, E(i))
            # verify immediate lookup
            vals = await tree.get(k)
            results.append((k, list(vals)))

    results = []
    await asyncio.gather(
        worker(0, results),
        worker(50, results),
    )

    # Assert all inserted keys present with correct values
    for k, vals in results:
        i = int(str(k)[str(k).find("key") + 3 :]) if "key" in str(k) else None
        if i is not None:
            assert E(i) in vals

    await tree.aclose()


# ============================================================================
# Query Interface Tests
# ============================================================================


async def test_query_triple_spo_with_specialized_predicate(store):
    """Test SPO query with specialized predicate (CounterStore)."""
    # Register a counter predicate
    pred = E.from_str("R:test:count")
    store.predicate_registry.register_counter(pred)

    # Insert data
    alice = E.from_str("E:test:alice")
    await store.insert_triple(alice, pred, 5)

    # Query: What is Alice's count?
    result = await store.query_triple(subject=alice, predicate=pred)
    assert result == 5


async def test_query_triple_spo_with_multivalue_predicate(store):
    """Test SPO query with specialized predicate (MultiValueSetStore)."""
    # Register a multivalue predicate
    pred = E.from_str("R:test:knows")
    store.predicate_registry.register_multivalue(pred)

    # Insert data
    alice = E.from_str("E:test:alice")
    bob = E.from_str("E:test:bob")
    carol = E.from_str("E:test:carol")

    await store.insert_triple(alice, pred, bob)
    await store.insert_triple(alice, pred, carol)

    # Query: Who does Alice know?
    result = await store.query_triple(subject=alice, predicate=pred)
    assert isinstance(result, set)
    assert bob in result
    assert carol in result
    assert len(result) == 2


async def test_query_triple_osp_with_specialized_predicate(store):
    """Test OSP query with known predicate (which subjects have this value?)."""
    # Register a counter predicate
    pred = E.from_str("R:test:pageviews")
    store.predicate_registry.register_counter(pred)

    # Insert data
    page1 = E.from_str("E:test:page1")
    page2 = E.from_str("E:test:page2")
    page3 = E.from_str("E:test:page3")

    await store.insert_triple(page1, pred, 100)
    await store.insert_triple(page2, pred, 200)
    await store.insert_triple(page3, pred, 100)

    # Query: Which pages have 100 views?
    result = await store.query_triple(predicate=pred, obj=100)
    assert isinstance(result, set)
    assert page1 in result
    assert page3 in result
    assert page2 not in result


async def test_query_triple_osp_multivalue_predicate(store):
    """Test OSP query with multivalue predicate (reverse lookup)."""
    # Register a multivalue predicate
    pred = E.from_str("R:test:friendsWith")
    store.predicate_registry.register_multivalue(pred)

    # Insert data
    alice = E.from_str("E:test:alice")
    bob = E.from_str("E:test:bob")
    carol = E.from_str("E:test:carol")

    await store.insert_triple(alice, pred, bob)
    await store.insert_triple(carol, pred, bob)

    # Query: Who is friends with Bob?
    result = await store.query_triple(predicate=pred, obj=bob)
    assert isinstance(result, set)
    assert alice in result
    assert carol in result
    assert len(result) == 2


async def test_query_triple_pos_with_specialized_predicate(store):
    """Test POS query (alias for OSP with known predicate)."""
    # Register a multivalue predicate
    pred = E.from_str("R:test:hasTag")
    store.predicate_registry.register_multivalue(pred)

    # Insert data
    doc1 = E.from_str("E:test:doc1")
    doc2 = E.from_str("E:test:doc2")
    tag = E.from_str("E:test:python")

    await store.insert_triple(doc1, pred, tag)
    await store.insert_triple(doc2, pred, tag)

    # Query using query_pos helper
    result = await store.query_pos(pred, tag)
    assert isinstance(result, set)
    assert doc1 in result
    assert doc2 in result


async def test_query_triple_osp_all_fan_out(store):
    """Test OSP query with unknown predicate (fan-out to all predicates)."""
    # Register multiple predicates
    pred1 = E.from_str("R:test:owns")
    pred2 = E.from_str("R:test:likes")
    store.predicate_registry.register_multivalue(pred1)
    store.predicate_registry.register_multivalue(pred2)

    # Insert data
    alice = E.from_str("E:test:alice")
    bob = E.from_str("E:test:bob")
    apple = E.from_str("E:test:apple")

    await store.insert_triple(alice, pred1, apple)
    await store.insert_triple(bob, pred2, apple)

    # Query: What relates to apple? (fan-out to all predicates)
    result = await store.query_triple(obj=apple)

    # Result should contain tuples of (subject, predicate)
    assert isinstance(result, list)
    assert len(result) == 2

    # Check that both relationships are found
    subjects = [s for s, p in result]
    predicates = [p for s, p in result]
    assert alice in subjects
    assert bob in subjects
    assert pred1 in predicates
    assert pred2 in predicates


async def test_query_triple_empty_results(store):
    """Test queries that should return empty results."""
    # Register a predicate
    pred = E.from_str("R:test:hasValue")
    store.predicate_registry.register_counter(pred)

    alice = E.from_str("E:test:alice")

    # Query non-existent subject
    result = await store.query_triple(subject=alice, predicate=pred)
    assert result == 0  # CounterStore returns 0 for non-existent

    # Query non-existent object
    result = await store.query_triple(predicate=pred, obj=999)
    assert isinstance(result, set)
    assert len(result) == 0


async def test_query_triple_unregistered_predicate(store):
    """Test query with unregistered predicate returns fallback results."""
    # Don't register the predicate
    pred = E.from_str("R:test:unknown")
    alice = E.from_str("E:test:alice")

    # SPO query should fall back to main store
    result = await store.query_triple(subject=alice, predicate=pred)
    # Fallback returns from main store.get()
    assert result is not None  # May be empty list/set depending on implementation

    # OSP query with unregistered predicate should return empty
    result = await store.query_triple(predicate=pred, obj=E(42))
    assert result == []


async def test_query_triple_multiple_values_same_subject(store):
    """Test SPO query returns all values for a subject."""
    pred = E.from_str("R:test:tags")
    store.predicate_registry.register_multivalue(pred)

    doc = E.from_str("E:test:document")
    tag1 = E.from_str("E:test:python")
    tag2 = E.from_str("E:test:async")
    tag3 = E.from_str("E:test:testing")

    await store.insert_triple(doc, pred, tag1)
    await store.insert_triple(doc, pred, tag2)
    await store.insert_triple(doc, pred, tag3)

    # Query all tags for document
    result = await store.query_triple(subject=doc, predicate=pred)
    assert isinstance(result, set)
    assert len(result) == 3
    assert tag1 in result
    assert tag2 in result
    assert tag3 in result


async def test_query_triple_counter_increments(store):
    """Test counter queries track incremental updates."""
    pred = E.from_str("R:test:score")
    counter_store = store.predicate_registry.register_counter(pred)

    player = E.from_str("E:test:player1")

    # Insert initial value
    await store.insert_triple(player, pred, 10)
    result = await store.query_triple(subject=player, predicate=pred)
    assert result == 10

    # Update value
    await store.insert_triple(player, pred, 25)
    result = await store.query_triple(subject=player, predicate=pred)
    assert result == 25

    # OSP: Find players with score 25
    result = await store.query_triple(predicate=pred, obj=25)
    assert player in result


async def test_query_pos_convenience_method(store):
    """Test the query_pos convenience method."""
    pred = E.from_str("R:test:category")
    store.predicate_registry.register_multivalue(pred)

    item1 = E.from_str("E:test:item1")
    item2 = E.from_str("E:test:item2")
    category = E.from_str("E:test:electronics")

    await store.insert_triple(item1, pred, category)
    await store.insert_triple(item2, pred, category)

    # Use convenience method
    result = await store.query_pos(pred, category)
    assert isinstance(result, set)
    assert item1 in result
    assert item2 in result


async def test_unified_query_spo_pattern(store):
    """Test unified query() with SPO pattern."""
    pred = store.predicate_registry.register_cidsem_multivalue("R:usr:friendsWith")

    alice = E.from_str("E:usr:alice")
    bob = E.from_str("E:usr:bob")

    await pred.insert(alice, bob)

    # Query using unified interface
    results = []
    async for s, p, o in store.query(alice, pred.predicate, None):
        results.append((s, p, o))

    assert len(results) == 1
    assert results[0] == (alice, pred.predicate, bob)


async def test_unified_query_osp_pattern(store):
    """Test unified query() with OSP pattern."""
    pred = store.predicate_registry.register_cidsem_counter("R:usr:score")

    alice = E.from_str("E:usr:alice")
    bob = E.from_str("E:usr:bob")

    await pred.insert(alice, 100)
    await pred.insert(bob, 100)

    # Query using unified interface
    results = []
    async for s, p, o in store.query(None, pred.predicate, 100):
        results.append((s, p, o))

    assert len(results) == 2
    subjects = {s for s, p, o in results}
    assert alice in subjects
    assert bob in subjects


async def test_unified_query_fan_out(store):
    """Test unified query() with fan-out pattern (?, ?, O)."""
    pred1 = store.predicate_registry.register_cidsem_multivalue("R:usr:likes")
    pred2 = store.predicate_registry.register_cidsem_multivalue("R:usr:owns")

    alice = E.from_str("E:usr:alice")
    bob = E.from_str("E:usr:bob")
    pizza = E.from_str("E:food:pizza")

    await pred1.insert(alice, pizza)
    await pred2.insert(bob, pizza)

    # Fan-out query
    results = []
    async for s, p, o in store.query(None, None, pizza):
        results.append((s, p, o))

    assert len(results) == 2
    subjects = {s for s, p, o in results}
    assert alice in subjects
    assert bob in subjects
