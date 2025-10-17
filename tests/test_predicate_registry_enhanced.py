"""Tests for PredicateRegistry enhancements (Spec 13: Plugin Infrastructure).

Tests load_from_config(), query_osp_parallel(), audit_all_plugins(), and CIDSem validation.
"""

import asyncio

import pytest

from cidstore.keys import E
from cidstore.predicates import CounterStore, PredicateRegistry


@pytest.mark.asyncio
async def test_load_from_config_basic():
    """Test loading predicates from configuration."""
    registry = PredicateRegistry()

    config = {
        "predicates": [
            {
                "name": "R:usr:ownedQuantity",
                "plugin": "counter",
                "config": {},
                "justification": "O(1) aggregation",
            }
        ]
    }

    await registry.load_from_config(config)

    # Verify predicate was registered
    pred_name = "R:usr:ownedQuantity"
    assert pred_name in registry.predicate_to_cid
    pred_cid = registry.predicate_to_cid[pred_name]
    assert registry.get(pred_cid) is not None
    assert isinstance(registry.get(pred_cid), CounterStore)


@pytest.mark.asyncio
async def test_load_from_config_multiple_predicates():
    """Test loading multiple predicates from config."""
    registry = PredicateRegistry()

    config = {
        "predicates": [
            {
                "name": "R:usr:ownedQuantity",
                "plugin": "counter",
                "config": {},
                "justification": "O(1) aggregation",
            },
            {
                "name": "R:usr:friendsWith",
                "plugin": "multivalue_set",
                "config": {},
                "justification": "Bidirectional queries",
            },
        ]
    }

    await registry.load_from_config(config)

    assert "R:usr:ownedQuantity" in registry.predicate_to_cid
    assert "R:usr:friendsWith" in registry.predicate_to_cid
    assert len(registry.all_predicates()) == 2


@pytest.mark.asyncio
async def test_load_from_config_invalid_cidsem_raises():
    """Test that invalid CIDSem format raises ValueError."""
    registry = PredicateRegistry()

    config = {
        "predicates": [
            {
                "name": "invalid_format",  # Missing colons
                "plugin": "counter",
                "config": {},
            }
        ]
    }

    with pytest.raises(ValueError, match="Invalid CIDSem format"):
        await registry.load_from_config(config)


@pytest.mark.asyncio
async def test_load_from_config_unknown_plugin_raises():
    """Test that unknown plugin raises ValueError."""
    registry = PredicateRegistry()

    config = {
        "predicates": [
            {
                "name": "R:usr:test",
                "plugin": "nonexistent_plugin",
                "config": {},
            }
        ]
    }

    with pytest.raises(ValueError, match="Unknown plugin"):
        await registry.load_from_config(config)


@pytest.mark.asyncio
async def test_load_from_config_with_concurrency_limit():
    """Test loading config with concurrency_limit creates semaphore."""
    registry = PredicateRegistry()

    config = {
        "predicates": [
            {
                "name": "R:usr:limited",
                "plugin": "counter",
                "config": {"concurrency_limit": 5},
                "justification": "Rate-limited",
            }
        ]
    }

    await registry.load_from_config(config)

    pred_cid = registry.predicate_to_cid["R:usr:limited"]
    assert pred_cid in registry._semaphores
    semaphore = registry._semaphores[pred_cid]
    assert isinstance(semaphore, asyncio.Semaphore)


@pytest.mark.asyncio
async def test_query_osp_parallel_basic():
    """Test parallel OSP query across multiple predicates."""
    registry = PredicateRegistry()

    # Register two predicates
    pred1 = registry.register_cidsem_multivalue("R:usr:friendsWith")
    pred2 = registry.register_cidsem_multivalue("R:usr:follows")

    # Insert test data
    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)

    await pred1.insert(alice, bob)  # alice friends with bob
    await pred2.insert(charlie, bob)  # charlie follows bob

    # Query: who has bob as object?
    results = []
    async for triple in registry.query_osp_parallel(bob):
        results.append(triple)

    # Should find both relationships
    assert len(results) == 2
    subjects = {s for s, p, o in results}
    assert alice in subjects
    assert charlie in subjects


@pytest.mark.asyncio
async def test_query_osp_parallel_with_subject_filter():
    """Test OSP query with subject filter."""
    registry = PredicateRegistry()

    pred = registry.register_cidsem_multivalue("R:usr:likes")

    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)
    pizza = E(10, 10)

    await pred.insert(alice, pizza)
    await pred.insert(bob, pizza)
    await pred.insert(charlie, pizza)

    # Filter to only alice and bob
    subject_filter = {alice, bob}

    results = []
    async for triple in registry.query_osp_parallel(pizza, subject_filter):
        results.append(triple)

    subjects = {s for s, p, o in results}
    assert subjects == {alice, bob}
    assert charlie not in subjects


@pytest.mark.asyncio
async def test_query_osp_parallel_respects_supports_osp():
    """Test that OSP query skips plugins that don't support OSP."""
    registry = PredicateRegistry()

    # CounterStore supports OSP
    counter = registry.register_cidsem_counter("R:usr:count")
    alice = E(1, 1)
    await counter.insert(alice, 5)

    # Query should work
    results = []
    async for triple in registry.query_osp_parallel(5):
        results.append(triple)

    assert len(results) == 1
    assert results[0][0] == alice


@pytest.mark.asyncio
async def test_query_osp_parallel_handles_exceptions():
    """Test that query continues even if one predicate fails."""
    registry = PredicateRegistry()

    # Create two predicates
    pred1 = registry.register_cidsem_multivalue("R:usr:test1")
    pred2 = registry.register_cidsem_multivalue("R:usr:test2")

    alice = E(1, 1)
    bob = E(2, 2)
    value = E(10, 10)

    await pred1.insert(alice, value)
    await pred2.insert(bob, value)

    # Monkey-patch one to raise exception
    original_query = pred1.query_osp

    async def failing_query(obj):
        raise RuntimeError("Intentional failure")

    pred1.query_osp = failing_query

    # Query should still return results from pred2
    results = []
    async for triple in registry.query_osp_parallel(value):
        results.append(triple)

    # Should have result from pred2, but not pred1 (failed)
    assert len(results) >= 1
    subjects = {s for s, p, o in results}
    assert bob in subjects


def test_audit_all_plugins():
    """Test auditing performance across all plugins."""
    registry = PredicateRegistry()

    # Register predicates
    registry.register_cidsem_counter("R:usr:count", "test")
    registry.register_cidsem_multivalue("R:usr:friends", "test")

    # Get audit
    audit = registry.audit_all_plugins()

    assert "summary" in audit
    assert "plugins" in audit
    assert "ontology_audit" in audit

    summary = audit["summary"]
    assert summary["total_plugins"] == 2
    assert summary["total_operations"] >= 0

    plugins = audit["plugins"]
    assert "R:usr:count" in plugins
    assert "R:usr:friends" in plugins


@pytest.mark.asyncio
async def test_audit_all_plugins_with_operations():
    """Test audit shows operation counts after actual operations."""
    registry = PredicateRegistry()

    counter = registry.register_cidsem_counter("R:usr:count")
    alice = E(1, 1)
    bob = E(2, 2)

    # Perform operations
    await counter.insert(alice, 5)
    await counter.insert(bob, 10)
    await counter.query_spo(alice)
    await counter.query_osp(5)

    audit = registry.audit_all_plugins()

    counter_metrics = audit["plugins"]["R:usr:count"]
    assert counter_metrics["total_operations"] == 4  # 2 inserts + 2 queries
    assert counter_metrics["counter_count"] == 2


def test_validate_cidsem_format_valid():
    """Test CIDSem validation for valid formats."""
    registry = PredicateRegistry()

    valid_formats = [
        "E:usr:alice",
        "R:usr:friendsWith",
        "EV:sys:login",
        "L:int:42",
        "C:app:context",
        "R:namespace:label",
        "R:ns:label:with:extra:colons",  # Extra colons in label allowed
    ]

    for fmt in valid_formats:
        assert registry._validate_cidsem_format(fmt), f"Failed for {fmt}"


def test_validate_cidsem_format_invalid():
    """Test CIDSem validation rejects invalid formats."""
    registry = PredicateRegistry()

    invalid_formats = [
        "invalid",  # No colons
        "E:only_one",  # Only one colon
        "X:usr:label",  # Invalid kind
        "invalid:usr:label",  # Invalid kind
        ":usr:label",  # Empty kind
    ]

    for fmt in invalid_formats:
        assert not registry._validate_cidsem_format(fmt), f"Should fail for {fmt}"


def test_compute_predicate_cid_deterministic():
    """Test that predicate CID computation is deterministic."""
    registry = PredicateRegistry()

    cid1 = registry._compute_predicate_cid("R:usr:test")
    cid2 = registry._compute_predicate_cid("R:usr:test")

    assert cid1 == cid2


def test_compute_predicate_cid_different():
    """Test that different predicates get different CIDs."""
    registry = PredicateRegistry()

    cid1 = registry._compute_predicate_cid("R:usr:test1")
    cid2 = registry._compute_predicate_cid("R:usr:test2")

    assert cid1 != cid2


def test_audit_ontology_discipline_violations():
    """Test ontology audit detects missing justifications."""
    registry = PredicateRegistry()

    # Register without justification
    registry.register_cidsem_counter("R:usr:noJustification", "")

    audit = registry.audit_ontology_discipline()

    assert len(audit["violations"]) == 1
    assert "No performance justification" in audit["violations"][0]


def test_audit_ontology_discipline_size_warning():
    """Test ontology audit warns when approaching 200 predicate limit."""
    registry = PredicateRegistry()

    # Register 201 predicates
    for i in range(201):
        registry.register_cidsem_counter(f"R:usr:pred{i}", "test")

    audit = registry.audit_ontology_discipline()

    assert len(audit["warnings"]) > 0
    assert any("performance degradation" in w for w in audit["warnings"])


def test_get_predicate_name():
    """Test getting predicate name from CID."""
    registry = PredicateRegistry()

    pred_name = "R:usr:test"
    registry.register_cidsem_counter(pred_name)

    pred_cid = registry.predicate_to_cid[pred_name]
    retrieved_name = registry.get_predicate_name(pred_cid)

    assert retrieved_name == pred_name


def test_get_predicate_cid():
    """Test getting predicate CID from name."""
    registry = PredicateRegistry()

    pred_name = "R:usr:test"
    counter = registry.register_cidsem_counter(pred_name)

    retrieved_cid = registry.get_predicate_cid(pred_name)

    assert retrieved_cid == counter.predicate


def test_load_cidsem_ontology():
    """Test loading CIDSem ontology specification."""
    registry = PredicateRegistry()

    ontology = {
        "R:usr:friendsWith": {
            "type": "relation",
            "justification": "Social graph",
        },
        "R:usr:ownedQuantity": {
            "type": "counter",
            "justification": "Quantity tracking",
        },
    }

    registry.load_cidsem_ontology(ontology)

    assert len(registry.cidsem_ontology) == 2
    assert "R:usr:friendsWith" in registry.cidsem_ontology


def test_load_cidsem_ontology_invalid_format_raises():
    """Test that loading ontology with invalid format raises ValueError."""
    registry = PredicateRegistry()

    ontology = {
        "invalid_format": {"type": "relation"}  # Missing colons
    }

    with pytest.raises(ValueError, match="Invalid CIDSem format"):
        registry.load_cidsem_ontology(ontology)
