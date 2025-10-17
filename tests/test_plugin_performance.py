"""Tests for plugin performance auditing and metrics collection.

Tests audit_performance() on plugins and audit_all_plugins() on PredicateRegistry.
"""

import pytest

from cidstore.keys import E
from cidstore.predicates import PredicateRegistry


@pytest.mark.asyncio
async def test_counter_store_audit_performance():
    """Test CounterStore.audit_performance() returns correct metrics."""
    registry = PredicateRegistry()
    counter = registry.register_cidsem_counter("R:usr:score")

    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)

    # Perform operations
    await counter.insert(alice, 100)
    await counter.insert(bob, 200)
    await counter.insert(charlie, 100)
    await counter.query_spo(alice)
    await counter.query_osp(100)

    # Audit performance
    metrics = counter.audit_performance()

    assert metrics["total_operations"] == 5  # 3 inserts + 2 queries
    assert metrics["total_increments"] == 3
    assert metrics["counter_count"] == 3
    assert metrics["unique_values"] == 2  # 100 and 200
    assert "data_structure_stats" in metrics
    assert metrics["data_structure_stats"]["spo_entries"] == 3


@pytest.mark.asyncio
async def test_multivalue_store_audit_performance():
    """Test MultiValueSetStore.audit_performance() returns correct metrics."""
    registry = PredicateRegistry()
    multivalue = registry.register_cidsem_multivalue("R:usr:friendsWith")

    alice = E(1, 1)
    bob = E(2, 2)
    charlie = E(3, 3)
    dave = E(4, 4)

    # Perform operations
    await multivalue.insert(alice, bob)
    await multivalue.insert(alice, charlie)
    await multivalue.insert(bob, charlie)
    await multivalue.query_spo(alice)

    # Audit performance
    metrics = multivalue.audit_performance()

    assert metrics["total_operations"] == 4  # 3 inserts + 1 query
    assert metrics["set_count"] == 2  # alice and bob
    assert metrics["total_values"] == 3  # bob, charlie, charlie
    assert "avg_set_size" in metrics
    assert metrics["data_structure_stats"]["spo_entries"] == 2


@pytest.mark.asyncio
async def test_counter_avg_value_calculation():
    """Test that CounterStore calculates average value correctly."""
    registry = PredicateRegistry()
    counter = registry.register_cidsem_counter("R:usr:points")

    users = [E(i, i) for i in range(1, 6)]
    values = [10, 20, 30, 40, 50]

    for user, value in zip(users, values):
        await counter.insert(user, value)

    metrics = counter.audit_performance()

    # Average: (10 + 20 + 30 + 40 + 50) / 5 = 30
    assert metrics["avg_value"] == 30.0


@pytest.mark.asyncio
async def test_multivalue_avg_set_size():
    """Test that MultiValueSetStore calculates average set size correctly."""
    registry = PredicateRegistry()
    multivalue = registry.register_cidsem_multivalue("R:usr:skills")

    alice = E(1, 1)
    bob = E(2, 2)

    # Alice has 3 skills
    await multivalue.insert(alice, E(10, 1))
    await multivalue.insert(alice, E(10, 2))
    await multivalue.insert(alice, E(10, 3))

    # Bob has 1 skill
    await multivalue.insert(bob, E(10, 1))

    metrics = multivalue.audit_performance()

    # Total values: 4 (3 for alice + 1 for bob)
    # Set count: 2 (alice, bob)
    # Average: 4 / 2 = 2.0
    assert metrics["avg_set_size"] == 2.0


def test_audit_all_plugins_empty_registry():
    """Test auditing empty registry returns zero metrics."""
    registry = PredicateRegistry()
    audit = registry.audit_all_plugins()

    assert audit["summary"]["total_plugins"] == 0
    assert audit["summary"]["total_operations"] == 0
    assert len(audit["plugins"]) == 0


@pytest.mark.asyncio
async def test_audit_all_plugins_multiple_predicates():
    """Test audit_all_plugins aggregates metrics from all plugins."""
    registry = PredicateRegistry()

    counter = registry.register_cidsem_counter("R:usr:score", "test")
    multivalue = registry.register_cidsem_multivalue("R:usr:friends", "test")

    alice = E(1, 1)
    bob = E(2, 2)

    # Perform operations on both
    await counter.insert(alice, 100)
    await counter.query_spo(alice)
    await multivalue.insert(alice, bob)
    await multivalue.query_spo(alice)

    audit = registry.audit_all_plugins()

    # Summary
    assert audit["summary"]["total_plugins"] == 2
    assert audit["summary"]["total_operations"] == 4  # 2 per plugin

    # Per-plugin metrics
    assert "R:usr:score" in audit["plugins"]
    assert "R:usr:friends" in audit["plugins"]
    assert audit["plugins"]["R:usr:score"]["total_operations"] == 2
    assert audit["plugins"]["R:usr:friends"]["total_operations"] == 2


@pytest.mark.asyncio
async def test_audit_performance_tracks_operations():
    """Test that metrics are updated with each operation."""
    registry = PredicateRegistry()
    counter = registry.register_cidsem_counter("R:usr:count")

    alice = E(1, 1)

    # Initial state
    metrics = counter.audit_performance()
    assert metrics["total_operations"] == 0

    # After insert
    await counter.insert(alice, 1)
    metrics = counter.audit_performance()
    assert metrics["total_operations"] == 1

    # After query
    await counter.query_spo(alice)
    metrics = counter.audit_performance()
    assert metrics["total_operations"] == 2


@pytest.mark.asyncio
async def test_audit_all_plugins_includes_ontology_audit():
    """Test that audit_all_plugins includes ontology discipline audit."""
    registry = PredicateRegistry()

    # Register without justification
    registry.register_cidsem_counter("R:usr:noJustification", "")

    audit = registry.audit_all_plugins()

    assert "ontology_audit" in audit
    assert "violations" in audit["ontology_audit"]
    assert len(audit["ontology_audit"]["violations"]) > 0


@pytest.mark.asyncio
async def test_counter_unique_values_count():
    """Test that CounterStore tracks unique values correctly."""
    registry = PredicateRegistry()
    counter = registry.register_cidsem_counter("R:usr:age")

    # Three people with two different ages
    await counter.insert(E(1, 1), 30)
    await counter.insert(E(2, 2), 25)
    await counter.insert(E(3, 3), 30)

    metrics = counter.audit_performance()

    assert metrics["unique_values"] == 2  # 25 and 30
    assert metrics["counter_count"] == 3  # 3 people


@pytest.mark.asyncio
async def test_multivalue_unique_objects_count():
    """Test that MultiValueSetStore tracks unique objects correctly."""
    registry = PredicateRegistry()
    multivalue = registry.register_cidsem_multivalue("R:usr:likes")

    pizza = E(10, 1)
    burger = E(10, 2)

    # Three people like two different foods
    await multivalue.insert(E(1, 1), pizza)
    await multivalue.insert(E(2, 2), burger)
    await multivalue.insert(E(3, 3), pizza)

    metrics = multivalue.audit_performance()

    assert metrics["unique_objects"] == 2  # pizza and burger
    assert metrics["set_count"] == 3  # 3 people


@pytest.mark.asyncio
async def test_performance_metrics_after_updates():
    """Test that metrics update correctly after value updates."""
    registry = PredicateRegistry()
    counter = registry.register_cidsem_counter("R:usr:score")

    alice = E(1, 1)

    # Initial insert
    await counter.insert(alice, 100)
    metrics1 = counter.audit_performance()

    # Update to new value
    await counter.insert(alice, 200)
    metrics2 = counter.audit_performance()

    # Total increments should increase
    assert metrics2["total_increments"] > metrics1["total_increments"]
    # But counter_count stays the same (still just alice)
    assert metrics2["counter_count"] == metrics1["counter_count"]


@pytest.mark.asyncio
async def test_audit_performance_includes_all_indices():
    """Test that audit includes stats for all indices (SPO, OSP, POS)."""
    registry = PredicateRegistry()
    counter = registry.register_cidsem_counter("R:usr:test")

    alice = E(1, 1)
    await counter.insert(alice, 5)

    metrics = counter.audit_performance()

    stats = metrics["data_structure_stats"]
    assert "spo_entries" in stats
    assert "osp_entries" in stats
    assert "pos_entries" in stats
    # All should be 1 for single insert
    assert stats["spo_entries"] == 1
    assert stats["osp_entries"] == 1
    assert stats["pos_entries"] == 1
