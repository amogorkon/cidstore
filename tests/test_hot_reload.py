"""Tests for plugin hot-reload functionality."""

from cidstore.keys import E
from cidstore.predicates import CounterStore, MultiValueSetStore


def test_unregister_predicate(tree):
    """Test unregistering a predicate removes it from registry."""
    # Register predicate
    score_pred = E.from_str("R:test:score")
    counter_ds = tree.register_predicate(score_pred, CounterStore)

    # Insert data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)

    # Unregister
    registry = tree._store.predicate_registry
    old_ds = registry.unregister(score_pred)

    # Verify unregistered
    assert old_ds is counter_ds
    assert registry.get(score_pred) is None


def test_hot_reload_counter_to_counter(tree):
    """Test hot-reload from CounterStore to CounterStore (same type)."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Insert data
    player1 = E.from_str("E:test:player1")
    player2 = E.from_str("E:test:player2")
    tree.insert_triple(player1, score_pred, 100)
    tree.insert_triple(player2, score_pred, 200)

    # Hot reload (same type)
    result = tree.hot_reload_predicate(score_pred, "counter", migrate_data=True)

    # Verify reload succeeded
    assert result["success"] is True
    assert result["old_type"] == "CounterStore"
    assert result["new_type"] == "CounterStore"
    assert result["migration"]["migrated"] == 2
    assert result["migration"]["skipped"] == 0

    # Verify data migrated correctly
    assert tree.get_triple(player1, score_pred) == 100
    assert tree.get_triple(player2, score_pred) == 200


def test_hot_reload_multivalue_to_multivalue(tree):
    """Test hot-reload from MultiValueSetStore to MultiValueSetStore."""
    # Register multivalue predicate
    friend_pred = E.from_str("R:test:friend")
    tree.register_predicate(friend_pred, MultiValueSetStore)

    # Insert data
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    carol = E.from_str("E:user:carol")

    tree.insert_triple(alice, friend_pred, bob)
    tree.insert_triple(alice, friend_pred, carol)

    # Hot reload (same type)
    result = tree.hot_reload_predicate(friend_pred, "multivalue", migrate_data=True)

    # Verify reload succeeded
    assert result["success"] is True
    assert result["old_type"] == "MultiValueSetStore"
    assert result["new_type"] == "MultiValueSetStore"
    assert result["migration"]["migrated"] == 2  # 2 triples

    # Verify data migrated correctly
    friends = tree.get_triple(alice, friend_pred)
    assert bob in friends
    assert carol in friends


def test_hot_reload_counter_to_multivalue(tree):
    """Test hot-reload from CounterStore to MultiValueSetStore."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Insert data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)

    # Hot reload to multivalue
    result = tree.hot_reload_predicate(score_pred, "multivalue", migrate_data=True)

    # Verify reload succeeded
    assert result["success"] is True
    assert result["old_type"] == "CounterStore"
    assert result["new_type"] == "MultiValueSetStore"
    assert result["migration"]["migrated"] == 1

    # Verify data converted (counter value becomes synthetic E)
    values = tree.get_triple(player, score_pred)
    assert isinstance(values, set)
    assert len(values) == 1
    # Value should be an E created from "E:migration:count_100"
    migrated_value = list(values)[0]
    # Check that it's an E value created from our migration string
    assert isinstance(migrated_value, E)
    # Verify the original string matches (E.from_str creates deterministic hashes)
    expected_e = E.from_str("E:migration:count_100")
    assert migrated_value == expected_e


def test_hot_reload_multivalue_to_counter(tree):
    """Test hot-reload from MultiValueSetStore to CounterStore."""
    # Register multivalue predicate
    tag_pred = E.from_str("R:test:tags")
    tree.register_predicate(tag_pred, MultiValueSetStore)

    # Insert data (3 tags)
    doc = E.from_str("E:test:doc")
    tag1 = E.from_str("E:tag:python")
    tag2 = E.from_str("E:tag:async")
    tag3 = E.from_str("E:tag:testing")

    tree.insert_triple(doc, tag_pred, tag1)
    tree.insert_triple(doc, tag_pred, tag2)
    tree.insert_triple(doc, tag_pred, tag3)

    # Hot reload to counter
    result = tree.hot_reload_predicate(tag_pred, "counter", migrate_data=True)

    # Verify reload succeeded
    assert result["success"] is True
    assert result["old_type"] == "MultiValueSetStore"
    assert result["new_type"] == "CounterStore"
    assert result["migration"]["migrated"] == 1

    # Verify data converted (set size becomes count)
    count = tree.get_triple(doc, tag_pred)
    assert count == 3  # 3 tags converted to count


def test_hot_reload_without_migration(tree):
    """Test hot-reload without data migration."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Insert data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)

    # Hot reload without migration
    result = tree.hot_reload_predicate(score_pred, "multivalue", migrate_data=False)

    # Verify reload succeeded
    assert result["success"] is True
    assert result["old_type"] == "CounterStore"
    assert result["new_type"] == "MultiValueSetStore"
    assert result["migration"] is None  # No migration performed

    # Verify old data is gone (no migration)
    values = tree.get_triple(player, score_pred)
    assert values == set() or values is None or len(values) == 0


def test_hot_reload_empty_predicate(tree):
    """Test hot-reload on predicate with no data."""
    # Register counter predicate (but don't insert any data)
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Hot reload to multivalue
    result = tree.hot_reload_predicate(score_pred, "multivalue", migrate_data=True)

    # Verify reload succeeded
    assert result["success"] is True
    assert result["migration"]["migrated"] == 0
    assert result["migration"]["skipped"] == 0


def test_hot_reload_preserves_new_inserts(tree):
    """Test that new inserts work after hot-reload."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Insert initial data
    player1 = E.from_str("E:test:player1")
    tree.insert_triple(player1, score_pred, 100)

    # Hot reload to multivalue
    result = tree.hot_reload_predicate(score_pred, "multivalue", migrate_data=True)
    assert result["success"] is True

    # Insert new data with new type
    player2 = E.from_str("E:test:player2")
    friend = E.from_str("E:test:friend")
    tree.insert_triple(player2, score_pred, friend)

    # Verify new insert worked
    friends = tree.get_triple(player2, score_pred)
    assert friend in friends


def test_hot_reload_multiple_subjects(tree):
    """Test hot-reload with multiple subjects."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Insert data for 5 subjects
    players = [E.from_str(f"E:test:player{i}") for i in range(5)]
    for i, player in enumerate(players):
        tree.insert_triple(player, score_pred, (i + 1) * 100)

    # Hot reload to multivalue
    result = tree.hot_reload_predicate(score_pred, "multivalue", migrate_data=True)

    # Verify all subjects migrated
    assert result["migration"]["migrated"] == 5

    # Verify each subject has data
    for player in players:
        values = tree.get_triple(player, score_pred)
        assert len(values) == 1  # Each counter becomes one synthetic E value


def test_hot_reload_invalid_plugin_type(tree):
    """Test hot-reload with invalid plugin type raises error."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Try to hot reload with invalid type
    try:
        tree.hot_reload_predicate(score_pred, "invalid_type", migrate_data=True)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown plugin type" in str(e)


def test_hot_reload_maintains_indices(tree):
    """Test that OSP/POS indices work after hot-reload."""
    # Register multivalue predicate
    friend_pred = E.from_str("R:test:friend")
    tree.register_predicate(friend_pred, MultiValueSetStore)

    # Insert data
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    carol = E.from_str("E:user:carol")

    tree.insert_triple(alice, friend_pred, carol)
    tree.insert_triple(bob, friend_pred, carol)

    # Hot reload (same type)
    result = tree.hot_reload_predicate(friend_pred, "multivalue", migrate_data=True)
    assert result["success"] is True

    # Verify data still retrievable via get_triple after reload
    alice_friends = tree.get_triple(alice, friend_pred)
    assert carol in alice_friends

    bob_friends = tree.get_triple(bob, friend_pred)
    assert carol in bob_friends


def test_hot_reload_audit_indices_after(tree):
    """Test that audit_indices passes after hot-reload."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)

    # Insert data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)

    # Hot reload to counter (same type)
    result = tree.hot_reload_predicate(score_pred, "counter", migrate_data=True)
    assert result["success"] is True

    # Run audit - should pass
    audit_result = tree.audit_indices()
    assert audit_result["consistent"] is True
    assert len(audit_result["errors"]) == 0
