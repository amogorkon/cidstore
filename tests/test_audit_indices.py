"""Tests for index consistency auditing."""

from cidstore.keys import E
from cidstore.predicates import CounterStore, MultiValueSetStore


def test_audit_empty_store(tree):
    """Test audit on empty store with no data."""
    result = tree.audit_indices()
    
    assert result['consistent'] is True
    assert result['predicates_audited'] == 0
    assert result['errors'] == []
    assert result['warnings'] == []
    assert result['stats']['total_spo_entries'] == 0
    assert result['stats']['total_osp_entries'] == 0


def test_audit_counter_consistent(tree):
    """Test audit on consistent CounterStore data."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)
    
    # Insert data
    player1 = E.from_str("E:test:player1")
    player2 = E.from_str("E:test:player2")
    player3 = E.from_str("E:test:player3")
    
    tree.insert_triple(player1, score_pred, 100)
    tree.insert_triple(player2, score_pred, 200)
    tree.insert_triple(player3, score_pred, 100)  # Same score as player1
    
    # Audit should pass
    result = tree.audit_indices()
    
    assert result['consistent'] is True, f"Errors: {result['errors']}"
    assert result['predicates_audited'] == 1
    assert result['errors'] == []
    assert result['stats']['total_spo_entries'] == 3


def test_audit_multivalue_consistent(tree):
    """Test audit on consistent MultiValueSetStore data."""
    # Register multivalue predicate
    friend_pred = E.from_str("R:test:friend")
    tree.register_predicate(friend_pred, MultiValueSetStore)
    
    # Insert data
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    carol = E.from_str("E:user:carol")
    
    tree.insert_triple(alice, friend_pred, bob)
    tree.insert_triple(alice, friend_pred, carol)
    tree.insert_triple(bob, friend_pred, carol)
    
    # Audit should pass
    result = tree.audit_indices()
    
    assert result['consistent'] is True, f"Errors: {result['errors']}"
    assert result['predicates_audited'] == 1
    assert result['errors'] == []


def test_audit_multiple_predicates(tree):
    """Test audit with multiple predicates."""
    # Register predicates
    score_pred = E.from_str("R:test:score")
    friend_pred = E.from_str("R:test:friend")
    
    tree.register_predicate(score_pred, CounterStore)
    tree.register_predicate(friend_pred, MultiValueSetStore)
    
    # Insert data for both
    player = E.from_str("E:test:player")
    friend = E.from_str("E:test:friend_entity")
    
    tree.insert_triple(player, score_pred, 42)
    tree.insert_triple(player, friend_pred, friend)
    
    # Audit should pass
    result = tree.audit_indices()
    
    assert result['consistent'] is True
    assert result['predicates_audited'] == 2
    assert result['stats']['predicates_registered'] == 2


def test_audit_detects_spo_osp_mismatch_counter(tree):
    """Test that audit detects SPO-OSP inconsistency in CounterStore."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    counter_ds = tree.register_predicate(score_pred, CounterStore)
    
    # Insert valid data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)
    
    # Manually corrupt the OSP index (simulate bug)
    counter_ds.osp_index[100].discard(player)
    
    # Audit should detect inconsistency
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    assert len(result['errors']) >= 1
    error_types = {e['type'] for e in result['errors']}
    assert 'SPO_OSP_mismatch' in error_types


def test_audit_detects_osp_spo_missing_counter(tree):
    """Test that audit detects OSP entry with missing SPO entry."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    counter_ds = tree.register_predicate(score_pred, CounterStore)
    
    # Insert valid data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)
    
    # Manually corrupt: remove SPO entry but leave OSP
    del counter_ds.spo_index[player]
    
    # Audit should detect inconsistency
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    assert len(result['errors']) >= 1
    error_types = {e['type'] for e in result['errors']}
    assert 'OSP_SPO_missing' in error_types


def test_audit_detects_pos_osp_mismatch(tree):
    """Test that audit detects POS-OSP inconsistency."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    counter_ds = tree.register_predicate(score_pred, CounterStore)
    
    # Insert valid data
    player1 = E.from_str("E:test:player1")
    player2 = E.from_str("E:test:player2")
    tree.insert_triple(player1, score_pred, 100)
    tree.insert_triple(player2, score_pred, 100)
    
    # Manually corrupt: remove one subject from POS but not OSP
    counter_ds.pos_index[100].discard(player1)
    
    # Audit should detect inconsistency
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    error_types = {e['type'] for e in result['errors']}
    assert 'POS_OSP_missing' in error_types or 'POS_OSP_extra' in error_types


def test_audit_detects_multivalue_spo_osp_mismatch(tree):
    """Test audit detects SPO-OSP mismatch in MultiValueSetStore."""
    # Register multivalue predicate
    friend_pred = E.from_str("R:test:friend")
    mvs_ds = tree.register_predicate(friend_pred, MultiValueSetStore)
    
    # Insert valid data
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    tree.insert_triple(alice, friend_pred, bob)
    
    # Manually corrupt: remove from OSP index
    mvs_ds.osp_index[bob].discard(alice)
    
    # Audit should detect inconsistency
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    assert len(result['errors']) >= 1
    assert result['errors'][0]['type'] in ['SPO_OSP_mismatch', 'POS_OSP_extra']


def test_audit_detects_multivalue_osp_spo_mismatch(tree):
    """Test audit detects OSP-SPO mismatch in MultiValueSetStore."""
    # Register multivalue predicate
    friend_pred = E.from_str("R:test:friend")
    mvs_ds = tree.register_predicate(friend_pred, MultiValueSetStore)
    
    # Insert valid data
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    tree.insert_triple(alice, friend_pred, bob)
    
    # Manually corrupt: add extra entry in OSP without SPO
    fake_subject = E.from_str("E:user:fake")
    mvs_ds.osp_index[bob].add(fake_subject)
    
    # Audit should detect inconsistency
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    error_types = {e['type'] for e in result['errors']}
    assert 'OSP_SPO_missing' in error_types or 'OSP_SPO_mismatch' in error_types


def test_audit_after_delete_operations(tree):
    """Test audit remains consistent after delete operations."""
    # Register predicate
    friend_pred = E.from_str("R:test:friend")
    tree.register_predicate(friend_pred, MultiValueSetStore)
    
    # Insert and delete data
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    carol = E.from_str("E:user:carol")
    
    tree.insert_triple(alice, friend_pred, bob)
    tree.insert_triple(alice, friend_pred, carol)
    tree.delete_triple(alice, friend_pred, bob)
    
    # Audit should still pass after delete
    result = tree.audit_indices()
    
    assert result['consistent'] is True, f"Errors after delete: {result['errors']}"
    assert result['errors'] == []


def test_audit_counter_value_mismatch(tree):
    """Test audit detects when OSP and SPO have different values."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    counter_ds = tree.register_predicate(score_pred, CounterStore)
    
    # Insert valid data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)
    
    # Manually corrupt: change SPO value without updating OSP
    counter_ds.spo_index[player] = 200  # Changed from 100
    # OSP still has player under 100
    
    # Audit should detect value mismatch
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    error_types = {e['type'] for e in result['errors']}
    assert 'OSP_SPO_value_mismatch' in error_types or 'SPO_OSP_mismatch' in error_types


def test_audit_statistics_accuracy(tree):
    """Test that audit statistics are accurate."""
    # Register predicates
    score_pred = E.from_str("R:test:score")
    friend_pred = E.from_str("R:test:friend")
    
    tree.register_predicate(score_pred, CounterStore)
    tree.register_predicate(friend_pred, MultiValueSetStore)
    
    # Insert data
    player1 = E.from_str("E:test:player1")
    player2 = E.from_str("E:test:player2")
    alice = E.from_str("E:user:alice")
    bob = E.from_str("E:user:bob")
    
    tree.insert_triple(player1, score_pred, 100)
    tree.insert_triple(player2, score_pred, 200)
    tree.insert_triple(alice, friend_pred, bob)
    
    # Run audit
    result = tree.audit_indices()
    
    # Verify statistics
    assert result['stats']['predicates_registered'] == 2
    assert result['stats']['total_spo_entries'] == 3  # 2 counters + 1 multivalue
    assert result['stats']['total_osp_entries'] >= 2  # At least 2 unique values


def test_audit_handles_empty_osp_sets(tree):
    """Test audit handles edge case of empty OSP sets gracefully."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    tree.register_predicate(score_pred, CounterStore)
    
    # Insert and immediately delete to create edge case
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)
    tree.delete_triple(player, score_pred, 100)
    
    # Audit should pass (no data left)
    result = tree.audit_indices()
    
    assert result['consistent'] is True
    assert result['stats']['total_spo_entries'] == 0


def test_audit_pos_missing_object(tree):
    """Test audit detects when object exists in OSP but not POS."""
    # Register counter predicate
    score_pred = E.from_str("R:test:score")
    counter_ds = tree.register_predicate(score_pred, CounterStore)
    
    # Insert valid data
    player = E.from_str("E:test:player")
    tree.insert_triple(player, score_pred, 100)
    
    # Manually corrupt: remove object from POS index entirely
    del counter_ds.pos_index[100]
    
    # Audit should detect missing object
    result = tree.audit_indices()
    
    assert result['consistent'] is False
    error_types = {e['type'] for e in result['errors']}
    assert 'POS_missing_object' in error_types
