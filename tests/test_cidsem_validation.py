"""Tests for CIDSem term validation (Spec 14).

Tests:
- Valid term formats accepted
- Invalid term formats rejected with helpful errors
- Predicate limit enforcement (~200 limit)
- kind:namespace:label parsing
- Character validation
"""

import pytest
from cidstore.predicates import PredicateRegistry, PREDICATE_LIMIT
from cidstore.exceptions import InvalidCIDSemTermError, PredicateLimitExceededError


def test_validate_cidsem_valid_entity():
    """Test valid entity terms accepted."""
    registry = PredicateRegistry()
    
    valid_terms = [
        "E:person:alice",
        "E:usr:bob",
        "E:sys:user123",
        "E:test_ns:test-label",
        "E:my_namespace:my-label_123",
    ]
    
    for term in valid_terms:
        assert registry._validate_cidsem_format(term), f"Should accept valid term: {term}"


def test_validate_cidsem_valid_relation():
    """Test valid relation terms accepted."""
    registry = PredicateRegistry()
    
    valid_terms = [
        "R:knows:friendOf",
        "R:usr:ownedQuantity",
        "R:test:hasValue",
        "R:my-ns:my_rel",
    ]
    
    for term in valid_terms:
        assert registry._validate_cidsem_format(term), f"Should accept valid term: {term}"


def test_validate_cidsem_valid_event():
    """Test valid event terms accepted."""
    registry = PredicateRegistry()
    
    valid_terms = [
        "EV:sys:login",
        "EV:user:pageView",
        "EV:analytics:click",
    ]
    
    for term in valid_terms:
        assert registry._validate_cidsem_format(term), f"Should accept valid term: {term}"


def test_validate_cidsem_valid_literal():
    """Test valid literal terms accepted."""
    registry = PredicateRegistry()
    
    valid_terms = [
        "L:int:42",
        "L:str:hello",
        "L:float:3-14",
    ]
    
    for term in valid_terms:
        assert registry._validate_cidsem_format(term), f"Should accept valid term: {term}"


def test_validate_cidsem_valid_context():
    """Test valid context terms accepted."""
    registry = PredicateRegistry()
    
    valid_terms = [
        "C:app:production",
        "C:env:staging",
        "C:tenant:customer123",
    ]
    
    for term in valid_terms:
        assert registry._validate_cidsem_format(term), f"Should accept valid term: {term}"


def test_validate_cidsem_reject_empty():
    """Test empty terms rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Empty term"):
        registry._validate_cidsem_strict("")


def test_validate_cidsem_reject_missing_colons():
    """Test terms without colons rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Missing colon separators"):
        registry._validate_cidsem_strict("notvalid")


def test_validate_cidsem_reject_insufficient_parts():
    """Test terms with < 3 parts rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Insufficient components"):
        registry._validate_cidsem_strict("R:onlyonecolon")


def test_validate_cidsem_reject_too_many_parts():
    """Test terms with > 3 parts rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Too many components"):
        registry._validate_cidsem_strict("R:too:many:parts")


def test_validate_cidsem_reject_invalid_kind():
    """Test invalid kind rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Invalid kind"):
        registry._validate_cidsem_strict("X:usr:value")  # X not in {E, R, EV, L, C}


def test_validate_cidsem_reject_empty_namespace():
    """Test empty namespace rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Empty namespace"):
        registry._validate_cidsem_strict("R::label")


def test_validate_cidsem_reject_empty_label():
    """Test empty label rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="Empty label"):
        registry._validate_cidsem_strict("R:namespace:")


def test_validate_cidsem_reject_invalid_chars_namespace():
    """Test invalid characters in namespace rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="invalid characters"):
        registry._validate_cidsem_strict("R:inva lid:label")  # space not allowed
    
    with pytest.raises(InvalidCIDSemTermError, match="invalid characters"):
        registry._validate_cidsem_strict("R:inva.lid:label")  # dot not allowed


def test_validate_cidsem_reject_invalid_chars_label():
    """Test invalid characters in label rejected."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError, match="invalid characters"):
        registry._validate_cidsem_strict("R:namespace:inva lid")  # space not allowed
    
    with pytest.raises(InvalidCIDSemTermError, match="invalid characters"):
        registry._validate_cidsem_strict("R:namespace:inva.lid")  # dot not allowed


def test_register_cidsem_counter_valid():
    """Test registering counter with valid CIDSem term."""
    registry = PredicateRegistry()
    
    ds = registry.register_cidsem_counter(
        "R:usr:ownedQuantity",
        "Enables O(1) quantity aggregation"
    )
    
    assert ds is not None
    assert "R:usr:ownedQuantity" in registry.predicate_to_cid


def test_register_cidsem_counter_invalid_term():
    """Test registering counter with invalid term raises error."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError):
        registry.register_cidsem_counter("invalid_term")


def test_register_cidsem_multivalue_valid():
    """Test registering multivalue with valid CIDSem term."""
    registry = PredicateRegistry()
    
    ds = registry.register_cidsem_multivalue(
        "R:usr:friendsWith",
        "Bidirectional graph queries"
    )
    
    assert ds is not None
    assert "R:usr:friendsWith" in registry.predicate_to_cid


def test_register_cidsem_multivalue_invalid_term():
    """Test registering multivalue with invalid term raises error."""
    registry = PredicateRegistry()
    
    with pytest.raises(InvalidCIDSemTermError):
        registry.register_cidsem_multivalue("bad:format")


def test_predicate_limit_enforcement():
    """Test that PREDICATE_LIMIT is enforced."""
    registry = PredicateRegistry()
    
    # Register predicates up to the limit
    for i in range(PREDICATE_LIMIT):
        # Use register_counter to avoid CIDSem validation overhead
        from cidstore.keys import E
        p = E.from_str(f"predicate_{i}")
        registry.register_counter(p)
    
    # Next registration should fail
    from cidstore.keys import E
    p_overflow = E.from_str("overflow_predicate")
    
    with pytest.raises(PredicateLimitExceededError) as exc_info:
        registry.register_counter(p_overflow)
    
    assert exc_info.value.limit == PREDICATE_LIMIT
    assert exc_info.value.attempted == PREDICATE_LIMIT + 1


def test_predicate_limit_cidsem_counter():
    """Test predicate limit enforced for CIDSem counter registration."""
    registry = PredicateRegistry()
    
    # Fill up to limit
    for i in range(PREDICATE_LIMIT):
        from cidstore.keys import E
        p = E.from_str(f"predicate_{i}")
        registry.register_counter(p)
    
    # CIDSem registration should also respect limit
    with pytest.raises(PredicateLimitExceededError):
        registry.register_cidsem_counter("R:test:overflow")


def test_predicate_limit_cidsem_multivalue():
    """Test predicate limit enforced for CIDSem multivalue registration."""
    registry = PredicateRegistry()
    
    # Fill up to limit
    for i in range(PREDICATE_LIMIT):
        from cidstore.keys import E
        p = E.from_str(f"predicate_{i}")
        registry.register_counter(p)
    
    # CIDSem registration should also respect limit
    with pytest.raises(PredicateLimitExceededError):
        registry.register_cidsem_multivalue("R:test:overflow")


def test_cidsem_compute_predicate_cid_deterministic():
    """Test that _compute_predicate_cid is deterministic."""
    registry = PredicateRegistry()
    
    term = "R:usr:ownedQuantity"
    
    cid1 = registry._compute_predicate_cid(term)
    cid2 = registry._compute_predicate_cid(term)
    
    assert cid1 == cid2


def test_cidsem_compute_predicate_cid_unique():
    """Test that different terms produce different CIDs."""
    registry = PredicateRegistry()
    
    cid1 = registry._compute_predicate_cid("R:usr:ownedQuantity")
    cid2 = registry._compute_predicate_cid("R:usr:friendsWith")
    
    assert cid1 != cid2
