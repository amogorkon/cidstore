"""test_entity.py

Tests for the entity.py module which defines the E class and composite key functions.
"""

from hypothesis import assume, given
from hypothesis import strategies as st

# Import the module under test.
from cidtree.keys import (
    E,
    create_composite_key,
    enhanced_mix,
    rot64,
)

# ---------------------------------------------------
# Tests for the E class (128-bit entity keys)
# ---------------------------------------------------


def test_e_high_low():
    """Test that the high and low properties yield the correct 64-bit parts."""
    # We'll manually construct a 128-bit integer.
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    # Compute expected parts.
    expected_high = value >> 64
    expected_low = value & ((1 << 64) - 1)
    assert e.high == expected_high
    assert e.low == expected_low


def test_to_from_hdf5():
    """Test that converting to HDF5 and back preserves the key."""
    orig = E(0xFEDCBA0987654321FEDCBA0987654321)
    arr = orig.to_hdf5()
    reconstructed = E.from_hdf5(arr)
    assert reconstructed == orig


def test_e_str_repr():
    """Test that __str__ and __repr__ produce non-empty strings."""
    e = E(0xABCDE12345ABCDE12345ABCDE12345)
    rep = repr(e)
    s = str(e)
    assert isinstance(rep, str) and rep != ""
    assert isinstance(s, str) and s != ""


# ---------------------------------------------------
# Tests for helper functions
# ---------------------------------------------------


def test_rot64_known_value():
    """Test rot64 on a known value."""
    # For example, if we rotate 0x0123456789ABCDEF by 8 bits,
    # compute the expected value manually.
    x = 0x0123456789ABCDEF
    n = 8
    # Perform 64-bit left rotation.
    expected = ((x << n) | (x >> (64 - n))) & 0xFFFFFFFFFFFFFFFF
    result = rot64(x, n)
    assert result == expected


@given(
    x=st.integers(min_value=0, max_value=(1 << 64) - 1),
)
def test_enhanced_mix_output_range(x):
    """Test that enhanced_mix returns a 64-bit integer and is deterministic."""
    result1 = enhanced_mix(x)
    result2 = enhanced_mix(x)
    # Check determinism.
    assert result1 == result2
    # Check that the result is a 64-bit int.
    assert 0 <= result1 < (1 << 64)


# ---------------------------------------------------
# Tests for composite key creation
# ---------------------------------------------------


# The composite key is now also an E instance
@given(
    a_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
    b_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
)
def test_create_composite_key_returns_E(a_val, b_val):
    a = E(a_val)
    b = E(b_val)
    comp = create_composite_key(a, b)
    assert isinstance(comp, E)


@given(
    a_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
    b_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
)
def test_create_composite_key_idempotence(a_val, b_val):
    """
    Test that create_composite_key returns the same result on repeated calls.
    """
    a = E(a_val)
    b = E(b_val)
    key1 = create_composite_key(a, b)
    key2 = create_composite_key(a, b)
    assert key1 == key2


@given(
    a_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
    b_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
)
def test_create_composite_key_range(a_val, b_val):
    """
    Test that the composite key is an E and is within the 128-bit range.
    """
    a = E(a_val)
    b = E(b_val)
    comp = create_composite_key(a, b)
    assert isinstance(comp, E)
    assert 0 <= comp < (1 << 128)


@given(
    a_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
    b_val=st.integers(min_value=0, max_value=(1 << 128) - 1),
)
def test_create_composite_key_noncommutativity(a_val, b_val):
    """
    If a and b are distinct, the composite key should be non-commutative.
    That is, create_composite_key(a, b) != create_composite_key(b, a)
    whenever a != b.
    """
    assume(a_val != b_val)
    a = E(a_val)
    b = E(b_val)
    key_ab = create_composite_key(a, b)
    key_ba = create_composite_key(b, a)
    assert key_ab != key_ba, "Composite key function is commutative for distinct keys"
