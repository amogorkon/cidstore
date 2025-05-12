"""test_entity.py

Tests for the entity.py module which defines the E class and composite key functions.
"""

# Import the module under test.
from cidtree.keys import (
    E,
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
