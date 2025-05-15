# Additional tests for CID, immutability, and ValueSet/HashEntry integration
import pytest


def test_cid_immutability():
    e = E(0x1234567890ABCDEF1234567890ABCDEF)
    with pytest.raises(AttributeError):
        e.high = 0
    with pytest.raises(AttributeError):
        e.low = 0


def test_cid_in_valueset_and_hashentry():
    # Simulate ValueSet and HashEntry usage
    import tempfile

    from cidtree.main import CIDTree

    with tempfile.TemporaryDirectory() as tmp:
        tree = CIDTree(f"{tmp}/test.h5")
        key = E(0x11112222333344445555666677778888)
        value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
        tree.insert(key, value)
        result = list(tree.lookup(key))
        assert value in result or int(value) in [int(x) for x in result]


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
