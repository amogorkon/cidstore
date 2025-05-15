"""
Spec 2: Data Types and Structure (TDD)
Covers canonical data types, key/value encoding, and structure (E class, ValueSet, HashEntry, etc.).
All tests are TDD-style and implementation-agnostic.
"""

import pytest

from cidtree.keys import E


def test_e_high_low():
    """Test that the high and low properties yield the correct 64-bit parts."""
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    expected_high = value >> 64
    expected_low = value & ((1 << 64) - 1)
    assert e.high == expected_high
    assert e.low == expected_low


def test_e_immutability():
    """E instances should be immutable."""
    e = E(0x1234567890ABCDEF1234567890ABCDEF)
    with pytest.raises(AttributeError):
        e.high = 0
    with pytest.raises(AttributeError):
        e.low = 0


def test_e_to_from_hdf5():
    """Test that converting to HDF5 and back preserves the key."""
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    h5 = e.to_hdf5()
    e2 = E.from_hdf5(h5)
    assert int(e2) == int(e)


def test_e_in_valueset_and_hashentry(directory):
    """Test that E can be used as a key and value in ValueSet/HashEntry."""
    key = E(0x11112222333344445555666677778888)
    value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    directory.insert(key, value)
    result = list(directory.lookup(key))
    assert value in result or int(value) in [int(x) for x in result]
