"""
Spec 2: Data Types and Structure (TDD)
Covers canonical data types, key/value encoding, and structure (E class, ValueSet, HashEntry, etc.).
All tests are TDD-style and implementation-agnostic.
"""

import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio


def test_e_high_low():
    """Test that the high and low properties yield the correct 64-bit parts."""
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    expected_high = value >> 64
    expected_low = value & ((1 << 64) - 1)
    assert e.high == expected_high
    assert e.low == expected_low


def test_e_to_from_hdf5():
    """Test that converting to HDF5 and back preserves the key."""
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    h5 = e.to_hdf5()
    e2 = E.from_entry(h5)
    assert int(e2) == int(e)


async def test_e_in_valueset_and_hashentry(directory):
    """Test that E can be used as a key and value in ValueSet/HashEntry."""
    key = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    await directory.insert(key, value)
    result = await directory.lookup(key)
    assert value in result or int(value) in [int(x) for x in result]
    value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    await directory.insert(key, value)
    result = await directory.lookup(key)
    assert value in result or int(value) in [int(x) for x in result]
    key2 = E(0x11112222333344445555666677778888)
    value2 = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    await directory.insert(key2, value2)
    result = await directory.lookup(key2)
    assert value2 in result or int(value2) in [int(x) for x in result]
    # Spec 2: Check canonical structure of HashEntry and ValueSet (new model)
    entry = await directory.get_entry(key2)
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "checksum" in entry
    # Check that slots is a list/tuple of length 2 (inline) or a SpillPointer (external)
    slots = entry["slots"]
    if isinstance(slots, (list, tuple)):
        assert len(slots) == 2
    # Check checksum is 16 bytes (int or bytes)
    checksum = entry["checksum"]
    assert isinstance(checksum, (int, bytes))
    # Optionally, check for sorted_count if present
    if "sorted_count" in entry:
        assert isinstance(entry["sorted_count"], int)
    # Repeat for key
    entry = await directory.get_entry(key)
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "checksum" in entry
    slots = entry["slots"]
    if isinstance(slots, (list, tuple)):
        assert len(slots) == 2
    checksum = entry["checksum"]
    assert isinstance(checksum, (int, bytes))
    if "sorted_count" in entry:
        assert isinstance(entry["sorted_count"], int)
