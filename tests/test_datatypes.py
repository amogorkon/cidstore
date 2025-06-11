"""
Spec 2: Data Types and Structure (TDD)
Covers canonical data types, key/value encoding, and structure (E class, ValueSet, HashEntry, etc.).
All tests are TDD-style and implementation-agnostic.
"""

import pytest

from cidstore.keys import E

pytestmark = pytest.mark.asyncio


def test_e_high_low():
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    assert e.high == 0x1234567890ABCDEF
    assert e.low == 0x0FEDCBA098765432


def test_e_from_str():
    s = "test-entity"
    e = E.from_str(s)
    assert isinstance(e, E)
    assert str(e).startswith("E('")


def test_e_from_jackhash():
    # Use a valid JACK hash string (simulate)
    jackhash = "00000000000000000000000000000001"
    e = E.from_jackhash(jackhash)
    assert isinstance(e, E)


def test_e_repr_str():
    e = E(123)
    assert isinstance(repr(e), str)
    assert isinstance(str(e), str)


def test_e_to_from_hdf5():
    value = (0x1234567890ABCDEF << 64) | 0x0FEDCBA098765432
    e = E(value)
    h5 = e.to_hdf5()
    e2 = E.from_hdf5(h5)
    assert int(e2) == int(e)
    e3 = E.from_entry(h5)
    assert int(e3) == int(e)


def test_e_from_hdf5_invalid():
    import numpy as np

    arr = np.array([1, 2, 3])
    try:
        E.from_hdf5(arr)
    except AssertionError:
        pass
    else:
        assert False, "Expected AssertionError for invalid shape"


async def test_e_as_key(directory):
    key = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    value = E(123)
    await directory.insert(key, value)
    result = await directory.get(key)
    assert value in result or int(value) in [int(x) for x in result]


async def test_e_as_value(directory):
    key = E(123)
    value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    await directory.insert(key, value)
    result = await directory.get(key)
    assert value in result or int(value) in [int(x) for x in result]


async def test_duplicate_insert(directory):
    key = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    await directory.insert(key, value)
    await directory.insert(key, value)
    result = await directory.get(key)
    assert value in result or int(value) in [int(x) for x in result]


async def test_hashentry_structure(directory):
    key = E(0x11112222333344445555666677778888)
    value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
    await directory.insert(key, value)
    entry = await directory.get_entry(key)
    assert "key_high" in entry
    assert "key_low" in entry
    assert "slots" in entry
    assert "checksum" in entry
    slots = entry["slots"]
    if isinstance(slots, (list, tuple)):
        assert len(slots) == 2
    checksum = entry["checksum"]
    import numpy as np

    if isinstance(checksum, (int, bytes)):
        pass
    elif isinstance(checksum, (list, tuple, np.ndarray)):
        assert all(isinstance(x, (int, np.integer, bytes)) for x in checksum), (
            f"Checksum elements: {checksum}"
        )
    else:
        assert False, f"Unexpected checksum type: {type(checksum)}"
    if "sorted_count" in entry:
        assert isinstance(entry["sorted_count"], int)
