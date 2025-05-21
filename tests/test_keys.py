# Additional tests for CID, immutability, and ValueSet/HashEntry integration
from cidstore.keys import E


def test_cid_in_valueset_and_hashentry():
    # Simulate ValueSet and HashEntry usage
    import tempfile

    from cidstore.main import CIDTree

    with tempfile.TemporaryDirectory() as tmp:
        from cidstore.store import WAL, Storage

        storage = Storage(f"{tmp}/test.h5")
        wal = WAL(None)
        tree = CIDTree(storage, wal=wal)
        key = E(0x11112222333344445555666677778888)
        value = E(0x9999AAAABBBBCCCCDDDDEEEEFFFF0000)
        tree.insert(key, value)
        result = list(tree.get(key))
        assert value in result or int(value) in [int(x) for x in result]


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
    rep = repr(e)
    s = str(e)
    assert isinstance(rep, str) and rep != ""
    assert isinstance(s, str) and s != ""
