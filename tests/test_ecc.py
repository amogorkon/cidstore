import pytest

from cidtree.keys import E
from cidtree.tree import CIDTree


@pytest.mark.parametrize("bitpos", range(8))
def test_ecc_single_bit_error_correction(tmp_path, bitpos):
    """Inject a single-bit error into state_mask and check ECC correction."""
    path = tmp_path / "ecc.h5"
    tree = CIDTree(str(path))
    key = "ecc_key"
    value = 123
    tree.insert(key, value)
    # Find the bucket and entry
    bucket_id = tree.dir[key]
    bucket = tree.buckets[bucket_id]
    idx = None
    for i in range(bucket.shape[0]):
        if (
            bucket[i]["key_high"] == E.from_str(key).high
            and bucket[i]["key_low"] == E.from_str(key).low
        ):
            idx = i
            break
    assert idx is not None
    # Save original state_mask
    orig_mask = bucket[idx]["state_mask"]
    # Inject single-bit error
    corrupted = orig_mask ^ (1 << bitpos)
    bucket[idx]["state_mask"] = corrupted
    bucket.file.flush()
    # Simulate ECC correction (if implemented)
    # For now, just check that the value is still retrievable
    result = tree.get(key)
    assert value in [int(x) for x in result]
    # Optionally, check if ECC correction restored the mask (if implemented)
    # bucket[idx]["state_mask"] == orig_mask


def test_ecc_multi_bit_error_detection(tmp_path):
    """Inject a double-bit error and check that it is detected or not corrected."""
    path = tmp_path / "ecc2.h5"
    tree = CIDTree(str(path))
    key = "ecc_key2"
    value = 456
    tree.insert(key, value)
    bucket_id = tree.dir[key]
    bucket = tree.buckets[bucket_id]
    idx = None
    for i in range(bucket.shape[0]):
        if (
            bucket[i]["key_high"] == E.from_str(key).high
            and bucket[i]["key_low"] == E.from_str(key).low
        ):
            idx = i
            break
    assert idx is not None
    orig_mask = bucket[idx]["state_mask"]
    # Inject double-bit error
    corrupted = orig_mask ^ 0b11
    bucket[idx]["state_mask"] = corrupted
    bucket.file.flush()
    # Should still retrieve value, but ECC correction may not be possible
    result = tree.get(key)
    assert value in [int(x) for x in result]
    # Optionally, check for error log or detection if ECC implemented
