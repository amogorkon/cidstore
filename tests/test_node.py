# Node structure tests for CIDTree
import numpy as np

from cidtree.node import InternalNode, Leaf, internal_key_dtype, leaf_dtype


def test_leaf_node_structure():
    arr = np.zeros(2, dtype=leaf_dtype)
    arr[0]["key_high"] = 1
    arr[0]["key_low"] = 2
    arr[0]["prev"] = b"0" * 32
    arr[0]["next"] = b"1" * 32
    node = Leaf(arr)
    assert node.ds.shape == (2,)
    assert node.ds[0]["key_high"] == 1
    assert node.ds[0]["prev"] == b"0" * 32


def test_internal_node_structure():
    arr = np.zeros(2, dtype=internal_key_dtype)
    arr[0]["key_high"] = 1
    arr[0]["child"] = b"a" * 32
    node = InternalNode(arr)
    assert node.ds.shape == (2,)
    assert node.ds[0]["child"] == b"a" * 32
