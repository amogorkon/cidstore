# Node structure tests for CIDTree

from cidtree.keys import E
from cidtree.node import InternalNode, Leaf


def test_leaf_node_structure(tmp_path):
    import h5py

    h5file = h5py.File(tmp_path / "test_leaf.h5", "w")
    node = Leaf(h5file, "/leaf0")
    node.insert(E(1 << 64), E(2 << 64))
    assert node.ds.shape == (1,)
    assert node.ds[0]["key_high"] == 1
    h5file.close()


def test_internal_node_structure(tmp_path):
    import h5py

    h5file = h5py.File(tmp_path / "test_internal.h5", "w")
    node = InternalNode(h5file, "/internal0")
    # Insert a key and a child
    node.insert(E(1 << 64), "/leaf0")
    assert node.keys_ds.shape == (1,)
    assert node.keys_ds[0]["key_high"] == 1
    assert node.children_ds[1].decode() == "/leaf0"
    h5file.close()
