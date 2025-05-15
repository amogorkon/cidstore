"""node.py - Node structure and dtype definitions for CIDTree"""

"""node.py - Node structure and dtype definitions for CIDTree"""

import numpy as np

leaf_dtype = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("value_high", "<u8"),
    ("value_low", "<u8"),
    ("prev", "S16"),
    ("next", "S16"),
])

internal_keys_dtype = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
])
child_ptr_dtype = np.dtype("S16")


class NodeSchema:
    # Abstract base
    pass


class LeafNode(NodeSchema):
    def __init__(self, dataset):
        self.ds = dataset

    # methods: insert_entry, split, promote_multi_value, validate


class InternalNode(NodeSchema):
    def __init__(self, keys_ds, ptr_ds):
        self.keys = keys_ds
        self.children = ptr_ds

    # methods: insert_key, split, merge, validate
