# tree.py

import h5py
import numpy as np
from filelock import FileLock
from typing import List, Union, Tuple

from .config import VALUES_GROUP
from .keys import E
from .node import Leaf, InternalNode
from .storage import StorageManager
from .wal import WAL

LEAF_PREFIX = "/sp/nodes/leaf"
INTERNAL_PREFIX = "/sp/nodes/internal"
ROOT_ATTR = "root_node"

INTERNAL_MAX_KEYS = Leaf.CHUNK  # tune separately if desired


class BPlusTree:
    def __init__(self, storage: StorageManager):
        self.storage = storage
        storage.apply_insert = self.insert
        storage.apply_delete = self.delete

        self.lock = FileLock(f"{storage.path}.lock")
        self.file = storage.open()
        self.wal = WAL(storage)

        root_name = self.file.attrs.get(ROOT_ATTR)
        if root_name and root_name in self.file:
            self.root = self.file[root_name]
        else:
            self.root = self._create_root()
            self.file.attrs[ROOT_ATTR] = self.root.name

    def insert(self, key: Union[E, str], value: Union[E, int]) -> None:
        key = key if isinstance(key, E) else E.from_str(str(key))
        value = value if isinstance(value, E) else E(value)
        with self.lock:
            leaf = self._find_leaf(key)
            leaf.insert(key, value)

            txn_id = int(self.wal.ds.shape[0])
            rec = {
                "txn_id": txn_id,
                "op_type": WAL.OpType.INSERT.value,
                "key_high": key.high,
                "key_low": key.low,
                "value_high": value.high,
                "value_low": value.low,
            }
            self.wal.append([rec])
            self.storage.flush()

            if leaf.is_overfull():
                self._promote_multi_value_keys(leaf)
                if leaf.is_overfull():
                    self._split_leaf(leaf)

    def lookup(self, key: Union[E, str]) -> List[E]:
        key = key if isinstance(key, E) else E.from_str(str(key))
        leaf = self._find_leaf(key)
        return leaf.lookup(key)

    def delete(self, key: Union[E, str]) -> None:
        key = key if isinstance(key, E) else E.from_str(str(key))
        with self.lock:
            leaf = self._find_leaf(key)
            leaf.delete(key)

            txn_id = int(self.wal.ds.shape[0])
            rec = {
                "txn_id": txn_id,
                "op_type": WAL.OpType.DELETE.value,
                "key_high": key.high,
                "key_low": key.low,
                "value_high": 0,
                "value_low": 0,
            }
            self.wal.append([rec])
            self.storage.flush()

    def _find_leaf(self, key: E) -> Leaf:
        path = self.root.name
        if path.startswith(LEAF_PREFIX):
            return Leaf(self.file, path)

        node = InternalNode(self.file, path)
        while True:
            child = node.find_child(key)
            if child.startswith(LEAF_PREFIX):
                return Leaf(self.file, child)
            node = InternalNode(self.file, child)

    def _create_root(self) -> h5py.Group:
        leaf0 = f"{LEAF_PREFIX}0"
        if leaf0 not in self.file:
            self.file.create_group(leaf0)
            Leaf(self.file, leaf)
