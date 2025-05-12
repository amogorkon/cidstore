# tree.py

from typing import List, Union

import h5py
from filelock import FileLock

from .keys import E
from .node import InternalNode, Leaf
from .storage import StorageManager
from .wal import WAL

LEAF_PREFIX = "/sp/nodes/leaf"
INTERNAL_PREFIX = "/sp/nodes/internal"
ROOT_ATTR = "root_node"

INTERNAL_MAX_KEYS = Leaf.CHUNK  # tune separately if desired


class BPlusTree:
    def _promote_multi_value_keys(self, leaf: Leaf) -> None:
        # For now, do nothing (stub for multi-value promotion)
        pass

    def _split_leaf(self, leaf: Leaf) -> None:
        # Split the leaf node and update parent/internal nodes as needed
        new_path, sep = leaf.split()
        # If root is a leaf, create a new root as internal node
        if self.root.name == leaf.path:
            # Create new internal node
            idx = 0
            while f"{INTERNAL_PREFIX}{idx}" in self.file:
                idx += 1
            internal_path = f"{INTERNAL_PREFIX}{idx}"
            grp = self.file.create_group(internal_path)
            internal = InternalNode(self.file, internal_path)
            # Set up keys and children (always as bytes)
            internal.keys_ds.resize((1,))
            internal.keys_ds[0] = (sep.high, sep.low)
            internal.children_ds.resize((2,))
            internal.children_ds[0] = (
                leaf.path.encode("utf-8") if isinstance(leaf.path, str) else leaf.path
            )
            internal.children_ds[1] = (
                new_path.encode("utf-8") if isinstance(new_path, str) else new_path
            )
            self.root = grp
            self.file.attrs[ROOT_ATTR] = internal_path
        else:
            # Find parent and insert separator key and new child pointer
            self._insert_in_parent(leaf.path, sep, new_path)

    def _insert_in_parent(self, old_child: str, sep: E, new_child: str) -> None:
        # Always compare and store child paths as bytes
        old_child_bytes = (
            old_child.encode("utf-8") if isinstance(old_child, str) else old_child
        )
        new_child_bytes = (
            new_child.encode("utf-8") if isinstance(new_child, str) else new_child
        )
        path = self.root.name
        if path.startswith(LEAF_PREFIX):
            return  # Should not happen
        node = InternalNode(self.file, path)
        while True:
            ch = node.children_ds[:]
            ch_bytes = [
                c if isinstance(c, bytes) else str(c).encode("utf-8") for c in ch
            ]
            if old_child_bytes in ch_bytes:
                idx = ch_bytes.index(old_child_bytes)
                node.insert(sep, new_child)  # node.insert will encode as needed
                if len(node.keys_ds) > INTERNAL_MAX_KEYS:
                    self._split_internal(node)
                return
            # Descend to the correct child
            idx = ch_bytes.index(old_child_bytes) if old_child_bytes in ch_bytes else -1
            if idx == -1:
                return  # Not found
            next_child = ch[idx]
            # Decode for path
            if isinstance(next_child, bytes):
                next_child_str = next_child.decode("utf-8").rstrip("\x00")
            else:
                next_child_str = str(next_child)
            if next_child_str.startswith(LEAF_PREFIX):
                return
            node = InternalNode(self.file, next_child_str)

    def _split_internal(self, node: InternalNode) -> None:
        # Split the internal node and update parent as needed
        new_path, sep = node.split()
        if self.root.name == node.path:
            # Create new root
            idx = 0
            while f"{INTERNAL_PREFIX}{idx}" in self.file:
                idx += 1
            internal_path = f"{INTERNAL_PREFIX}{idx}"
            grp = self.file.create_group(internal_path)
            internal = InternalNode(self.file, internal_path)
            internal.keys_ds.resize((1,))
            internal.keys_ds[0] = (sep.high, sep.low)
            internal.children_ds.resize((2,))
            internal.children_ds[0] = (
                node.path.encode("utf-8") if isinstance(node.path, str) else node.path
            )
            internal.children_ds[1] = (
                new_path.encode("utf-8") if isinstance(new_path, str) else new_path
            )
            self.root = grp
            self.file.attrs[ROOT_ATTR] = internal_path
        else:
            # Find parent and insert separator key and new child pointer
            self._insert_in_parent(node.path, sep, new_path)

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

            from .wal import OpType

            txn_id = int(self.wal.ds.shape[0])
            rec = {
                "txn_id": txn_id,
                "op_type": OpType.INSERT.value,
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

            from .wal import OpType

            txn_id = int(self.wal.ds.shape[0])
            rec = {
                "txn_id": txn_id,
                "op_type": OpType.DELETE.value,
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
        leaf = f"{LEAF_PREFIX}0"
        if leaf not in self.file:
            grp = self.file.create_group(leaf)
            Leaf(self.file, leaf)
            return grp
        return self.file[leaf]
