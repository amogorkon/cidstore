# node.py

from abc import ABC, abstractmethod
from typing import List, Tuple

import h5py
import numpy as np

from .keys import E

# --- On‐disk dtypes ---
leaf_dtype = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("value_high", "<u8"),
    ("value_low", "<u8"),
    ("deleted", "i1"),
])
internal_key_dtype = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
])


class Node(ABC):
    def __init__(self, file: h5py.File, path: str):
        self.file = file
        self.path = path
        if path not in file:
            file.create_group(path)
        self.group = file[path]

    @abstractmethod
    def validate(self) -> bool: ...


class Leaf(Node):
    """
    On-disk leaf: appendix-only entries with tombstones.
    """

    CHUNK = 1024
    DTYPE = leaf_dtype

    def __init__(self, file: h5py.File, path: str):
        super().__init__(file, path)
        if "entries" not in self.group:
            self.group.create_dataset(
                "entries", shape=(0,), maxshape=(None,), dtype=self.DTYPE, chunks=True
            )
        self.ds: h5py.Dataset = self.group["entries"]

    def insert(self, key: E, value: E) -> None:
        arr = np.zeros(1, dtype=self.DTYPE)
        arr[0] = (key.high, key.low, value.high, value.low, 0)
        self.ds.resize((len(self.ds) + 1,))
        self.ds[-1] = arr[0]

    def is_overfull(self) -> bool:
        return len(self.ds) > self.CHUNK

    def lookup(self, key: E) -> List[E]:
        out: List[E] = []
        for row in self.ds:
            if (
                row["deleted"] == 0
                and int(row["key_high"]) == key.high
                and int(row["key_low"]) == key.low
            ):
                hi = int(row["value_high"])
                lo = int(row["value_low"])
                out.append(E((hi << 64) | lo))
        return out

    def delete(self, key: E) -> None:
        for i, row in enumerate(self.ds):
            if (
                row["deleted"] == 0
                and int(row["key_high"]) == key.high
                and int(row["key_low"]) == key.low
            ):
                self.ds[i]["deleted"] = 1

    def split(self) -> Tuple[str, E]:
        total = len(self.ds)
        pivot = total // 2
        base = self.path
        idx = 0
        while f"{base}{idx}" in self.file:
            idx += 1
        new_path = f"{base}{idx}"
        new_group = self.file.create_group(new_path)
        new_group.create_dataset(
            "entries",
            data=self.ds[pivot:],
            maxshape=(None,),
            dtype=self.DTYPE,
            chunks=True,
        )
        self.ds.resize((pivot,))
        # separator is first key of new leaf
        first = new_group["entries"][0]
        sep = E((int(first["key_high"]) << 64) | int(first["key_low"]))
        return new_path, sep

    def validate(self) -> bool:
        keys = [(int(r["key_high"]), int(r["key_low"])) for r in self.ds]
        return keys == sorted(keys)


class InternalNode(Node):
    """
    On-disk internal node: separate 'keys' and 'children' datasets.
    """

    def __init__(self, file: h5py.File, path: str):
        super().__init__(file, path)
        if "keys" not in self.group:
            self.group.create_dataset(
                "keys",
                shape=(0,),
                maxshape=(None,),
                dtype=internal_key_dtype,
                chunks=True,
            )
        if "children" not in self.group:
            dt = h5py.string_dtype(encoding="utf-8")
            self.group.create_dataset(
                "children", shape=(1,), maxshape=(None,), dtype=dt
            )
        self.keys_ds: h5py.Dataset = self.group["keys"]
        self.children_ds: h5py.Dataset = self.group["children"]

    def find_child(self, key: E) -> str:
        highs = self.keys_ds[:]["key_high"]
        idx = np.searchsorted(highs, key.high, side="right")
        return self.children_ds[idx]

    def insert(self, sep_key: E, new_child: str) -> None:
        highs = self.keys_ds[:]["key_high"].tolist()
        lows = self.keys_ds[:]["key_low"].tolist()
        idx = np.searchsorted(highs, sep_key.high)
        highs.insert(idx, sep_key.high)
        lows.insert(idx, sep_key.low)
        # rewrite keys
        self.keys_ds.resize((len(highs),))
        for i, (h, l) in enumerate(zip(highs, lows)):
            self.keys_ds[i] = (h, l)
        # rewrite children
        ch = self.children_ds[:].tolist()
        ch.insert(idx + 1, new_child)
        self.children_ds.resize((len(ch),))
        self.children_ds[:] = ch

    def split(self) -> Tuple[str, E]:
        total = len(self.keys_ds)
        mid = total // 2
        rec = self.keys_ds[mid]
        promoted = E((int(rec["key_high"]) << 64) | int(rec["key_low"]))
        base = self.path
        idx = 0
        while f"{base}{idx}" in self.file:
            idx += 1
        new_path = f"{base}{idx}"
        new_node = InternalNode(self.file, new_path)
        # move right-side keys
        right_keys = self.keys_ds[mid + 1 :]
        new_node.keys_ds.resize((len(right_keys),))
        new_node.keys_ds[:] = right_keys
        # move right-side children
        ch = self.children_ds[:].tolist()
        right_ch = ch[mid + 1 :]
        new_node.children_ds.resize((len(right_ch),))
        new_node.children_ds[:] = right_ch
        # shrink original
        self.keys_ds.resize((mid,))
        self.children_ds.resize((mid + 1,))
        return new_path, promoted

    def validate(self) -> bool:
        nk = len(self.keys_ds)
        nc = len(self.children_ds)
        if nc != nk + 1:
            return False
        highs = [int(r["key_high"]) for r in self.keys_ds]
        return highs == sorted(highs)
