"""keys.py: Entity Logic"""

from __future__ import annotations

from uuid import NAMESPACE_DNS, uuid4, uuid5

import numpy as np
from numpy.typing import NDArray

_kv_store: dict[
    int, str
] = {}  # Placeholder for a key-value store, to be defined elsewhere

KEY_DTYPE = np.dtype([("high", "<u8"), ("low", "<u8")])
"Static dtype definition for HDF5 serialization"


class E(int):
    def __getitem__(self, item):
        if item == "high":
            return [self.high]
        elif item == "low":
            return [self.low]
        else:
            raise KeyError(item)

    __slots__ = ()

    def __new__(cls, id_: int | None = None) -> E:
        assert id_ is not None and 0 <= id_ < (1 << 128), "ID must be a 128-bit integer"
        if id_ is None:
            id_ = uuid4().int
        return super().__new__(cls, id_)

    @classmethod
    def from_str(cls, value: str) -> E:
        id_ = uuid5(NAMESPACE_DNS, value).int
        _kv_store.setdefault(id_, value)
        return cls(id_)

    @property
    def value(self) -> str | None:
        return _kv_store.get(self)

    @property
    def high(self) -> int:
        return self >> 64

    @property
    def low(self) -> int:
        return self & ((1 << 64) - 1)

    def __repr__(self) -> str:
        return f"E({hex(self)})"

    def __str__(self) -> str:
        hex_str = hex(self)
        return f"E({hex_str[2:9]}..)" if len(hex_str) > 8 else f"E({hex_str[2:]})"

    def to_hdf5(self) -> NDArray[np.void]:
        """Convert to HDF5-compatible array"""
        return np.array((self.high, self.low), dtype=KEY_DTYPE)

    @classmethod
    def from_hdf5(cls, arr: NDArray[np.void]) -> E:
        """
        Create an E from an HDF5 row. Accepts either ('high', 'low') or ('key_high', 'key_low') or ('value_high', 'value_low') fields.
        """
        # Try all possible field name pairs
        fields = arr.dtype.fields
        if fields is not None:
            for hi, lo in [
                ("high", "low"),
                ("key_high", "key_low"),
                ("value_high", "value_low"),
            ]:
                if hi in fields and lo in fields:
                    return cls((int(arr[hi].item()) << 64) | int(arr[lo].item()))
        raise ValueError("HDF5 row does not contain recognized high/low fields")
