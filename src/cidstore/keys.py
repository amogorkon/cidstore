"""keys.py - Entity (E) logic and key utilities"""

from __future__ import annotations

from uuid import NAMESPACE_DNS, uuid4, uuid5

import numpy as np
from numpy.typing import NDArray

from cidstore.constants import HASH_ENTRY_DTYPE

from .jackhash import JACK_as_num, hexdigest_as_JACK, num_as_hexdigest
from .utils import assumption

_kv_store: dict[int, str] = {}

KEY_DTYPE = np.dtype([("high", "<u8"), ("low", "<u8")])


class E(int):
    """
    E: 128-bit entity identifier for CIDTree keys/values.

    - Immutable, hashable, and convertible to/from HDF5.
    - Used for all key/value storage and WAL logging.
    - See Spec 2 for canonical dtype and encoding.
    """

    def __getitem__(self, item):
        assert item in ("high", "low"), "item must be 'high' or 'low'"
        if item == "high":
            return [self.high]
        elif item == "low":
            return [self.low]
        else:
            raise KeyError(item)

    __slots__ = ()

    def __new__(
        cls,
        id_: int | str | list[int] | tuple[int, int] | None = None,
        b: int | None = None,
    ) -> E:
        # Delegate to from_jackhash if a string is passed that looks like a JACK hash
        if isinstance(id_, str):
            return cls.from_jackhash(id_)
        if isinstance(id_, (list, tuple, np.void)):
            assert len(id_) == 2, "input must be a list of two integers"
            # Check all elements are int or np.uint64
            for i in id_:
                assert assumption(i, int, np.uint64)
            a, b = id_
            return cls.from_int((int(a) << 64) | int(b))
        if id_ is None:
            id_ = uuid4().int
        if b is not None:
            assert assumption(b, int), "b must be an integer"
            return cls.from_int((int(id_) << 64) | int(b))
        return super().__new__(cls, id_)

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
        return f"E('{hexdigest_as_JACK(num_as_hexdigest(self))}')"

    def __str__(self) -> str:
        return f"E('{hexdigest_as_JACK(num_as_hexdigest(self))}')"

    def to_hdf5(self) -> NDArray[np.void]:
        # No assert needed, self is E
        """Convert to HDF5-compatible array"""
        return np.array((self.high, self.low), dtype=KEY_DTYPE)

    @classmethod
    def from_entry(cls, entry: NDArray[np.void]) -> E:
        """
        Create an E from an HDF5 row. Accepts either ('high', 'low') or ('key_high', 'key_low') or ('value_high', 'value_low') fields.
        """
        assert entry.dtype == HASH_ENTRY_DTYPE

        return cls((int(entry["high"].item()) << 64) | int(entry["low"].item()))

    @classmethod
    def from_int(cls, id_: int) -> E:
        """
        Create an E from an integer. Copilot is confused without it.
        """
        assert assumption(id_, int)
        assert id_ is not None and 0 <= id_ < (1 << 128), "ID must be a 128-bit integer"
        return cls(id_)

    @classmethod
    def from_jackhash(cls, value: str) -> E:
        """
        Create an E from a JACK hash string.
        """
        return cls(JACK_as_num(value))

    @classmethod
    def from_str(cls, value: str) -> E:
        assert assumption(value, str)
        id_ = uuid5(NAMESPACE_DNS, value).int
        _kv_store.setdefault(id_, value)
        return cls(id_)
