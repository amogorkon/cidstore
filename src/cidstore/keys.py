"""keys.py - Entity (E) logic and key utilities"""

from __future__ import annotations

import hashlib
from uuid import uuid4

import numpy as np
from numpy.typing import NDArray

from cidstore.constants import HASH_ENTRY_DTYPE

from .jackhash import JACK_as_num, hexdigest_as_JACK, num_as_hexdigest
from .utils import assumption

_kv_store: dict[int, str] = {}

# 256-bit key dtype with 4×64-bit components
KEY_DTYPE = np.dtype([
    ("high", "<u8"),
    ("high_mid", "<u8"),
    ("low_mid", "<u8"),
    ("low", "<u8"),
])


class E(int):
    """
    E: 256-bit Content Identifier (CID) for CIDTree keys/values.

    - Immutable, hashable, and convertible to/from HDF5.
    - Uses full SHA-256 (256 bits) for content addressing.
    - Stored as 4×64-bit components: high, high_mid, low_mid, low
    - Used for all key/value storage and WAL logging.
    - See Spec 2 for canonical dtype and encoding.
    """

    def __getitem__(self, item):
        assert item in ("high", "high_mid", "low_mid", "low"), (
            "item must be 'high', 'high_mid', 'low_mid', or 'low'"
        )
        if item == "high":
            return [self.high]
        elif item == "high_mid":
            return [self.high_mid]
        elif item == "low_mid":
            return [self.low_mid]
        elif item == "low":
            return [self.low]
        else:
            raise KeyError(item)

    __slots__ = ()

    def __new__(
        cls,
        id_: int | str | list[int] | tuple[int, ...] | None = None,
        b: int | None = None,
        c: int | None = None,
        d: int | None = None,
    ) -> E:
        # Delegate to from_jackhash if a string is passed that looks like a JACK hash
        if isinstance(id_, str):
            return cls.from_jackhash(id_)
        if isinstance(id_, (list, tuple, np.void)):
            if len(id_) == 4:
                # 256-bit: (high, high_mid, low_mid, low)
                for i in id_:
                    assert assumption(i, int, np.uint64)
                a, b, c, d = id_
                return cls.from_int(
                    (int(a) << 192) | (int(b) << 128) | (int(c) << 64) | int(d)
                )
            elif len(id_) == 2:
                # Legacy 128-bit compatibility: (high, low) -> extend to 256-bit
                for i in id_:
                    assert assumption(i, int, np.uint64)
                a, b = id_
                # Zero-extend to 256 bits
                return cls.from_int((int(a) << 64) | int(b))
            else:
                raise ValueError(
                    f"Input must be a list of 2 or 4 integers, got {len(id_)}"
                )
        if id_ is None:
            # Generate random 256-bit UUID
            id_ = uuid4().int | (uuid4().int << 128)
        if b is not None:
            # Explicit 4-component construction
            assert assumption(b, int), "b must be an integer"
            if c is not None and d is not None:
                # All 4 components provided
                return cls.from_int(
                    (int(id_) << 192) | (int(b) << 128) | (int(c) << 64) | int(d)
                )
            else:
                # Only 2 components, legacy 128-bit mode
                return cls.from_int((int(id_) << 64) | int(b))
        return super().__new__(cls, id_)

    @property
    def value(self) -> str | None:
        return _kv_store.get(self)

    @property
    def high(self) -> int:
        """Highest 64 bits of the 256-bit CID"""
        return self >> 192

    @property
    def high_mid(self) -> int:
        """High-middle 64 bits of the 256-bit CID"""
        return (self >> 128) & ((1 << 64) - 1)

    @property
    def low_mid(self) -> int:
        """Low-middle 64 bits of the 256-bit CID"""
        return (self >> 64) & ((1 << 64) - 1)

    @property
    def low(self) -> int:
        """Lowest 64 bits of the 256-bit CID"""
        return self & ((1 << 64) - 1)

    def __repr__(self) -> str:
        return f"E('{hexdigest_as_JACK(num_as_hexdigest(self))}')"

    def __str__(self) -> str:
        return f"E('{hexdigest_as_JACK(num_as_hexdigest(self))}')"

    def to_hdf5(self) -> NDArray[np.void]:
        """Convert to HDF5-compatible array with 4×64-bit components"""
        return np.array(
            (self.high, self.high_mid, self.low_mid, self.low), dtype=KEY_DTYPE
        )

    @classmethod
    def from_entry(cls, entry: NDArray[np.void]) -> E:
        """
        Create an E from an HDF5 row. Accepts 256-bit fields
        ('high', 'high_mid', 'low_mid', 'low') or their key_/value_ prefixed variants.
        Also supports legacy 128-bit fields for backward compatibility.
        """
        fields = entry.dtype.fields
        if fields is not None:
            # Try 256-bit formats first
            if all(f in fields for f in ["high", "high_mid", "low_mid", "low"]):
                return cls(
                    (int(entry["high"]) << 192)
                    | (int(entry["high_mid"]) << 128)
                    | (int(entry["low_mid"]) << 64)
                    | int(entry["low"])
                )
            elif all(
                f in fields
                for f in ["key_high", "key_high_mid", "key_low_mid", "key_low"]
            ):
                return cls(
                    (int(entry["key_high"]) << 192)
                    | (int(entry["key_high_mid"]) << 128)
                    | (int(entry["key_low_mid"]) << 64)
                    | int(entry["key_low"])
                )
            elif all(
                f in fields
                for f in ["value_high", "value_high_mid", "value_low_mid", "value_low"]
            ):
                return cls(
                    (int(entry["value_high"]) << 192)
                    | (int(entry["value_high_mid"]) << 128)
                    | (int(entry["value_low_mid"]) << 64)
                    | int(entry["value_low"])
                )
            # Legacy 128-bit compatibility (zero-extended to 256 bits)
            elif "high" in fields and "low" in fields:
                return cls((int(entry["high"]) << 64) | int(entry["low"]))
            elif "key_high" in fields and "key_low" in fields:
                return cls((int(entry["key_high"]) << 64) | int(entry["key_low"]))
            elif "value_high" in fields and "value_low" in fields:
                return cls((int(entry["value_high"]) << 64) | int(entry["value_low"]))
        raise ValueError(
            "Input must have 256-bit fields (high/high_mid/low_mid/low) or legacy 128-bit fields (high/low)"
        )

    @classmethod
    def from_int(cls, id_: int) -> E:
        """Create an E from a 256-bit integer."""
        assert assumption(id_, int)
        assert id_ is not None and 0 <= id_ < (1 << 256), "ID must be a 256-bit integer"
        return cls(id_)

    @classmethod
    def from_jackhash(cls, value: str) -> E:
        """Create an E from a JACK hash string."""
        return cls(JACK_as_num(value))

    @classmethod
    def from_str(cls, value: str) -> E:
        """Create an E from a string using full SHA-256 (256 bits).

        Computes the SHA-256 hash of the input string and uses all
        256 bits as the Content Identifier (CID). The hash is split
        into 4×64-bit components for storage.

        Args:
            value: String to hash into a CID

        Returns:
            E: 256-bit CID from SHA-256(value)
        """
        assert assumption(value, str)
        # Compute SHA-256 hash (full 256 bits)
        hash_bytes = hashlib.sha256(value.encode("utf-8")).digest()
        # Split into 4×64-bit components
        high = int.from_bytes(hash_bytes[0:8], byteorder="big", signed=False)
        high_mid = int.from_bytes(hash_bytes[8:16], byteorder="big", signed=False)
        low_mid = int.from_bytes(hash_bytes[16:24], byteorder="big", signed=False)
        low = int.from_bytes(hash_bytes[24:32], byteorder="big", signed=False)
        # Combine into 256-bit integer
        id_ = (high << 192) | (high_mid << 128) | (low_mid << 64) | low
        _kv_store.setdefault(id_, value)
        return cls(id_)

    @classmethod
    def from_hdf5(cls, arr: NDArray[np.void]) -> E:
        """
        Create an E from an HDF5-compatible array (as produced by to_hdf5).
        Accepts a numpy structured array with fields 'high', 'high_mid', 'low_mid', 'low',
        or a shape (4,) array.
        """
        assert hasattr(arr, "dtype"), "Input must have a dtype attribute (numpy array)"
        assert arr.dtype == HASH_ENTRY_DTYPE or arr.dtype == KEY_DTYPE
        if arr.dtype.fields is not None:
            # Structured array with named fields
            if all(
                f in arr.dtype.fields for f in ["high", "high_mid", "low_mid", "low"]
            ):
                high = int(arr["high"])
                high_mid = int(arr["high_mid"])
                low_mid = int(arr["low_mid"])
                low = int(arr["low"])
                return cls.from_int(
                    (high << 192) | (high_mid << 128) | (low_mid << 64) | low
                )
        elif hasattr(arr, "shape") and arr.shape == (4,):
            # Plain array with 4 elements
            high, high_mid, low_mid, low = arr
            return cls.from_int(
                (int(high) << 192)
                | (int(high_mid) << 128)
                | (int(low_mid) << 64)
                | int(low)
            )
        raise ValueError(
            "Input must be a structured array with 256-bit fields or a shape (4,) array"
        )


def composite_key(subject: E, predicate: E, obj: E) -> E:
    """Create a composite key for SPO triple storage (Spec 20).

    For non-specialized predicates, we store triples using a composite key
    derived from (S, P, O). This enables triple storage without requiring
    a specialized plugin.

    The composite key is computed as:
        SHA-256(S || P || O)  where || is concatenation

    Uses the full 256-bit SHA-256 hash as the composite CID.

    Args:
        subject: Subject entity (E)
        predicate: Predicate entity (E)
        obj: Object entity (E)

    Returns:
        E: Composite key for the triple (256-bit CID from SHA-256)

    Example:
        >>> s = E.from_str("person:alice")
        >>> p = E.from_str("rel:knows")
        >>> o = E.from_str("person:bob")
        >>> key = composite_key(s, p, o)
    """
    # Concatenate the three 256-bit values into a single string
    # Format: S_high:S_high_mid:S_low_mid:S_low:P_high:P_high_mid:P_low_mid:P_low:O_high:O_high_mid:O_low_mid:O_low
    composite_str = (
        f"{subject.high}:{subject.high_mid}:{subject.low_mid}:{subject.low}:"
        f"{predicate.high}:{predicate.high_mid}:{predicate.low_mid}:{predicate.low}:"
        f"{obj.high}:{obj.high_mid}:{obj.low_mid}:{obj.low}"
    )
    # Hash to get deterministic E using SHA-256
    return E.from_str(composite_str)


def composite_value(predicate: E, obj: E) -> E:
    """Create a composite value encoding P and O for reverse lookup (Spec 20).

    For composite key storage, we need to store (P, O) as the value when
    using S as the lookup key. This function encodes P and O into a single E
    using SHA-256 hashing.

    Uses the full 256-bit SHA-256 hash as the composite value CID.

    Args:
        predicate: Predicate entity (E)
        obj: Object entity (E)

    Returns:
        E: Composite value encoding P and O (256-bit CID from SHA-256)

    Example:
        >>> p = E.from_str("rel:knows")
        >>> o = E.from_str("person:bob")
        >>> val = composite_value(p, o)
    """
    composite_str = (
        f"{predicate.high}:{predicate.high_mid}:{predicate.low_mid}:{predicate.low}:"
        f"{obj.high}:{obj.high_mid}:{obj.low_mid}:{obj.low}"
    )
    return E.from_str(composite_str)


def decode_composite_value(composite: E) -> tuple[E, E]:
    """Decode a composite value back into (P, O).

    This is the inverse of composite_value(). Note: this only works
    if the original predicate and object strings are still in the _kv_store.

    Args:
        composite: Composite value E

    Returns:
        tuple[E, E]: (predicate, object)

    Raises:
        ValueError: If composite value cannot be decoded
    """
    # Retrieve original string
    value_str = _kv_store.get(composite)
    if value_str is None:
        raise ValueError(f"Composite value {composite} not found in key-value store")

    # Parse format: P_high:P_high_mid:P_low_mid:P_low:O_high:O_high_mid:O_low_mid:O_low
    parts = value_str.split(":")
    if len(parts) != 8:
        raise ValueError(
            f"Invalid composite value format: expected 8 parts, got {len(parts)}"
        )

    try:
        p_high, p_high_mid, p_low_mid, p_low, o_high, o_high_mid, o_low_mid, o_low = (
            map(int, parts)
        )
        predicate = E.from_int(
            (p_high << 192) | (p_high_mid << 128) | (p_low_mid << 64) | p_low
        )
        obj = E.from_int(
            (o_high << 192) | (o_high_mid << 128) | (o_low_mid << 64) | o_low
        )
        return predicate, obj
    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to decode composite value: {e}") from e
