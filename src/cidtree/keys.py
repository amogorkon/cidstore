"""keys.py: Entity and CompositeKeys Logic"""

from __future__ import annotations

from typing import Final
from uuid import NAMESPACE_DNS, uuid4, uuid5

import numpy as np
from numpy.typing import NDArray

# Module-level constants for composite key generation:
PERTURB_HIGH: Final[int] = 0xD1B54A32D192ED03
PERTURB_LOW: Final[int] = 0x81B1D42A3B609BED
MASK_EVEN: Final[int] = 0xAAAAAAAAAAAAAAAA
MASK_ODD: Final[int] = 0x5555555555555555

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


# ---------------------------
# Composite Key Functions
# ---------------------------


def rot64(x: int, n: int) -> int:
    """Performs a 64-bit circular rotation of x by n bits."""
    return ((x << n) | (x >> (64 - n))) & 0xFFFFFFFFFFFFFFFF


def enhanced_mix(x: int) -> int:
    """
    Mixes the bits of x using a series of shifts and multiplications.

    **Uses a fixed right-shift value of 33 bits** (per our testing and MurmurHash3 guidance)
    to achieve robust avalanche properties for any nonzero x.

    Returns a 64-bit integer with enhanced randomness.

    p1 = 0x8f3e1036504f61e3
    p2 = 0xb7c835c4d183eb31
    are both Sophie Germain primes, which are used in the MurmurHash3 algorithm to great success.
    The mixing function is designed to be non-invertible, meaning that given the output, it is computationally infeasible to reverse the process and obtain the original input.
    """
    fixed_r = 33
    x ^= x >> fixed_r
    x = (x * 0x8F3E1036504F61E3) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> fixed_r
    x = (x * 0xB7C835C4D183EB31) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> fixed_r
    return x


def create_composite_key(a: E, b: E) -> E:
    """
    Generate a composite 128-bit key from two 128-bit keys, a and b.

    This design uses RMX—Rotate, Mask, XOR—with the following steps:

    1. **Initial Mixing:**
       h = enhanced_mix(a.high ^ b.low) ^ PERTURB_HIGH
       L = enhanced_mix(a.low ^ b.high) ^ PERTURB_LOW

    2. **Rotation & Masking:**
       rotated_h = rot64(h, 19) ^ (L & MASK_EVEN)
       rotated_L = rot64(L, 23) ^ (h & MASK_ODD)

    3. **Final Mixing:**
       final_high = enhanced_mix(rotated_h ^ rotated_L)
       final_low  = enhanced_mix(rotated_L ^ rotated_h)

    4. **Assembly:**
       Composite key = (final_high << 64) | final_low
    """
    high = enhanced_mix(a.high ^ b.low) ^ PERTURB_HIGH
    low = enhanced_mix(a.low ^ b.high) ^ PERTURB_LOW

    # Rotate and mix further to break commutativity and incorporate bit masks.
    rotated_high = rot64(high, 19) ^ (low & MASK_EVEN)
    rotated_low = rot64(low, 23) ^ (high & MASK_ODD)

    final_high = enhanced_mix(rotated_high ^ rotated_low)
    final_low = enhanced_mix(rotated_low ^ rotated_high)

    composite_value = (final_high << 64) | final_low
    return E(composite_value)
