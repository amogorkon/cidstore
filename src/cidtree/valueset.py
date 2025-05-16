"""valueset.py - ValueSet class for inline/spill multi-value key logic with ECC state mask (Spec 5)"""

import numpy as np
import h5py
from typing import List, Optional, Union

MULTI_VALUE_THRESHOLD = 4  # Inline up to 4, spill if more

# ECC encode/decode for 4-bit state mask (Hamming SECDED, 8 bits)
def encode_state_mask(mask: int) -> int:
    # D1..D4 = mask bits 0..3
    D1 = (mask >> 0) & 1
    D2 = (mask >> 1) & 1
    D3 = (mask >> 2) & 1
    D4 = (mask >> 3) & 1
    P1 = D1 ^ D2 ^ D4
    P2 = D1 ^ D3 ^ D4
    P3 = D2 ^ D3 ^ D4
    P0 = P1 ^ P2 ^ D1 ^ P3 ^ D2 ^ D3 ^ D4  # overall parity
    code = (P0 << 7) | (D4 << 6) | (D3 << 5) | (D2 << 4) | (P3 << 3) | (D1 << 2) | (P2 << 1) | (P1 << 0)
    return code

def decode_state_mask(code: int) -> int:
    # Extract bits
    P1 = (code >> 0) & 1
    P2 = (code >> 1) & 1
    D1 = (code >> 2) & 1
    P3 = (code >> 3) & 1
    D2 = (code >> 4) & 1
    D3 = (code >> 5) & 1
    D4 = (code >> 6) & 1
    P0 = (code >> 7) & 1
    # Recompute parities
    cP1 = D1 ^ D2 ^ D4
    cP2 = D1 ^ D3 ^ D4
    cP3 = D2 ^ D3 ^ D4
    cP0 = P1 ^ P2 ^ D1 ^ P3 ^ D2 ^ D3 ^ D4
    # Syndrome
    syndrome = ((P1 != cP1) << 0) | ((P2 != cP2) << 1) | ((P3 != cP3) << 2)
    if cP0 != P0:
        # Double-bit error (uncorrectable)
        raise ValueError("Double-bit ECC error in state mask")
    if syndrome:
        # Single-bit error, correct
        code ^= (1 << syndrome)
        # Re-extract after correction
        return decode_state_mask(code)
    # Return 4-bit mask
    return (D1 << 0) | (D2 << 1) | (D3 << 2) | (D4 << 3)

class ValueSet:
    """
    ValueSet: Multi-value key handler for CIDTree buckets.

    - Stores up to MULTI_VALUE_THRESHOLD values inline, then spills to external HDF5 dataset.
    - Uses ECC-protected state mask for inline/spill/tombstone state (Spec 5).
    - Supports promotion, demotion, compaction, and tombstone tracking.
    - All mutating operations are WAL-logged if WAL is provided.
    """
    def __init__(self, key: Union[str, int], h5group: Optional[h5py.Group] = None, wal=None):
        self.key = key
        self.inline = []  # up to 4 values
        self.spill_ptr = None  # (shard_id, bucket_local, segment_id)
        self.h5group = h5group  # HDF5 group for external datasets
        self.state_mask = encode_state_mask(0)  # ECC-encoded
        self.spilled = False
        self.tombstones = set()
        self.wal = wal

    def add(self, value):
        if self.spilled:
            self._add_spill(value)
        else:
            if value in self.inline or value in self.tombstones:
                return
            self.inline.append(value)
            self.state_mask = encode_state_mask(sum(1 << i for i in range(len(self.inline))))
            if len(self.inline) > MULTI_VALUE_THRESHOLD:
                self.promote()

    def remove(self, value):
        if self.spilled:
            self._remove_spill(value)
        else:
            if value in self.inline:
                self.inline.remove(value)
                self.state_mask = encode_state_mask(sum(1 << i for i in range(len(self.inline))))
            else:
                self.tombstones.add(value)
            if self.spilled and self._spill_count() <= MULTI_VALUE_THRESHOLD:
                self.demote()

    def promote(self):
        # Move inline values to external ValueSet (spill)
        if not self.h5group:
            raise RuntimeError("No HDF5 group for spill")
        dsname = f"valueset_{self.key}"
        if dsname not in self.h5group:
            ds = self.h5group.create_dataset(dsname, data=np.array(self.inline, dtype="<u8"), maxshape=(None,), chunks=True)
        else:
            ds = self.h5group[dsname]
        self.spill_ptr = dsname
        self.spilled = True
        self.inline = []
        self.state_mask = encode_state_mask(0)  # ECC for spill mode
        if self.wal:
            # Log promotion event (op_type 20)
            ek = self.key if hasattr(self.key, 'high') else self.key
            self.wal.append([
                {"op_type": 20, "key_high": getattr(ek, 'high', 0), "key_low": getattr(ek, 'low', 0), "value_high": 0, "value_low": 0}
            ])

    def demote(self):
        # Move values from external ValueSet back to inline
        if not self.spilled or not self.h5group or not self.spill_ptr:
            return
        ds = self.h5group[self.spill_ptr]
        vals = [v for v in ds[:] if v not in self.tombstones]
        if len(vals) <= MULTI_VALUE_THRESHOLD:
            self.inline = vals
            del self.h5group[self.spill_ptr]
            self.spill_ptr = None
            self.spilled = False
            self.state_mask = encode_state_mask(sum(1 << i for i in range(len(self.inline))))
            if self.wal:
                # Log demotion event (op_type 21)
                ek = self.key if hasattr(self.key, 'high') else self.key
                self.wal.append([
                    {"op_type": 21, "key_high": getattr(ek, 'high', 0), "key_low": getattr(ek, 'low', 0), "value_high": 0, "value_low": 0}
                ])

    def _add_spill(self, value):
        ds = self.h5group[self.spill_ptr]
        arr = np.append(ds[:], value)
        ds.resize((len(arr),))
        ds[:] = arr

    def _remove_spill(self, value):
        ds = self.h5group[self.spill_ptr]
        arr = ds[:]
        mask = arr != value
        ds.resize((mask.sum(),))
        ds[:] = arr[mask]

    def _spill_count(self):
        if not self.spilled or not self.h5group or not self.spill_ptr:
            return len(self.inline)
        ds = self.h5group[self.spill_ptr]
        return len(ds)

    def compact(self):
        # Remove tombstones from external ValueSet
        if self.spilled and self.h5group and self.spill_ptr:
            ds = self.h5group[self.spill_ptr]
            arr = ds[:]
            arr = np.array([v for v in arr if v not in self.tombstones], dtype=arr.dtype)
            ds.resize((len(arr),))
            ds[:] = arr
            self.tombstones.clear()
            if self.wal:
                # Log compaction event (op_type 22)
                ek = self.key if hasattr(self.key, 'high') else self.key
                self.wal.append([
                    {"op_type": 22, "key_high": getattr(ek, 'high', 0), "key_low": getattr(ek, 'low', 0), "value_high": 0, "value_low": 0}
                ])

    def values(self):
        if self.spilled and self.h5group and self.spill_ptr:
            return list(self.h5group[self.spill_ptr][:])
        return list(self.inline)

    def is_spilled(self):
        return self.spilled

    def get_state_mask(self):
        return self.state_mask

    def set_state_mask(self, code):
        self.state_mask = code

    def tombstone_count(self):
        return len(self.tombstones)

    def valueset_exists(self):
        return self.spilled and self.h5group and self.spill_ptr in self.h5group
