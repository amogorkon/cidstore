"""constants.py - Canonical numpy dtypes, enums and constants for CIDTree."""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple, Optional

import numpy as np

from .logger import get_logger

logger = get_logger(__name__)

# Key (256-bit CID from SHA-256, stored as 4×64-bit components)
KEY_DTYPE = np.dtype([
    ("high", "<u8"),
    ("high_mid", "<u8"),
    ("low_mid", "<u8"),
    ("low", "<u8"),
])

# Bucket entry (hash table entry)
HASH_ENTRY_DTYPE = np.dtype([
    ("key_high", "<u8"),
    ("key_high_mid", "<u8"),
    ("key_low_mid", "<u8"),
    ("key_low", "<u8"),
    (
        "slots",
        [("high", "<u8"), ("high_mid", "<u8"), ("low_mid", "<u8"), ("low", "<u8")],
        2,
    ),  # 2 256-bit CIDs inline
    ("checksum", "<u8", 4),  # 4 x uint64 for checksum (u256)
])

# Directory pointer (for attribute/dataset/sharded modes)
BUCKET_POINTER_DTYPE = np.dtype([
    ("bucket_id", "<u8"),
    ("hdf5_ref", "<u8"),
])

# ValueSet (external value list for multi-value keys)
VALUESET_DTYPE = np.dtype([
    ("high", "<u8"),
    ("high_mid", "<u8"),
    ("low_mid", "<u8"),
    ("low", "<u8"),
])

# SpillPointer (reference to ValueSet dataset)
SPILL_POINTER_DTYPE = np.dtype([
    ("ref", "<u8"),
])

# HybridTime (if used for MVCC or versioning)
HYBRID_TIME_DTYPE = np.dtype([
    ("physical", "<u8"),  # nanoseconds since epoch
    ("logical", "<u4"),  # logical counter
    ("shard_id", "<u4"),  # optional: for sharded WALs
])

# Example: WAL and deletion log record dtypes using HybridTime
WAL_RECORD_DTYPE = np.dtype([
    ("op_ver", "<u1"),
    ("op_type", "<u1"),
    ("hybrid_time", HYBRID_TIME_DTYPE),
    ("k_high", "<u8"),
    ("k_high_mid", "<u8"),
    ("k_low_mid", "<u8"),
    ("k_low", "<u8"),
    ("v_high", "<u8"),
    ("v_high_mid", "<u8"),
    ("v_low_mid", "<u8"),
    ("v_low", "<u8"),
])

DELETION_RECORD_DTYPE = np.dtype([
    ("key_high", "<u8"),
    ("key_high_mid", "<u8"),
    ("key_low_mid", "<u8"),
    ("key_low", "<u8"),
    ("value_high", "<u8"),
    ("value_high_mid", "<u8"),
    ("value_low_mid", "<u8"),
    ("value_low", "<u8"),
    ("hybrid_time", HYBRID_TIME_DTYPE),
])

CONF = "/config"
BUCKS = "/buckets"
ROOT = "/root"
HDF5_NOT_OPEN_MSG = "HDF5 file is not open."

# WAL constants
MAX_TXN_RECORDS = 1000
RECORD_SIZE = 128  # Increased for 256-bit CIDs
# Record core: version_op(1) + reserved(1) + nanos(8) + seq(4) + shard_id(4) + 8×u64 for key/value(64) = 82 bytes
RECORD_CORE_SIZE = 82
CHECKSUM_SIZE = 4
PADDING_SIZE = RECORD_SIZE - RECORD_CORE_SIZE - CHECKSUM_SIZE  # 128 - 82 - 4 = 42


# Store constants
SPLIT_THRESHOLD: int = 128
DIRECTORY_DATASET_THRESHOLD = 1000
DIRECTORY_SHARD_THRESHOLD = 50000


class OpType(Enum):
    INSERT = 1
    DELETE = 2
    TXN_START = 3
    TXN_COMMIT = 4
    TXN_ABORT = 5

    @classmethod
    def from_value(cls, value: int) -> OpType:
        if not 0 <= value <= 0x3F:
            raise ValueError(f"OpType value {value} out of 6-bit range (0-63)")
        try:
            return cls(value)
        except ValueError as e:
            logger.warning(f"Unknown OpType value encountered: {value}")
            raise ValueError(f"Unknown OpType value: {value}") from e


class OpVer(Enum):
    PAST = 0
    NOW = 1
    NEXT = 2
    FUTURE = 3

    @classmethod
    def from_value(cls, value: int) -> "OpVer":
        if not 0 <= value <= 0x03:
            raise ValueError(f"OpVersion value {value} out of 2-bit range (0-3)")
        return cls(value)


class OP(NamedTuple):
    """Typed representation of a WAL operation record.

    Fields mirror the namedtuple previously used. All value components
    are Optional to allow representing delete/txn records where a value
    may not be present. Supports 256-bit CIDs with 4×64-bit components.
    """

    version: int  # OpVer
    optype: int  # OpType
    reserved: int  # currently unused
    nanos: int  # int, nanoseconds since epoch
    seq: int  # int, sequence number for this shard
    shard_id: int  # int, shard ID (0 for non-sharded WALs)
    k_high: int  # int, highest 64 bits of 256-bit key
    k_high_mid: int  # int, high-middle 64 bits of 256-bit key
    k_low_mid: int  # int, low-middle 64 bits of 256-bit key
    k_low: int  # int, lowest 64 bits of 256-bit key
    v_high: Optional[int]  # int | None, highest 64 bits of 256-bit value
    v_high_mid: Optional[int]  # int | None, high-middle 64 bits of 256-bit value
    v_low_mid: Optional[int]  # int | None, low-middle 64 bits of 256-bit value
    v_low: Optional[int]  # int | None, lowest 64 bits of 256-bit value
    checksum: int  # int, CRC32 checksum of the record core
