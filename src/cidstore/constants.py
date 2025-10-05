"""constants.py - Canonical numpy dtypes, enums and constants for CIDTree."""

from __future__ import annotations

from typing import NamedTuple, Optional
from enum import Enum

import numpy as np

from .logger import get_logger

logger = get_logger(__name__)

# Key (128-bit)
KEY_DTYPE = np.dtype([("high", "<u8"), ("low", "<u8")])

# Bucket entry (hash table entry)
HASH_ENTRY_DTYPE = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("slots", [("high", "<u8"), ("low", "<u8")], 2),  # 2 CIDs inline
    ("checksum", "<u8", 2),  # 2 x uint64 for checksum (u128)
])

# Directory pointer (for attribute/dataset/sharded modes)
BUCKET_POINTER_DTYPE = np.dtype([
    ("bucket_id", "<u8"),
    ("hdf5_ref", "<u8"),
])

# ValueSet (external value list for multi-value keys)
VALUESET_DTYPE = np.dtype([
    ("value", "<u8"),
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
    ("k_low", "<u8"),
    ("v_high", "<u8"),
    ("v_low", "<u8"),
])

DELETION_RECORD_DTYPE = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("value_high", "<u8"),
    ("value_low", "<u8"),
    ("hybrid_time", HYBRID_TIME_DTYPE),
])

CONF = "/config"
BUCKS = "/buckets"
ROOT = "/root"
HDF5_NOT_OPEN_MSG = "HDF5 file is not open."

# WAL constants
MAX_TXN_RECORDS = 1000
RECORD_SIZE = 64
RECORD_CORE_SIZE = 50
CHECKSUM_SIZE = 4
PADDING_SIZE = RECORD_SIZE - RECORD_CORE_SIZE - CHECKSUM_SIZE


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

    Fields mirror the namedtuple previously used. `v_high` and `v_low`
    are Optional to allow representing delete/txn records where a value
    may not be present.
    """

    version: int  # OpVer
    optype: int  # OpType
    reserved: int  # currently unused
    nanos: int  # int, nanoseconds since epoch
    seq: int  # int, sequence number for this shard
    shard_id: int  # int, shard ID (0 for non-sharded WALs)
    k_high: int  # int, high part of key
    k_low: int  # int, low part of key
    v_high: Optional[int]  # int | None, high part of value
    v_low: Optional[int]  # int | None, low part of value
    checksum: int  # int, CRC32 checksum of the record core
