"""dtypes.py - Canonical numpy dtypes for all core CIDStore structures"""

import numpy as np

# Key (128-bit)
KEY_DTYPE = np.dtype([("high", "<u8"), ("low", "<u8")])

# Bucket entry (hash table entry)
HASH_ENTRY_DTYPE = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("slots", "<u8", 2),  # 2 slots for inline/spill
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
