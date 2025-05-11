## 2. Data Types

### 2.1 Key Components

| Component         | Structure                                      | Size  | Description                                                                 |
|-------------------|-----------------------------------------------|-------|-----------------------------------------------------------------------------|
| **Key**           | `[high: u64, low: u64]`                      | 16B   | Immutable 128-bit identifier derived from SHA3 hashes or composite triplestore logic. |
| **Hash Entry**    | `[key_high, key_low, value_group, value_start, value_count]` | 32B   | Maps a key to its value set location. Stored in hash buckets.              |
| **Value Set**     | `[value_high: u64, value_low: u64]`           | 16B   | Individual value in a sharded group dataset.                               |
| **WAL Record**    | `[txn_id, op_type, key, value, checksum]`     | Variable | Logs inserts/deletes for crash recovery.                                   |
| **Deletion Record** | `[key_high, key_low, value_group]`          | 28B   | Tracks obsolete keys for background GC.                                   |

### 2.2 Detailed Schemas

#### 2.2.1 Hash Table Entry

```python
hash_entry_dtype = np.dtype([
    ("key_high", "<u8"),      # 8B: High 64 bits of the key
    ("key_low", "<u8"),       # 8B: Low 64 bits of the key
    ("value_group", "S8"),    # 8B: Sharded group ID (e.g., "grp0012")
    ("value_start", "<u4"),   # 4B: Starting index in the group dataset
    ("value_count", "<u4")    # 4B: Number of values for this key
])  # Total = 32 bytes
```

**Atomicity**: Entries are immutable after creation to avoid locking during reads.

**Lookup Workflow**:

1. Hash key → bucket.
2. Scan bucket for matching `(key_high, key_low)`.
3. Read `value_group[value_start : value_start + value_count]`.

#### 2.2.2 Value Set Group

```python
value_dtype = np.dtype([
    ("value_high", "<u8"),  # 8B
    ("value_low", "<u8")    # 8B
])  # Total = 16 bytes per value
```

**Sharding**: Groups are named `/values/grpXXXX` (e.g., `grp0000`, `grp0001`).

**Storage**:

- Chunked into 64KB blocks (4,096 values/chunk).
- Compressed with LZF (lossless, fast decompression).
- Append-only; deletions are handled via tombstoning.

#### 2.2.3 WAL Record

```python
wal_dtype = np.dtype([
    ("txn_id", "<u8"),       # 8B: Monotonically increasing transaction ID
    ("op_type", "u1"),       # 1B: 0=insert, 1=delete
    ("key_high", "<u8"),     # 8B
    ("key_low", "<u8"),      # 8B
    ("value_high", "<u8"),   # 8B (for inserts only)
    ("value_low", "<u8"),    # 8B (for inserts only)
    ("checksum", "<u4")      # 4B: CRC32 of the entire record
])  # Total = 45 bytes
```

**Recovery**: During crashes, WAL is replayed in `txn_id` order.

**Efficiency**: Batched writes (e.g., 1K entries/write) reduce HDF5 overhead.

#### 2.2.4 Deletion Log

```python
delete_dtype = np.dtype([
    ("key_high", "<u8"),     # 8B
    ("key_low", "<u8"),      # 8B
    ("value_group", "S8"),   # 8B
    ("timestamp", "<u8")     # 8B: Unix epoch in nanoseconds
])  # Total = 32 bytes
```

**Garbage Collection**: Background process scans this log to:

- Remove tombstoned keys from hash buckets.
- Compact or delete empty value groups.

### 2.3 SWMR Metadata

Stored as HDF5 attributes under `/config`:

```python
{
    "num_buckets": "<u4",        # Current number of hash buckets
    "hash_seed": "<u8",          # Seed for extensible hash function
    "group_counter": "<u4",      # Next sharded group ID
    "wal_head": "<u8"            # Offset of the last committed WAL entry
}
```

### 2.4 Key Constraints

- **Fixed-Size Types**: All fields use fixed-width encodings for O(1) access.
- **No Variable-Length Strings**: `value_group` is a deterministic 8-byte ID (e.g., `grp0123` → hex-encoded `u32`).
- **Immutable Keys**: A key’s `value_group` and `value_start` cannot change after insertion.

### 2.5 Size Estimates

| Component       | 1B Keys, 100 Values/Key | Storage |
|------------------|--------------------------|---------|
| **Hash Table**   | 1B × 32B                | 32 GB   |
| **Value Sets**   | 100B × 16B              | 1.6 TB  |
| **WAL (Transient)** | 1B × 45B             | 45 GB*  |
| **Deletion Log** | 1M deletions × 32B      | 32 MB   |

*WAL is periodically compacted after checkpoints.