# 9. HDF5 Layout

The HDF5 store is structured for fast concurrent access and extendible hash directory management:

```plaintext
root/
  ├── config: Group
  │   ├── global_depth: Attribute (uint8)
  │   ├── num_buckets: Attribute (uint32)
  │   ├── hash_seed: Attribute (uint64)
  │   └── format_version: Attribute (uint32)
  ├── directory: Dataset             # Array of BucketPointer structs
  ├── wal: Dataset                   # Write-Ahead Log
  ├── buckets/                       # Hash directory buckets
  │   ├── bucket_0000: Dataset       # Each with local_depth attribute
  │   ├── bucket_0001: Dataset
  │   └── ...
  └── values/                        # External value-list datasets for multi-value keys
      ├── valueset_0000: Dataset
      ├── valueset_0001: Dataset
      └── ...
```

## 9.1 Directory Dataset Structure

The directory is stored as an HDF5 dataset of compound type:

```python
BucketPointer = np.dtype([
    ('bucket_id', 'u4'),      # 32-bit bucket identifier
    ('hdf5_ref', 'u8'),       # 64-bit HDF5 object reference
])
```

**Directory Properties:**
- Size: `2^global_depth` entries
- Chunked for efficient access patterns
- Compressed with LZF for metadata efficiency
- Indexed by top `global_depth` bits of `key.high`

## 9.2 Bucket Dataset Attributes

Each bucket dataset includes the following HDF5 attributes:

| Attribute | Type | Description |
|-----------|------|-------------|
| `local_depth` | uint8 | Number of key bits determining bucket membership |
| `sorted_count` | uint32 | Number of entries in sorted region |
| `last_compacted` | uint64 | Timestamp of last maintenance operation |
| `entry_count` | uint32 | Total number of entries (sorted + unsorted) |

## 9.3 Configuration Attributes

Global configuration stored under `/config` group:

| Attribute | Type | Description |
|-----------|------|-------------|
| `global_depth` | uint8 | Current directory depth (max 64 for 64-bit keys) |
| `num_buckets` | uint32 | Total number of unique buckets |
| `hash_seed` | uint64 | Seed for hash function (if needed) |
| `format_version` | uint32 | On-disk format version |
| `created_timestamp` | float64 | Creation time (Unix timestamp) |

## 9.4 Atomic Updates

**Directory Updates:**
- Directory dataset is updated atomically using HDF5 dataset writes
- Global depth changes are committed via attribute updates
- WAL logs all directory modifications for recovery

**Bucket Creation/Splitting:**
- New bucket datasets are created with CoW semantics
- Directory pointers updated atomically after bucket creation
- Old buckets remain until directory update commits

- Buckets: extensible, chunked, LZF-compressed for high throughput
- All metadata and value-lists are stored in fixed, predictable locations
- Extendible hashing enables efficient, deterministic bucket lookup and splitting