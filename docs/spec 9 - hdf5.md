
# 9. HDF5 Layout

The HDF5 store is structured for fast concurrent access and dynamic bucket management:

```plaintext
root/
  ├── wal: Dataset         # Write-Ahead Log
  ├── config: Attribute    # Configuration (num_buckets, hash_seed, etc.)
  ├── buckets/             # Hash directory buckets (array of hash entries)
  └── values/              # External value-list datasets for multi-value keys
```

- Buckets: extensible, chunked, LZF-compressed for high throughput
- All metadata and value-lists are stored in fixed, predictable locations