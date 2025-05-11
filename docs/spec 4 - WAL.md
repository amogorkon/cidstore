## 4. WAL

- **Log Record Schema:** As described above in Section 4.1.
- **Checkpointing & Compaction:** WAL entries are periodically checkpointed and compacted to prevent unbounded growth.

### Locking & SWMR Compliance
- **Global RW Lock:** Ensures only one writer at a time (as required by HDF5 SWMR mode), while multiple readers access concurrently.
- **Writer Workflow:** Acquire global lock, perform copy-on-write updates (e.g., new dataset creation), update parent pointers atomically via attributes, and commit WAL entry.
- **Reader Workflow:** Multiple readers share access without blocking, per SWMR design.


### 4.1 WAL Mechanics
- **Schema Example:**
  ```python
  wal_record = {
      "txn_id": 123456789,
      "operation": "insert",
      "target_node": b"/sp/nodes/leaf/ab/...",
      "old_state": b"...",
      "new_state": b"...",
      "checksum": 0xABCD  # CRC32
  }
  ```
- **Recovery:** WAL entries are replayed to restore a consistent state.

### 4.2 Locking Granularity
- **Global RW Lock:** A single writer (holding a global lock) is used initially to ensure consistency under SWMR mode.
- **Future Optimization:** Per-node or subgroup-level mutex locks may be implemented.

### 4.3 SWMR Compatibility & Copy-on-Write Strategy
- **Copy-on-Write / Shadow Paging:** Operations that modify node structure (e.g., splits, merges) create new datasets instead of in-place modifications. Parent pointers (stored as attributes) are updated atomically.
- **Dataset Resizing:** Chunked, extendable datasets support safe appends where applicable.
- **Global RW Lock Enforcement:** Ensures one writer at a time; readers can access concurrently.
