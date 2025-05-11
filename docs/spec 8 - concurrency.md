## 8. Concurrency & Crash Recovery

### WAL Mechanics
- **Log Record Schema:** As described in WAL section.
- **Checkpointing & Compaction:** WAL entries are periodically checkpointed and compacted to prevent unbounded growth.

### Locking & SWMR Compliance
- **Global RW Lock:** Ensures only one writer at a time (as required by HDF5 SWMR mode), while multiple readers access concurrently.
- **Writer Workflow:** Acquire global lock, perform copy-on-write updates (e.g., new dataset creation), update parent pointers atomically via attributes, and commit WAL entry.
- **Reader Workflow:** Multiple readers share access without blocking, per SWMR design.

### SWMR Compatibility and Copy-on-Write Strategy

- **Copy-on-Write / Shadow Paging:**
  - Create new datasets instead of modifying existing ones.
  - Atomically update parent pointer (attribute).
  - Use WAL for rollback support.

- **Dataset Resizing:**
  - Append allowed for chunked/resizable datasets.

- **Global RW Lock:**
  - One writer at a time (SWMR-compatible).
  - Readers are concurrent and unaffected.