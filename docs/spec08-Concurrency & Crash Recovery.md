
# 8. Concurrency & Crash Recovery

## 8.1 WAL & Copy-on-Write

- All bucket-level modifications (insert, split/merge, promotion) are logged in WAL for recovery.
- All changes use copy-on-write (CoW) to ensure readers see a consistent snapshot.
- Checkpointing and compaction prevent WAL growth.

## 8.2 Locking & SWMR

- Single global RW lock for writer (HDF5 SWMR); readers access concurrently.

## 8.3 Crash Consistency Workflow

```mermaid
flowchart TD
    A[Acquire global lock] --> B[Read current bucket state]
    B --> C[WAL: log operation intent]
    C --> D[CoW: create new bucket(s)]
    D --> E[Validate new buckets]
    E --> F[WAL: log ready]
    F --> G[Atomic metadata update]
    G --> H[WAL: log commit]
    H --> I[Release lock]
    I --> J[Cleanup old buckets]
```

- If crash before metadata update: recovery ignores new buckets (not referenced)
- If crash after update: WAL commit ensures cleanup on next startup
- All steps are idempotent; WAL replay guarantees safe recovery

## 8.4 Metadata Consistency

- All directory metadata is updated atomically (HDF5 attributes)
- WAL logs every change before application
- Optional: checkpointing/double-write for extra safety

## 8.5 Concurrency Bottleneck

| Limitation             | Description                                         |
|-----------------------|-----------------------------------------------------|
| Global Writer Lock     | Only one writer at a time (HDF5 SWMR)               |
| Impact                | High write workloads can bottleneck                  |
| Mitigations           | Batch updates, efficient appends, atomic CoW         |
| Scaling Reads         | Multiple readers can access concurrently             |
| Scaling Writes        | Requires sharding or app-level parallelism           |
| Design Trade-off      | Serialization simplifies consistency                 |