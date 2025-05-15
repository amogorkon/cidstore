

# 4. Write-Ahead Log (WAL)

```mermaid

> **Note:** For canonical data structure diagrams, see [Spec 2: Data Types and Structure](spec%202%20-%20Data%20Types%20and%20Structure.md#data-structure).

## 4.1 WAL Overview

| Feature           | Description                                                        |
|-------------------|--------------------------------------------------------------------|
| Log Record Schema | Records all mutating ops (insert, split, merge, delete)            |
| Checkpointing     | Periodic compaction to bound WAL size                              |
| Atomicity         | All ops use HDF5 CoW; WAL logs every step for recovery             |
| Recovery          | WAL replay restores consistent state after crash                    |

## 4.2 WAL Record Structure

See Section 2 for schema. WAL records log all mutating operations for recovery and replay. Example fields: `txn_id`, `op_type`, `key`, `value`, `checksum`.


## 4.3 Locking & SWMR Compliance

| Role    | Locking Model         | Workflow Summary                                 |
|---------|-----------------------|--------------------------------------------------|
| Writer  | Global RW lock        | Acquire lock → CoW update → commit WAL entry      |
| Reader  | SWMR, no lock needed  | Multiple readers access concurrently              |

## 4.4 Copy-on-Write & Atomic Updates

- All structure changes (splits, merges) use CoW and atomic pointer updates.
- Chunked datasets support safe appends and resizing.
- Global RW lock ensures single-writer safety.

---


## 4.5 WAL-Driven Adaptive Maintenance

WAL acts as a dynamic maintenance queue for bucket health and merge scheduling. Each bucket tracks pending entries and computes a **danger score** to trigger async merges.

| Metric       | Source/Trigger         | Role in Maintenance         |
|--------------|-----------------------|----------------------------|
| pending      | WAL entries           | Danger score (40%)         |
| unsorted     | On-disk unsorted      | Danger score (30%)         |
| last_merged  | UNIX timestamp        | Danger score (30%)         |
| danger_score | Computed from above   | Merge trigger              |
| lock         | Per-bucket RWLock     | Merge isolation            |
| merge log    | /sp/maintenance_log/  | Recovery, journaling       |

```mermaid
flowchart TD
    A[WAL Append] --> B{Danger score > threshold?}
    B -- Yes --> C[Schedule Async Merge]
    B -- No --> D[Return to Normal Ops]
    C --> E[Acquire bucket.lock]
    E --> F[Merge & sort layers]
    F --> G[Atomic swap, update metadata]
    G --> H[Release lock]
```

**Merge Algorithm:**
- Merge in-memory WAL buffer and on-disk unsorted region, then merge with sorted base.
- Use SIMD for parallel merge; atomic swap with journal for durability.

| Condition                     | Action      |
|-------------------------------|-------------|
| `len(delta) < DELTA_THRESHOLD`| Apply delta |
| Otherwise                     | Full merge  |

**Protections:**
- Throttle writers or activate hardware merge if danger_score > 0.9.
- Only allow appends in append-only mode; throttle on overload.

**Cost-Aware Scheduling:**
`priority = (query_rate * danger_score) / estimated_merge_cost`

**Recovery & Journaling:**
- All merges are journaled under `/sp/maintenance_log/` for rollback.
- Rollback via `triplestore recover --merge-point <timestamp>`.

**Summary:**
WAL-driven adaptive maintenance uses danger scores to schedule non-blocking, cost-aware merges, supporting robust recovery and efficient operation.
