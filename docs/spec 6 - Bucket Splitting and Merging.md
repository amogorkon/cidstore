# 6. Bucket Splitting and Merging

All rebalancing occurs at the bucket level; the hash directory is unordered.

## 6.1 Overview (UML)

```mermaid

> **Note:** For canonical data structure diagrams, see [Spec 2: Data Types and Structure](spec%202%20-%20Data%20Types%20and%20Structure.md#data-structure).

## 6.2 Atomicity & Integrity

| Guarantee              | Mechanism                                                      |
|------------------------|---------------------------------------------------------------|
| Copy-on-Write          | Splits/merges create new bucket datasets, never in-place mods  |
| WAL Logging            | All changes (splits/merges) logged for rollback/recovery      |
| Atomic Pointer Updates | Directory metadata updated atomically via HDF5 attributes      |

After each operation, validation confirms bucket and directory mapping consistency (fixed-width types, occupancy thresholds, CRC32C per chunk).

## 6.3 Maintenance & GC

| Task                | Purpose                                         |
|---------------------|-------------------------------------------------|
| Merge/sort          | Consolidate unsorted data for efficient lookups |
| Background process  | Maintains performance under high insert load     |

## 6.4 Promotion Thresholds & Trade-Offs

| Threshold      | Pros                              | Cons                                         |
|---------------|-----------------------------------|----------------------------------------------|
| Lower (e.g. 64)| Faster lookups after promotion    | More frequent promotions, more ext. datasets |
| Higher (128+)  | Fewer promotions, less overhead   | Longer scans if many duplicates accumulate   |

Promotion threshold is power-of-two, tunable via `/config/promotion_threshold` (default: 128).

## 6.5 Promotion Workflow (Diagram)

```mermaid
flowchart TD
    A[Insert/Update Key] --> B{Inline duplicates >= threshold?}
    B -- No --> C[Keep inline]
    B -- Yes --> D[Promote to external value-list]
    D --> E[Update directory mapping]
```

---

**Summary:**
- Splits and merges are atomic (CoW + WAL).
- Promotion threshold is configurable and impacts performance trade-offs.
- Maintenance and GC ensure efficient lookups and storage.