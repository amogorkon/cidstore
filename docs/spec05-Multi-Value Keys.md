
# 5. Multi-Value Keys



> **Note:** For canonical data structure diagrams, see [Spec 2: Data Types and Structure](spec%202%20-%20Data%20Types%20and%20Structure.md#data-structure).


## Layout Summary

| Mode   | Slot[0] | Slot[1] |
|--------|--------------------------------|----------|
| Inline | CID     |  CID        |
| Spill  |0 | SpillPointer  |
---


#### Promotion Algorithm

- If a key has ≤2 values, store them inline in `slots[0]` and `slots[1]`.
- If a key receives a 3rd value, promote all values to an external value-list (spill mode).
- In spill mode, the entry stores a SpillPointer to the external dataset.
- **Note:** In this implementation, spill mode is indicated when `slot[0] == 0`.

---


```mermaid
flowchart TD
    A[Insert value CID] --> B{slot 0?}
    B -- 0 --> C{slot 1?}
    B -- not 0 --> C1{slot 1?}
    C1 -- 0 --> D[Store CID in slot 1] --> Z
    C1 -- not 0 --> G[Promote to SpillMode! Store CIDs in ValueSet, set slot 0 to 0, set slot 1 to SpillPointer] --> Z
    C -- 0 --> F[Store CID in slot 0] --> Z
    C -- not 0 --> E[SpillMode! Follow SpillPointer and store CID in ValueSet] --> Z
    Z[update checksum]
```



### 5.2 External Value Lists

For high-cardinality keys, an external HDF5 dataset (value list) is used. The bucket entry is updated to point to the external dataset. Queries retrieve the dataset in one read; deletions remove values and clean up empty datasets.


### 5.4 Value‑List Compaction

External value‑lists are append‑only with tombstoning. Compaction is a background process that removes tombstoned entries, orders lists, and reclaims space. Triggered by configurable thresholds, compaction is atomic and concurrent-safe, with metrics used to tune scheduling.

### 5.3 Hybrid & Promotion

Inline multi-values are used for low-cardinality keys; when a threshold is exceeded, promotion to an external value-list occurs (WAL-logged, atomic). Demotion is handled in maintenance to avoid thrashing. This hybrid approach reduces bucket bloat and ensures efficient memory use.


## 5.7 WAL‑Driven Adaptive Maintenance

See [Spec 4: Write-Ahead Log (WAL)](spec%204%20-%20WAL.md#45-wal-driven-adaptive-maintenance) for all details on danger score, merge scheduling, protections, and journaling.