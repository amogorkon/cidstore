3. Bucket Structure and Directory Management

# 3. Bucket Structure & Directory Management

> **Note:** For canonical data structure diagrams, see [Spec 2: Data Types and Structure](spec%202%20-%20Data%20Types%20and%20Structure.md#data-structure).

The hash directory is an array of buckets, each stored as an HDF5 dataset. Buckets use a dynamic in-place sorted region for fast inserts and efficient lookups.


### 3.1 Bucket Abstraction

| Component      | Description                                                      |
|---------------|------------------------------------------------------------------|
| Dataset       | Contiguous array of hash entries (`hash_entry_dtype`)             |
| Sorted Region | `sorted_count` entries, binary search for O(log N) lookup        |
| Unsorted      | Appended entries, linear scan; merged on maintenance             |

**Lookup:**
| Step   | Action                                      |
|--------|---------------------------------------------|
| 1      | Binary search in sorted region               |
| 2      | Linear scan in unsorted region if not found  |

**Insert:**
| Step   | Action                                      |
|--------|---------------------------------------------|
| 1      | Append to unsorted region                    |
| 2      | `sorted_count` unchanged until maintenance   |


### 3.2 In-Place Sorting / Merging

| Trigger                | Example/Condition         |
|------------------------|--------------------------|
| Unsorted > threshold   | Configurable             |
| Low write activity     | Background maintenance   |
| Regular maintenance    | Periodic scan            |

**Merge/Sort Workflow:**
```mermaid
flowchart TD
    A[Insert Entry] --> B[Append to unsorted region]
    B --> C{Maintenance Trigger?}
    C -- No --> D[Continue]
    C -- Yes --> E[Sort unsorted region]
    E --> F[Merge with sorted region]
    F --> G[Update sorted_count]
    G --> H[Atomic update (CoW + WAL)]
```


### 3.3 Concurrency Adjustments

| Aspect         | Guarantee/Approach                                                      |
|--------------- |-------------------------------------------------------------------------|
| SWMR           | Single-writer (HDF5 SWMR); exclusive lock for sorting                   |
| WAL Logging    | All merge/sort ops logged for crash recovery                            |
| Reader Impact  | Readers see old bucket until atomic swap completes                      |




#### Bucket Splitting and Directory Resizing

| Trigger            | Action                                                      |
|--------------------|------------------------------------------------------------|
| Overloaded bucket  | Split using next bit of `key_high`                          |
| Full bucket        | Create two buckets with longer prefix, update `num_buckets` |

Splits are atomic (HDF5 CoW) and logged (WAL) for recovery.





## 3.6 Caching Strategies

Efficient in-memory caching is critical for high-throughput and low-latency operation. CIDTree employs several targeted caches:


### 3.6.1 In‑Memory Bucket Metadata Cache

- Caches `BucketHealth`, inline slots, and spill pointers for recently accessed buckets.
- Uses LRU/ARC keyed by `bucket_id`.
- Evicts least-recently-used entries as needed.
- Invalidation occurs on merge/maintenance or WAL replay affecting a cached bucket.

---


### 3.6.2 ValueSet Dataset Cache

- Caches recently read external datasets under `/hash/shards/.../values`.
- Uses LRU for open HDF5 handles; supports chunk prefetch for sequential scans.

---


### 3.6.3 Directory Index Cache

- Caches hot shard mappings from `bucket_id` to `shard_id` to avoid repeated lookups.

---


### 3.6.4 Write‑Through & Consistency

- Inline cache is write-through: updates on every WAL insert/delete.
- Spill transitions and maintenance merges invalidate or refresh affected cache entries.

---


### 3.6.5 Monitoring Cache Effectiveness

Cache stats are exposed via metrics (hit/miss/ratio/entries) for observability.

To ensure safe evolution of on-disk formats, data types, and directory layouts, CIDTree introduces explicit **versioning**, **schema registration**, and **automated migration** steps.

### 3.5.1 Versioning Scheme

- **Format Version**: A 32-bit integer stored as an HDF5 attribute under `/config/format_version`.
- **Semantic Version**: A string `MAJOR.MINOR.PATCH` in `/config/version_string` for human reference.

| MAJOR | Incompatible on-disk change (require full migration) |
|-------|------------------------------------------------------|
| MINOR | Backwards-compatible additions (e.g. new dataset, new field) |
| PATCH | Bug fixes, maintenance (no schema change) |

---

### 3.5.2 On-Disk Version Attributes

```yaml
/config:
  attrs:
    format_version:   <u4>       # bump on any structural change
    version_string:   "1.2.0"    # human-readable
    created_timestamp: <f8>
```
`format_version` is checked on file open.

If `format_version` > code_supported_version, the system refuses to open (to prevent data loss).

If `format_version` < code_supported_version, migrations are applied automatically.

### 3.5.3 Schema Evolution Strategies

**Additive Fields**

New datasets or attributes may be added without breaking readers of older fields. Always provide a default for any new attribute or field so old files can be opened seamlessly.

**Deprecated Paths**

Mark old groups/datasets as deprecated by adding an attribute `deprecated = true`. Migration tool can coalesce or rename these to new paths.

**Field Renaming**

Introduce new field alongside the old, migrate data (via script), then deprecate old field in the next MAJOR bump.

### 3.5.4 Automated Migration Tools

Provide a CLI command:

```bash
cidtree migrate --from-format 1 --to-format 2 --input file.h5 --output file-v2.h5
```

**Steps:**

1. Open `file.h5`, verify `format_version == from-format`.
2. Apply per-format migration functions in sequence (e.g. `migrate_1_to_2(h5file)`):
   - Add new `/hash/shards` group.
   - Move existing `/hash/buckets` into the first shard.
   - Update `format_version = 2`.
3. Write updated file to `file-v2.h5` (or in-place with backup).
4. Validate using new schema invariants, logging any anomalies.

Each migration function is idempotent and reversible (via backup).

### 3.5.5 In-Place Upgrades with Rollback

On startup, if `format_version < current_version`, run migrations in-place under a global lock:

- Backup the original file to `file.h5.bak`
- Migrate each step, committing updates and bumping `format_version`
- Validate; if any error, restore from backup and exit.

Operators can opt out of automatic in-place migration and use the CLI tool instead.


### 3.5.6 Migration Examples

- **Adding state_mask ECC:** Upgrade all index entries to 8-bit SECDED code and bump `format_version`.
- **Sharding Buckets:** Move all buckets under `/hash/shards/shard0000/`, update pointers, and bump `format_version`.


### 3.5.7 Backwards Compatibility & Roll-Forward

Code supports reading any compatible `format_version`. New clients write the latest format. Explicit versioning and reversible migrations ensure safe, predictable upgrades.

To operate and tune the system in production, CIDTree exposes rich metrics, logs, and traces for maintenance, garbage collection, and error conditions.


### 3.4.1 Metrics

Metrics include bucket health, merge/GC stats, and error counters. All are exposed for monitoring and alerting.

---


### 3.4.2 Logging

Logs include merge/GC events, danger_score warnings, and error conditions. Log lines are structured for easy parsing and alerting.


### 3.4.3 Tracing

Traces span inserts, maintenance, and WAL operations, with trace IDs for cross-correlation.


### 3.4.4 Exposing Metrics

Metrics are exported via Prometheus and HDF5 snapshots, with alerting on key thresholds.

With these observability hooks, operators gain full visibility into bucket health, merge performance, GC progress, and error conditions—enabling rapid diagnosis, tuning, and automated alerting.

**Directory Management**

| Feature            | Mechanism/Guarantee                                                      |
|--------------------|--------------------------------------------------------------------------|
| Bucket ID          | Prefix of high 64 bits of key                                            |
| Split Trigger      | Overload → split by one more bit                                         |
| Directory Growth   | Only affected region grows                                               |
| Copy-on-Write      | New bucket dataset created, old left intact until ready                  |
| Atomic Metadata    | Directory (bucket index→dataset) updated atomically (HDF5 attr/metadata) |
| WAL Logging        | All splits/updates logged for crash recovery                             |
| Reader Consistency | Readers see either old or new state, never in-between                    |
| **Scalability**    | See below for dedicated/sharded directory dataset options                |


## Directory Storage and Migration Path

### Current Approaches

**Small/Medium Scale:** For deployments with a relatively modest number of buckets, the directory is stored as HDF5 attributes. This approach leverages HDF5’s lightweight attribute mechanism for rapid lookups and ease of management.

**Large Scale:** When the number of buckets grows (for example, exceeding ~1M buckets) or when the metadata update costs become a bottleneck, a dedicated directory dataset is used. This structure organizes bucket metadata in a tabular (or sharded) format that supports efficient indexing, chunking, and concurrent updates.

### Migration Path

To accommodate the evolution of deployments from small/medium to large scale, the following migration path is designed:

#### Monitoring and Threshold Detection

- **Threshold Parameter:** Define a configurable threshold (e.g., `MIGRATION_THRESHOLD` such as 1M buckets or a defined attribute size limit) that triggers the need for migration.
- **Monitoring:** Continuously monitor the directory’s attribute count and update frequency. When the observed metrics exceed the threshold, a migration is scheduled.

#### Preparation Phase

- **Migration Tool/Process:** Develop a migration tool (or integrate a scheduled maintenance process) that reads bucket metadata from HDF5 attributes and writes it into a newly created, dedicated directory dataset.
- **Schema Definition:** The dedicated dataset is defined using HDF5 compound types, with fields such as bucket ID, metadata pointers (e.g., spill pointers, ECC state mask), and versioning data.
- **Backup and Consistency:** Ensure that a consistent snapshot is taken—either by temporarily locking updates or by using multi-version coordination—to avoid discrepancies during migration.

#### Migration Execution

- **Batch Processing:** Transfer the metadata in manageable batches, updating the new directory dataset incrementally. This process can run as a background maintenance task during off-peak periods to minimize impact on ongoing operations.
- **Validation:** After migration, validate that every bucket’s metadata has been correctly transferred by comparing key integrity metrics between the attribute store and the new dataset.

#### Configuration Update

- **Atomic Switch:** Once the migration is complete and validated, update the system configuration (or central metadata pointer) to indicate that the directory is now served by the dedicated dataset rather than HDF5 attributes.
- **Cache Invalidation:** Invalidate or refresh any caches that reference the old attribute-based directory data to ensure consistency across the system.

#### Cleanup and Deprecation

- **Old Attributes Removal:** Optionally, after a grace period, the old HDF5 attributes can be cleaned up or marked as deprecated to avoid confusion and clear unneeded storage.
- **Documentation and Alerts:** Document the migration process in system logs and provide alerts if any discrepancies are detected. This feedback loop helps fine-tune future migrations as the deployment scales further.

#### Summary

This migration path ensures a smooth transition from an attribute-based directory to a dedicated dataset:

- It is configurable and adapts to thresholds that indicate when attribute overhead becomes prohibitive.
- It uses a controlled, batch-oriented migration process with proper validation to maintain consistency.
- It enables the system to scale seamlessly without interrupting service, while providing a clear rollback/cleanup plan.

**For details on the ECC-protected state mask, see [Spec 5: Multi-Value Keys](spec%205%20-%20Multi-Value%20Keys.md#ecc-protected-state-mask--slot-layout).**

**Sharded Directory:**
- Partition the directory into shards, each as a separate HDF5 group or dataset, with a higher-level directory mapping bucket ranges to shards.
- Reduces update contention and enables parallel access to different directory shards.

**Hybrid Approach:**
- Frequently modified metadata (e.g., recent bucket updates) is stored in a dedicated dataset or sharded structure, while static configuration remains as attributes.

**Implementation Considerations:**
- Indexing: Use HDF5 compound types and chunking to support binary search and rapid lookups.
- Concurrency: Use transactional semantics or external locking for updates to minimize contention and support high concurrency.
- Migration: Start with attributes for simplicity, but provide a migration path to a dedicated/sharded dataset as the number of buckets grows.

**Cost of Resizing:**

| Cost Factor         | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| Localized Impact    | Only overloaded bucket is affected; cost is linear in bucket size           |
| Amortized Perf.     | Splits are rare, so average-case performance is minimally impacted          |
| Pathological Cases  | Repeated splits reduce load; fallback (e.g., in-bucket index) if necessary |

---

## Value-List Compaction

## ValueSet Compaction

For details on value-set compaction (removal of deleted or obsolete values from external value-set datasets), see [Spec 5: Multi-Value Keys](spec%205%20-%20Multi-Value%20Keys.md#value-list-compaction). This process is essential for reclaiming space and maintaining performance in buckets with multi-value keys.

**Note:** Value-set compaction is also relevant to deletion and maintenance operations. For implementation and workflow, refer to the cross-referenced section in the multi-value key spec.