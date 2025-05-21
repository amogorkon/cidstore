# CIDStore Python API

## `CIDStore` (`src/cidstore/store.py`)

```
class CIDStore:
    """
    CIDStore: Main entry point for the CIDStore hash directory.

    - Provides insert, delete, lookup, and multi-value key support.
    - Handles directory migration (attributes → dataset) and background maintenance.
    - Exposes metrics/logging for Prometheus/monitoring.
    - All mutating operations are WAL-logged and crash-safe.
    - See Spec 2, 3, 5, 6, 7, 9 for details.
    """
```

**Key Methods:**
- `insert(key, value)`: Insert a value for a key (multi-value supported).
- `delete(key, value=None)`: Delete a key or a specific value.
- `lookup(key)`: Return all values for a key.
- `compact(key)`: Compact a ValueSet for a key.
- `demote_if_possible(key)`: Demote a spilled ValueSet to inline if possible.
- `valueset_exists(key)`: Check if a ValueSet exists for a key.
- `get_tombstone_count(key)`: Get tombstone count for a key.
- `run_gc_once()`: Run background GC (orphan/tombstone cleanup).
- `background_maintenance()`: Run background merge/sort/compaction.
- `migrate_directory()`: Migrate directory from attribute to dataset (Spec 3).
- `get_metrics()`: Return Prometheus/monitoring metrics.
- `log_event(event, **kwargs)`: Log merge/GC/error events.

---

## `ValueSet` (`src/cidstore/valueset.py`)

```
class ValueSet:
    """
    ValueSet: Multi-value key handler for CIDStore buckets.

    - Stores up to MULTI_VALUE_THRESHOLD values inline, then spills to external HDF5 dataset.
    - Uses ECC-protected state mask for inline/spill/tombstone state (Spec 5).
    - Supports promotion, demotion, compaction, and tombstone tracking.
    - All mutating operations are WAL-logged if WAL is provided.
    """
```

**Key Methods:**
- `add(value)`: Add a value (inline or spill).
- `remove(value)`: Remove a value (tombstone if spilled).
- `promote()`: Promote inline values to external ValueSet.
- `demote()`: Demote external ValueSet to inline if possible.
- `compact()`: Remove tombstones from external ValueSet.
- `values()`: Return all current values.
- `is_spilled()`: Return True if using external ValueSet.
- `tombstone_count()`: Return number of tombstones.
- `valueset_exists()`: Return True if external ValueSet exists.

---

## `E` (`src/cidstore/keys.py`)

```
class E(int):
    """
    E: 128-bit entity identifier for CIDStore keys/values.

    - Immutable, hashable, and convertible to/from HDF5.
    - Used for all key/value storage and WAL logging.
    - See Spec 2 for canonical dtype and encoding.
    """
```

**Key Methods/Properties:**
- `from_str(value: str) -> E`: Create E from string (UUID5).
- `high`: High 64 bits.
- `low`: Low 64 bits.
- `to_hdf5()`: Convert to HDF5-compatible array.

---

## `WAL` (`src/cidstore/wal.py`)

```
class WAL:
    """
    WAL: Write-ahead log and recovery logic for CIDStore (Spec 2.2, 3, 6, 7).

    - Canonical HDF5-backed WAL with chained HMAC for integrity.
    - All mutating operations (insert, delete, split, merge, compaction) are logged.
    - Supports replay, audit, and recovery (idempotent, crash-safe).
    - Exposes Prometheus metrics and logging for monitoring.
    """
```

**Key Methods:**
- `append(records)`: Append WAL records (with HMAC).
- `replay(apply_limit=None)`: Replay WAL for recovery.
- `truncate(confirmed_through)`: Truncate WAL after confirmed state.
- `audit_hmac(sample_size=100)`: Audit HMACs for integrity.
- `prometheus_metrics()`: Return Prometheus metrics for monitoring.

---

## `DeletionLog` (`src/cidstore/deletion_log.py`)

```
class DeletionLog:
    """
    DeletionLog: Dedicated deletion log for GC/orphan reclamation (Spec 7).

    - Appends all deletions (key/value/timestamp) for background GC.
    - Backed by HDF5 dataset with canonical dtype.
    - Used by BackgroundGC for safe, idempotent orphan cleanup.
    """
```

**Key Methods:**
- `append(key_high, key_low, value_high, value_low)`: Log a deletion.
- `scan()`: Return all log entries.
- `clear()`: Clear the log.

---

## `BackgroundGC` (`src/cidstore/deletion_log.py`)

```
class BackgroundGC(threading.Thread):
    """
    BackgroundGC: Background thread for garbage collection and orphan reclamation.

    - Periodically scans the deletion log and triggers compaction/cleanup.
    - Runs as a daemon thread; can be stopped via stop().
    """
```

**Key Methods:**
- `run()`: Main loop for background GC.
- `stop_event`: Event to signal thread termination.

---

## CLI Tools (`src/cidstore/cli.py`)

- `migrate <h5file>`: Migrate directory/bucket structure to canonical HDF5 layout.
- `backup <h5file> <backupfile>`: Create a backup of the HDF5 file (with WAL flush).
- `restore <backupfile> <h5file>`: Restore HDF5 file from backup (with WAL replay).

---

For more details, see the docstrings in each module and the `docs/` folder for full specifications.
