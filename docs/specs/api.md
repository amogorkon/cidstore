# CIDStore Python API

## CIDSem Integration Overview

CIDStore integrates with the CIDSem project to provide optimized storage for semantic triples derived from complex statements. The integration follows CIDSem's ontology format and term grammar for consistent predicate naming and performance-justified specialization.

### CIDSem Term Grammar Support

```python
from cidstore.predicates import PredicateRegistry
from cidstore.keys import E

# Initialize registry with CIDSem validation
registry = PredicateRegistry()

# Register predicates using CIDSem format: kind:namespace:label
friends_store = registry.register_cidsem_multivalue(
    "R:usr:friendsWith",  # Relation, user namespace, friendsWith label
    "Bidirectional social graph queries require specialized indexing"
)

quantity_store = registry.register_cidsem_counter(
    "R:usr:ownedQuantity",
    "Quantity aggregation enables O(1) operations vs O(n) triple enumeration"
)

# CIDSem entities (E:namespace:label format)
alice = E.from_str("E:usr:alice")
bob = E.from_str("E:usr:bob")

# Insert triples using specialized data structures
await friends_store.insert(alice, bob)  # Alice friends with Bob
await quantity_store.insert(alice, 5)   # Alice owns 5 items

# Audit ontology discipline per CIDSem constraints
audit = registry.audit_ontology_discipline()
if audit['violations']:
    print(f"Ontology violations: {audit['violations']}")
```

### Supported CIDSem Term Types

- **Entities**: `E:<namespace>:<label>` (e.g., `E:usr:alice`, `E:geo:berlin`)
- **Relations**: `R:<namespace>:<label>` (e.g., `R:usr:friendsWith`, `R:sys:madeClaimAbout`)
- **Events**: `EV:<namespace>:<label>` (e.g., `EV:usr:postedMessage`)
- **Literals**: `L:<type>:<value>` (e.g., `L:int:42`, `L:str:hello`)
- **Contexts**: `C:<namespace>:<label>` (e.g., `C:sys:CupPlacementRule`)

---

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
- `insert_triple(subject, predicate, object)`: Insert semantic triple with predicate specialization routing.
- `delete(key, value=None)`: Delete a key or a specific value.
- `lookup(key)`: Return all values for a key.
- `query_triple(subject=None, predicate=None, object=None)`: Query triples using SPO/OSP/POS patterns.
- `compact(key)`: Compact a ValueSet for a key.
- `demote_if_possible(key)`: Demote a spilled ValueSet to inline if possible.
- `valueset_exists(key)`: Check if a ValueSet exists for a key.
- `get_tombstone_count(key)`: Get tombstone count for a key.
- `run_gc_once()`: Run background GC (orphan/tombstone cleanup).
- `background_maintenance()`: Run background merge/sort/compaction.
- `migrate_directory()`: Migrate directory from attribute to dataset (Spec 3).
- `get_metrics()`: Return Prometheus/monitoring metrics.
- `log_event(event, **kwargs)`: Log merge/GC/error events.
- `predicate_registry`: PredicateRegistry instance for managing specialized data structures.

---

## `PredicatePluginManager` (`src/cidstore/plugins.py`)

```
class PredicatePluginManager:
    """
    PredicatePluginManager: Manages discovery, loading, and validation of predicate plugins.

    - Discovers plugins via Python entry points
    - Validates plugin implementations and configurations
    - Creates and manages specialized data structure instances
    - Supports hot reload and runtime configuration changes
    """
```

**Key Methods:**
- `discover_plugins()`: Discover all available plugins via entry points
- `load_plugin(plugin_name: str)`: Load and instantiate a plugin by name
- `register_predicate(predicate: E, plugin_name: str, config: Dict)`: Register predicate with plugin
- `get_plugin(plugin_name: str)`: Get loaded plugin instance

---

## `PredicatePlugin` (`src/cidstore/plugins.py`)

```
class PredicatePlugin:
    """
    Base class for all predicate specialization plugins.

    - Defines interface for plugin implementations
    - Handles configuration validation and schema definition
    - Creates specialized data structure instances per predicate
    - Enables extensible predicate specialization without core changes
    """
```

**Key Methods:**
- `plugin_name`: Unique plugin identifier property
- `supported_value_types`: Set of Python types this plugin handles
- `create_instance(predicate: E, config: Dict)`: Create DS instance for predicate
- `validate_config(config: Dict)`: Validate plugin-specific configuration
- `get_default_config()`: Return default configuration parameters
- `get_schema()`: Return JSON schema for configuration validation

---

## `SpecializedDataStructure` (`src/cidstore/predicates.py`)

```
class SpecializedDataStructure:
    """
    Base class for predicate-specialized data structures.

    - All specialized DS implement bidirectional indexing (forward + reverse)
    - Forward index: CID_SP -> values (for SPO queries)
    - Reverse index: value -> Set[CID_SP] (for OSP queries)
    - Composite key CID_SP = hash(Subject || Predicate) for efficient indexing
    """
```

**Key Methods:**
- `insert(subject: E, object: Any)`: Insert (subject, predicate, object) into specialized DS
- `query_spo(subject: E)`: SPO query - get object(s) for given subject
- `query_osp(object: Any)`: OSP query - get all subjects with this object value

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
    E: 256-bit entity identifier for CIDStore keys/values (4 × 64-bit components: high, high_mid, low_mid, low).

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

**Predicate Specialization:**
For semantic/RDF workloads, see [Spec 12: Predicate Specialization](spec%2012%20-%20Predicate%20Specialization.md) for complete coverage of specialized data structures, SPO/OSP/POS query patterns, and performance characteristics.
