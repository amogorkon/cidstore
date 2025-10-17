# 12. Predicate Specialization for Semantic Triple Queries

## 12.1 Overview

Predicate specialization extends CIDStore with efficient support for semantic/RDF triple patterns (Subject, Predicate, Object) by routing predicates to specialized data structures optimized for their specific semantics and query patterns.

| Feature | Description |
|---------|-------------|
| **Query Patterns** | SPO (Subject-Predicate-Object), OSP (Object-Subject-Predicate), POS (Predicate-Object-Subject) |
| **Predicate Scale** | ~200 predicates in our ontology |
| **Routing Mechanism** | Predicate registry + composite key system |
| **Unknown-P Performance** | Parallel fan-out across all predicates (~200 concurrent queries) |
| **Data Structure Types** | Counter, MultiValueSet, SingleValue, TimeSeries, BloomFilter |
| **Index Strategy** | Multi-directional: SPO index + OSP index + POS index per predicate |

## 12.2 Architecture

```mermaid
flowchart TD
    subgraph Main Store
        A[Triple Insert: (Alice, hasApple, 3)] --> B{hasApple registered?}
        B -- Yes --> C[Plugin: CounterStore]
        B -- No --> D[Composite keys: SPO/OSP/POS]
    end

    subgraph Predicate Registry
        E[PredicateRegistry] --> F[hasApple: CounterStore]
        E --> G[foaf:knows: MultiValueSetStore]
        E --> H[createdAt: TimestampStore]
    end

    subgraph Specialized Storage
        F --> I[SPO Index: Subject → int]
        F --> J[OSP Index: int → Set[Subject]]
        G --> K[SPO Index: Subject → Set[Object]]
        G --> L[OSP Index: Object → Set[Subject]]
    end

    subgraph Query Routing
        M[Query with P known] --> N{P in registry?}
        N -- Yes --> O[Route to plugin]
        N -- No --> P[Use composite keys]
        Q[Query with P unknown] --> R[Fan-out to all ~200 predicates]
        R --> S[Aggregate results with concurrency control]
    end
```

## 12.3 Core Components

### 12.3.1 API vs Implementation

**Important:** This specification defines the **API interface** for query patterns (SPO, OSP, POS), not implementation requirements. Each specialized data structure decides which indices to actually implement based on its use case:

| Query Pattern | API Method | Implementation Choice |
|---------------|------------|----------------------|
| **SPO** | `query_spo(subject)` | Always implemented (primary use case) |
| **OSP** | `query_osp(object)` | Optional - implement if reverse lookup needed |
| **POS** | `query_pos(object)` | Optional - usually same as OSP for single predicate |

**Examples:**
- A `CounterStore` for page views might only implement SPO (who cares who has exactly 42 page views?)
- A `MultiValueSetStore` for social connections needs both SPO and OSP (who knows Alice? who does Bob know?)
- A `TimestampStore` might implement SPO + time-range queries but skip OSP entirely

### 12.3.2 Query Routing: Registry Check vs Composite Keys

The system routes queries based on whether the predicate is registered in the plugin registry:

```python
async def route_query(P: E, S: E, O: Optional[E]):
    """Route query to specialized plugin or composite key system."""

    # Check if predicate is specialized
    plugin = predicate_registry.get(P)

    if plugin:
        # Specialized: use plugin's internal indices
        if O is None:
            return await plugin.query_spo(S)
        else:
            # Membership check
            objects = await plugin.query_spo(S)
            return [O] if O in objects else []
    else:
        # Non-specialized: use composite key system
        spo_key = create_composite_key(S, P, r=0)
        result = store.get(spo_key)
        return [result] if result else []
```

The predicate registry itself is the source of truth for specialization.

### 12.3.3 Composite Key Function for Non-Specialized Predicates

Non-specialized predicates use the composite key system. See [Spec 20: Composite Keys](spec20-Composite%20Keys.md) for the full specification.

```python
from cidstore.composite import create_composite_key

# Triple storage using composite keys
# For (S, P, O) triple, create three entries:

# SPO: composite(S, P, r=0) -> O
spo_key = create_composite_key(subject, predicate, r=0)
store[spo_key] = object

# OSP: composite(O, S, r=1) -> P
osp_key = create_composite_key(object, subject, r=1)
store[osp_key] = predicate

# POS: composite(P, O, r=2) -> S
pos_key = create_composite_key(predicate, object, r=2)
store[pos_key] = subject
```

**Properties:**
- Order-sensitive: `create_composite_key(a, b, r) ≠ create_composite_key(b, a, r)`
- Rotation-differentiated: Different `r` values prevent collisions between access patterns
- Space-efficient: 2 CIDs per entry (key+value) instead of 3, saving 33% storage
- Optimal diffusion: Near-theoretical avalanche properties (mean ≈64 bits, stddev ≈5.66)

### 12.3.4 Specialized Data Structure Types

Specialized predicates bypass the composite key system entirely and maintain their own optimized internal indices.

#### CounterStore
```python
class CounterStore(SpecializedDataStructure):
    """Integer counters with atomic increment/decrement."""

    def __init__(self, predicate: E):
        self.predicate = predicate
        # Plugin maintains its own indices - no composite keys needed
        self.spo_index = {}    # Subject → int
        self.osp_index = {}    # int → Set[Subject]
        self.pos_index = {}    # int → Set[Subject] (same as OSP for single predicate)

    async def insert(self, subject: E, count: int):
        """Set counter value with reverse index update."""

    async def incr(self, subject: E, delta: int = 1):
        """Atomic increment operation."""

    async def query_spo(self, subject: E) -> int:
        """SPO: How many X does subject have?"""

    async def query_osp(self, count: int) -> Set[E]:
        """OSP: Which subjects have exactly N items?"""
```

#### MultiValueSetStore
```python
class MultiValueSetStore(SpecializedDataStructure):
    """Deduplicated sets with membership queries."""

    def __init__(self, predicate: E):
        self.predicate = predicate
        # Plugin maintains its own indices - no composite keys needed
        self.spo_index = {}    # Subject → Set[Object]
        self.osp_index = {}    # Object → Set[Subject]
        self.pos_index = {}    # Object → Set[Subject] (same as OSP for single predicate)

    async def insert(self, subject: E, obj: E):
        """Add object to subject's set (deduplication automatic)."""

    async def query_spo(self, subject: E) -> Set[E]:
        """SPO: What objects does subject relate to?"""

    async def query_osp(self, obj: E) -> Set[E]:
        """OSP: Which subjects relate to this object?"""
```

#### TimeSeriesStore
```python
class TimeSeriesStore(SpecializedDataStructure):
    """Time-ordered append-only sequences with range queries."""

    def __init__(self, predicate: E, retention_days: int = 30):
        self.predicate = predicate
        self.retention = retention_days
        # Plugin maintains its own time-indexed storage
        self.spo_index = {}       # Subject → List[(timestamp, Object)]
        self.osp_time_index = {}  # timestamp_bucket → Set[Subject]
        self.osp_obj_index = {}   # Object → List[(timestamp, Subject)]
        self.pos_index = {}       # (timestamp_bucket, Object) → Set[Subject]

    async def append(self, subject: E, obj: E, timestamp: Optional[float] = None):
        """Append time-stamped observation."""

    async def query_spo_range(self, subject: E, start: float, end: float) -> List[Tuple[float, E]]:
        """SPO: Get time-series data for subject in time range."""

    async def query_osp_recent(self, obj: E, hours: int = 24) -> List[Tuple[E, float]]:
        """OSP: Which subjects recently observed this object?"""
```

## 12.4 Unified Query Interface

### 12.4.1 Query API

CIDStore provides a single unified query method that dispatches based on which parameters are bound:

```python
async def query(
    self,
    *,
    P: Optional[E] = None,
    S: Optional[E] = None,
    O: Optional[E] = None
) -> List[Tuple[E, E, E]]:
    """
    Unified triple query interface. Returns complete triples (S, P, O).

    Query patterns by complexity:

    Single-step (direct lookups):
    - (P, S, ?) : O(1) - SPO via predicate plugin
    - (P, ?, O) : O(1) - POS via predicate plugin
    - (P, S, O) : O(1) - membership check

    Two-step (enumerate predicate):
    - (P, ?, ?) : O(n) - enumerate all (S,O) from plugin[P]

    Fan-out (unknown predicate):
    - (?, S, ?) : O(m) - fan-out to all predicates, query SPO each
    - (?, S, O) : O(m) - fan-out to all predicates, check membership each
    - (?, ?, O) : O(m) - fan-out to all predicates with supports_osp

    Note: m ≤ 200 (ontology constraint), with concurrency control

    Rejected:
    - (?, ?, ?) : Raises ValueError (full scan)

    Examples:
        # SPO: What does Alice know?
        results = await store.query(P=knows, S=alice)

        # POS: Who knows kung-fu?
        results = await store.query(P=knows, O=kung_fu)

        # Two-step: What does Alice relate to?
        results = await store.query(S=alice)

        # Fan-out: What relates to kung-fu?
        results = await store.query(O=kung_fu)
    """

    # Validate input
    if P is None and S is None and O is None:
        raise ValueError("At least one parameter must be provided")

    # Dispatch logic
    if P is not None:
        return await self._query_with_predicate(P, S, O)
    elif S is not None:
        return await self._query_with_subject(S, O)
    elif O is not None:
        return await self._query_with_object(O)
    else:
        raise ValueError("Invalid query pattern")
```

### 12.4.2 Single-Step Queries (Predicate Known)

```python
async def _query_with_predicate(self, P: E, S: Optional[E], O: Optional[E]) -> List[Tuple[E, E, E]]:
    """Handle queries where predicate is known."""

    plugin = self.predicate_registry.get(P)
    if plugin is None:
        return []  # Unknown predicate

    if S is not None and O is not None:
        # (P, S, O) - membership check
        objects = await plugin.query_spo(S)
        return [(S, P, O)] if O in objects else []

    elif S is not None:
        # (P, S, ?) - SPO query
        objects = await plugin.query_spo(S)
        return [(S, P, o) for o in objects]

    elif O is not None:
        # (P, ?, O) - POS query
        subjects = await plugin.query_pos(O)
        return [(s, P, O) for s in subjects]

    else:
        # (P, ?, ?) - enumerate all triples with this predicate
        return await plugin.enumerate_all()
```

**Performance:** O(1) for SPO/POS, O(n) for enumerate

### 12.4.3 Fan-Out Queries (Subject Known, Predicate Unknown)

```python
async def _query_with_subject(self, S: E, O: Optional[E]) -> List[Tuple[E, E, E]]:
    """Handle queries where subject is known but predicate is not."""

    # Fan-out to all registered predicates
    async def query_one_predicate(P: E, plugin: SpecializedDataStructure):
        try:
            objects = await plugin.query_spo(S)
            if not objects:
                return []

            if O is not None:
                # (?, S, O) - check if O in results
                return [(S, P, O)] if O in objects else []
            else:
                # (?, S, ?) - return all objects
                return [(S, P, o) for o in objects]
        except Exception:
            return []

    # Execute in parallel with concurrency limit
    semaphore = asyncio.Semaphore(self.config["system"]["max_concurrent_queries"])

    async def limited_query(P: E, plugin: SpecializedDataStructure):
        async with semaphore:
            return await query_one_predicate(P, plugin)

    tasks = [
        limited_query(P, plugin)
        for P, plugin in self.predicate_registry.items()
    ]

    results = await asyncio.gather(*tasks)
    return [triple for sublist in results for triple in sublist]
```

**Performance:** O(m) where m ≤ 200 (ontology constraint), with concurrency control

### 12.4.4 Fan-Out Queries (Object Known, Subject and Predicate Unknown)

```python
async def _query_with_object(self, O: E) -> List[Tuple[E, E, E]]:
    """Handle OSP queries - fan-out across all OSP-capable predicates."""

    # Fan-out to all predicates with OSP support
    async def query_one_predicate(P: E, plugin: SpecializedDataStructure):
        if not plugin.supports_osp:
            return []
        try:
            subjects = await plugin.query_osp(O)
            return [(s, P, O) for s in subjects]
        except Exception:
            return []

    # Execute in parallel with concurrency limit
    semaphore = asyncio.Semaphore(self.config["system"]["max_concurrent_osp"])

    async def limited_query(P: E, plugin: SpecializedDataStructure):
        async with semaphore:
            return await query_one_predicate(P, plugin)

    tasks = [
        limited_query(P, plugin)
        for P, plugin in self.predicate_registry.items()
    ]

    results = await asyncio.gather(*tasks)
    return [triple for sublist in results for triple in sublist]
```

**Performance:** O(m) where m = number of OSP-capable predicates (typically ~200)
**Note:** This is the most expensive query pattern and uses concurrency control

## 12.5 Storage and Persistence

### 12.5.1 In-Memory Prototype

Current implementation maintains specialized data structures in memory:

```python
class CounterStore(SpecializedDataStructure):
    def __init__(self, predicate: E):
        # All indices in memory (dictionaries)
        self.forward: Dict[E, int] = {}
        self.reverse: Dict[int, Set[E]] = {}
        self.cid_sp_to_subject: Dict[E, E] = {}
```

**Limitations:**
- No persistence across restarts
- No WAL integration for crash recovery
- Memory usage grows with dataset size

### 12.5.2 HDF5-Backed Storage (Future)

Production implementation should persist specialized DS in HDF5:

```plaintext
root/
  ├── predicates/                          # Predicate specialization root
  │   ├── registry: Dataset                # Predicate → DS type mapping
  │   ├── counters/                        # Counter-type predicates
  │   │   ├── hasApple_forward: Dataset    # CID_SP → int
  │   │   ├── hasApple_reverse: Dataset    # int → CID_SP[]
  │   │   └── hasApple_subjects: Dataset   # CID_SP → Subject
  │   ├── multivalue_sets/                 # Set-type predicates
  │   │   ├── foaf_knows_forward: Dataset  # CID_SP → E[]
  │   │   ├── foaf_knows_reverse: Dataset  # E → CID_SP[]
  │   │   └── foaf_knows_subjects: Dataset
  │   └── timeseries/                      # Time-series predicates
  │       ├── observations_data: Dataset   # (CID_SP, timestamp, E)
  │       └── observations_index: Dataset  # Time-based indices
```

### 12.5.3 WAL Integration (Future)

Specialized DS operations must be WAL-logged for atomicity:

```python
# WAL record for specialized DS operations
class PredicateWALRecord:
    predicate: E           # Which predicate DS
    operation: str         # "insert", "delete", "incr"
    subject: E            # Subject CID
    object: Any           # Object value (int, E, etc.)
    timestamp: float      # Operation time

# Atomic dual-write pattern
async def insert_triple(self, subject: E, predicate: E, obj: Any):
    # 1. Log to WAL
    await self.wal.log_predicate_op("insert", subject, predicate, obj)

    # 2. Update specialized DS
    ds = self.predicate_registry.get(predicate)
    await ds.insert(subject, obj)
```

## 12.6 Performance Characteristics

| Query Pattern | Complexity | Scalability | Notes |
|---------------|------------|-------------|-------|
| **SPO (specialized)** | O(1) | Constant | Direct SPO index lookup |
| **SPO (generic)** | O(1) | Constant | Main store hash lookup |
| **OSP (single predicate)** | O(1) | Constant | Direct OSP index lookup |
| **OSP (all predicates)** | O(P) parallel | Linear in predicate count | P ≈ 200, excellent parallelism |
| **POS** | O(1) | Constant | Direct POS index lookup |

### 12.6.1 Memory Usage

**Per Predicate DS (CounterStore example):**
- SPO index: 16B (CID_SP) + 4B (int) = 20B per entry
- OSP index: 4B (int) + 16B (CID_SP) = 20B per entry
- POS index: 4B (int) + 16B (Subject) = 20B per entry
- Subject mapping: 16B (CID_SP) + 16B (Subject) = 32B per entry
- **Total:** ~92B per (Subject, Predicate, Object) triple**Comparison with Generic Multivalue:**
- Generic: 16B (Subject) + 16B (Object) = 32B per triple
- Specialized: 92B per triple (+188% overhead)
- **Trade-off:** 2.9x memory cost for ~100x OSP/POS query speedup

### 12.6.2 Scalability Limits

| Component | Limit | Mitigation |
|-----------|-------|------------|
| **Predicate Count** | ~200 predicates | OSP fan-out scales linearly; use predicate filtering |
| **Memory Usage** | 36-76B per triple (implementation-dependent) | Choose indices based on query patterns; use compression |
| **OSP Concurrency** | 200 parallel tasks | Use semaphore to limit concurrent DS queries |

## 12.7 Use Cases and Examples

### 12.7.1 Knowledge Graph Queries

```python
# Setup: Register specialized predicates using CIDSem format
store = CIDStoreWithPlugins(hdf, wal, "cidsem_predicates.yaml")

# CIDSem entities and relations
alice = E.from_str("E:usr:alice")
bob = E.from_str("E:usr:bob")
charlie = E.from_str("E:usr:charlie")
apple_quantity = E.from_str("R:usr:ownedQuantity")
friends_with = E.from_str("R:usr:friendsWith")

# Data insertion using CIDSem term grammar
await store.insert_triple(alice, apple_quantity, 5)    # Alice owns 5 (apples)
await store.insert_triple(bob, apple_quantity, 3)      # Bob owns 3 (apples)
await store.insert_triple(alice, friends_with, bob)    # Alice is friends with Bob
await store.insert_triple(charlie, friends_with, bob)  # Charlie is friends with Bob

# SPO queries (Subject-Predicate-Object)
apple_count = await store.query_triple(alice, hasApple)  # → 5
friends = await store.query_triple(alice, foaf_knows)    # → {bob}

# OSP queries (Object-Subject-Predicate)
who_has_3_apples = await store.query_triple(None, hasApple, 3)      # → [(bob, hasApple)]
who_knows_bob = await store.query_triple(None, foaf_knows, bob)     # → [(alice, foaf_knows), (charlie, foaf_knows)]

# Cross-predicate OSP (fan-out to all predicates)
all_relations_with_bob = await store.query_triple(None, None, bob)  # → [(alice, foaf_knows), (charlie, foaf_knows)]
```

### 12.7.2 Analytics and Aggregation

```python
# Counter-based analytics
page_views = E.from_str("pageViews")
store.predicate_registry.register_counter(page_views)

# Increment page view counts
await store.predicate_registry.get(page_views).incr(page1, 1)
await store.predicate_registry.get(page_views).incr(page1, 5)
await store.predicate_registry.get(page_views).incr(page2, 3)

# Analytics queries
page1_views = await store.query_triple(page1, page_views)           # → 6
pages_with_3_views = await store.query_triple(None, page_views, 3)  # → [(page2, pageViews)]

# Time-series analytics (future)
observations = E.from_str("observations")
store.predicate_registry.register_timeseries(observations)

await store.predicate_registry.get(observations).append(sensor1, temperature_reading, timestamp)
recent_readings = await store.predicate_registry.get(observations).query_spo_range(
    sensor1, start_time, end_time
)
```

## 12.8 Migration and Deployment

### 12.8.1 Incremental Adoption

```python
# Phase 1: Add predicate registry (no data migration)
store = CIDStore(hdf, wal)
# Existing data remains in generic multivalue storage

# Phase 2: Register high-value predicates
critical_predicates = ["hasApple", "foaf:knows", "pageViews"]
for pred_name in critical_predicates:
    pred_cid = E.from_str(pred_name)
    if pred_name == "pageViews":
        store.predicate_registry.register_counter(pred_cid)
    else:
        store.predicate_registry.register_multivalue(pred_cid)

# Phase 3: New data automatically routes to specialized DS
# Old data remains accessible via generic paths

# Phase 4: Background migration (optional)
await migrate_existing_triples_to_specialized_storage()
```

### 12.8.2 Rollback Strategy

```python
# Disable predicate specialization (emergency rollback)
store.predicate_registry = PredicateRegistry()  # Empty registry

# All queries fall back to composite key system
# System continues operating with reduced performance for specialized predicates
```

## 12.9 Future Extensions

### 12.9.1 Additional Data Structure Types

```python
class BloomFilterStore(SpecializedDataStructure):
    """Probabilistic membership queries for large sets."""

class CompressedBitmapStore(SpecializedDataStructure):
    """Compressed bitmaps for categorical/enumerated values."""

class GeospatialStore(SpecializedDataStructure):
    """R-tree or similar for spatial predicates."""

class FullTextStore(SpecializedDataStructure):
    """Inverted index for text search predicates."""
```

### 12.9.2 Query Optimization

```python
# Predicate selectivity statistics
class PredicateStats:
    cardinality: int      # Number of distinct objects
    frequency: int        # Number of triples with this predicate
    selectivity: float    # Average objects per subject

# Query planner: choose optimal execution strategy
def optimize_query(pattern: TriplePattern, stats: Dict[E, PredicateStats]):
    if pattern.is_spo():
        return DirectLookupPlan(pattern.subject, pattern.predicate)
    elif pattern.is_osp() and stats[pattern.predicate].selectivity < 0.1:
        return SinglePredicatePlan(pattern.predicate, pattern.object)
    else:
        return ParallelFanOutPlan(pattern.object)
```

### 12.9.3 Distributed Scaling

```python
# Predicate sharding for >200 predicates
class DistributedPredicateRegistry:
    shards: List[PredicateRegistry]  # Shard by predicate hash

    async def query_osp_distributed(self, obj: Any) -> List[Tuple[E, E]]:
        # Fan-out across N shards instead of 200 predicates
        tasks = [shard.query_osp(obj) for shard in self.shards]
        return await asyncio.gather(*tasks)
```

**Note:** This specification focuses on query patterns and API interfaces. For the plugin architecture that enables dynamic registration and loading of specialized data structures, see [Spec 13: Plugin Infrastructure](spec%2013%20-%20Plugin%20Infrastructure.md).

## 12.10 Summary

Predicate specialization enables CIDStore to efficiently support semantic triple queries through:

1. **Predicate Registry Routing:** In-memory check determines if predicate is specialized
2. **Composite Key System:** Deterministic subject+predicate hashing for non-specialized storage
3. **Multi-directional Indices:** SPO, OSP, and POS indices per predicate DS
4. **Parallel Fan-Out Queries:** Concurrent queries across ~200 predicates for unknown-predicate patterns
5. **Type-Specific Optimization:** Counters, sets, timestamps, and other domain-specific structures

**Performance Benefits:**
- SPO queries: O(1) direct lookup (same as baseline)
- OSP queries: O(200) parallel vs O(N) sequential scan (100x+ speedup for large datasets)
- Memory efficiency: 1.1-2.4x overhead (implementation-dependent), unlimited speedup for supported query patterns

**Deployment Strategy:**
- Incremental adoption with backward compatibility
- Zero-downtime migration from generic to specialized storage
- Emergency rollback via registry reset

This architecture positions CIDStore as a high-performance backend for knowledge graphs, semantic databases, and RDF stores requiring efficient SPO/OSP/POS query patterns.