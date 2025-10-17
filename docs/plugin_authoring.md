# Plugin Authoring Guide

This guide explains how to create custom specialized data structures (plugins) for CIDStore predicates, enabling efficient storage and querying patterns tailored to your specific use cases.

## Table of Contents
- [Overview](#overview)
- [SpecializedDataStructure Interface](#specializeddatastructure-interface)
- [Index Requirements](#index-requirements)
- [Built-in Examples](#built-in-examples)
- [Creating a Custom Plugin](#creating-a-custom-plugin)
- [Performance Tuning](#performance-tuning)
- [Testing Best Practices](#testing-best-practices)
- [Hot-Reload and Migration](#hot-reload-and-migration)

---

## Overview

CIDStore uses a **predicate registry** system that allows you to register specialized data structures for specific predicates. This enables:

- **Optimized storage**: Store data in formats optimized for specific access patterns
- **Custom semantics**: Implement counter semantics, set semantics, or custom logic
- **Efficient queries**: Support SPO (Subject-Predicate-Object), OSP (Object-Subject-Predicate), and POS (Predicate-Object-Subject) queries
- **Runtime flexibility**: Hot-reload plugins with data migration support

### Why Use Plugins?

Standard triple stores treat all predicates uniformly, but real-world data has structure:
- **Counters**: `(user, :score, 100)` - A single numeric value per subject
- **Sets**: `(user, :tags, {tag1, tag2, tag3})` - Multiple unique values per subject
- **Custom**: Your domain-specific semantics

Plugins let you encode these semantics directly, improving both performance and correctness.

---

## SpecializedDataStructure Interface

All plugins must inherit from `SpecializedDataStructure` and implement required methods.

### Base Class

```python
from cidstore.predicates import SpecializedDataStructure
from cidstore.keys import E
from typing import Any, Set, Optional

class SpecializedDataStructure:
    """Base class for specialized predicate data structures."""
    
    async def insert(self, subject: E, value: Any) -> None:
        """Insert a triple (subject, predicate, value).
        
        Args:
            subject: The subject entity
            value: The object value (type depends on plugin)
        
        Raises:
            ValueError: If value violates plugin semantics
        """
        raise NotImplementedError
    
    async def query_osp(self, obj: Any) -> Set[E]:
        """Query for subjects matching (?, predicate, obj).
        
        Args:
            obj: The object to search for
        
        Returns:
            Set of subject entities matching the query
        """
        raise NotImplementedError
    
    async def query_pos(self, predicate: E) -> Set[tuple[E, Any]]:
        """Query for all (subject, object) pairs for this predicate.
        
        Args:
            predicate: The predicate to query (usually self.predicate)
        
        Returns:
            Set of (subject, object) tuples
        """
        raise NotImplementedError
    
    async def delete(self, subject: E, obj: Optional[Any] = None) -> int:
        """Delete triples matching (subject, predicate, obj).
        
        Args:
            subject: The subject entity
            obj: Optional object value (None = delete all for subject)
        
        Returns:
            Number of triples deleted
        """
        raise NotImplementedError
```

### Required Attributes

Your plugin should maintain these internal indices:

```python
class MyPlugin(SpecializedDataStructure):
    def __init__(self, predicate: E, store):
        self.predicate = predicate
        self.store = store
        
        # Required indices
        self.spo_index: Dict[E, ValueType] = {}  # Subject → Object(s)
        self.osp_index: Dict[ValueType, Set[E]] = defaultdict(set)  # Object → Subjects
        self.pos_index: Set[tuple[E, ValueType]] = set()  # All (S, O) pairs
```

---

## Index Requirements

### SPO Index (Subject-Predicate-Object)
- **Purpose**: Fast lookup by subject: "What are the objects for this subject?"
- **Type**: `Dict[E, ValueType]`
- **Example**: `{user1: 100, user2: 200}` (CounterStore) or `{user1: {tag1, tag2}}` (MultiValueSetStore)

### OSP Index (Object-Subject-Predicate)
- **Purpose**: Reverse lookup: "Which subjects have this object?"
- **Type**: `Dict[ValueType, Set[E]]`
- **Example**: `{100: {user1, user3}, 200: {user2}}` (CounterStore) or `{tag1: {user1, user5}}` (MultiValueSetStore)

### POS Index (Predicate-Object-Subject)
- **Purpose**: List all triples for this predicate
- **Type**: `Set[tuple[E, ValueType]]`
- **Example**: `{(user1, 100), (user2, 200)}` (CounterStore) or `{(user1, tag1), (user1, tag2)}` (MultiValueSetStore)

### Index Consistency Rules

1. **Insert**: Update all three indices atomically
2. **Delete**: Remove from all three indices atomically
3. **Audit**: Implement `audit_indices()` to verify consistency:
   - SPO ↔ POS correspondence
   - OSP ↔ POS correspondence
   - No orphaned entries

---

## Built-in Examples

### CounterStore: Integer Values

**Use case**: Single numeric value per subject (scores, counts, timestamps)

```python
class CounterStore(SpecializedDataStructure):
    """Stores single integer values per subject."""
    
    def __init__(self, predicate: E, store):
        self.predicate = predicate
        self.store = store
        self.spo_index: Dict[E, int] = {}
        self.osp_index: Dict[int, Set[E]] = defaultdict(set)
        self.pos_index: Set[tuple[E, int]] = set()
    
    async def insert(self, subject: E, value: int) -> None:
        """Insert/update a counter value."""
        # Remove old value if exists
        if subject in self.spo_index:
            old_value = self.spo_index[subject]
            self.osp_index[old_value].discard(subject)
            self.pos_index.discard((subject, old_value))
        
        # Insert new value
        self.spo_index[subject] = value
        self.osp_index[value].add(subject)
        self.pos_index.add((subject, value))
    
    async def query_osp(self, obj: int) -> Set[E]:
        """Find all subjects with this counter value."""
        return self.osp_index.get(obj, set()).copy()
    
    async def query_pos(self, predicate: E) -> Set[tuple[E, int]]:
        """Get all (subject, value) pairs."""
        return self.pos_index.copy()
    
    async def delete(self, subject: E, obj: Optional[int] = None) -> int:
        """Delete counter for subject."""
        if subject not in self.spo_index:
            return 0
        
        old_value = self.spo_index[subject]
        
        # If obj specified, must match
        if obj is not None and old_value != obj:
            return 0
        
        # Remove from all indices
        del self.spo_index[subject]
        self.osp_index[old_value].discard(subject)
        self.pos_index.discard((subject, old_value))
        
        return 1
```

**Key characteristics**:
- One value per subject (overwrites on insert)
- Efficient counter queries: "Find all users with score=100"
- Memory efficient: Direct `subject → int` mapping

### MultiValueSetStore: Multiple Values

**Use case**: Multiple unique values per subject (tags, categories, relationships)

```python
class MultiValueSetStore(SpecializedDataStructure):
    """Stores sets of E values per subject."""
    
    def __init__(self, predicate: E, store):
        self.predicate = predicate
        self.store = store
        self.spo_index: Dict[E, Set[E]] = defaultdict(set)
        self.osp_index: Dict[E, Set[E]] = defaultdict(set)
        self.pos_index: Set[tuple[E, E]] = set()
    
    async def insert(self, subject: E, value: E) -> None:
        """Add a value to the subject's set."""
        if not isinstance(value, E):
            raise ValueError(f"MultiValueSetStore requires E values, got {type(value)}")
        
        # Add to indices (idempotent - set semantics)
        self.spo_index[subject].add(value)
        self.osp_index[value].add(subject)
        self.pos_index.add((subject, value))
    
    async def query_osp(self, obj: E) -> Set[E]:
        """Find all subjects containing this value."""
        return self.osp_index.get(obj, set()).copy()
    
    async def query_pos(self, predicate: E) -> Set[tuple[E, E]]:
        """Get all (subject, value) pairs."""
        return self.pos_index.copy()
    
    async def delete(self, subject: E, obj: Optional[E] = None) -> int:
        """Delete value(s) from subject's set."""
        if subject not in self.spo_index:
            return 0
        
        if obj is None:
            # Delete all values for subject
            values = self.spo_index[subject].copy()
            for val in values:
                self.osp_index[val].discard(subject)
                self.pos_index.discard((subject, val))
            del self.spo_index[subject]
            return len(values)
        else:
            # Delete specific value
            if obj not in self.spo_index[subject]:
                return 0
            
            self.spo_index[subject].discard(obj)
            if not self.spo_index[subject]:
                del self.spo_index[subject]
            
            self.osp_index[obj].discard(subject)
            self.pos_index.discard((subject, obj))
            
            return 1
```

**Key characteristics**:
- Multiple values per subject (set semantics)
- Idempotent inserts (no duplicates)
- Efficient membership queries: "Find all users tagged 'python'"

---

## Creating a Custom Plugin

### Example: TimestampStore with Range Queries

Let's create a plugin optimized for timestamp ranges.

```python
from cidstore.predicates import SpecializedDataStructure
from cidstore.keys import E
from typing import Optional, Set
from collections import defaultdict
from sortedcontainers import SortedDict  # pip install sortedcontainers

class TimestampStore(SpecializedDataStructure):
    """Stores timestamps with efficient range queries."""
    
    def __init__(self, predicate: E, store):
        self.predicate = predicate
        self.store = store
        
        # Standard indices
        self.spo_index: dict[E, int] = {}  # Subject → timestamp
        self.osp_index: dict[int, Set[E]] = defaultdict(set)  # Timestamp → subjects
        self.pos_index: Set[tuple[E, int]] = set()
        
        # Sorted index for range queries
        self.sorted_timestamps = SortedDict()  # Timestamp → Set[E]
    
    async def insert(self, subject: E, timestamp: int) -> None:
        """Insert a timestamp for a subject."""
        if not isinstance(timestamp, int):
            raise ValueError(f"Timestamp must be int, got {type(timestamp)}")
        
        # Remove old timestamp if exists
        if subject in self.spo_index:
            old_ts = self.spo_index[subject]
            self.osp_index[old_ts].discard(subject)
            self.pos_index.discard((subject, old_ts))
            
            # Remove from sorted index
            self.sorted_timestamps[old_ts].discard(subject)
            if not self.sorted_timestamps[old_ts]:
                del self.sorted_timestamps[old_ts]
        
        # Insert new timestamp
        self.spo_index[subject] = timestamp
        self.osp_index[timestamp].add(subject)
        self.pos_index.add((subject, timestamp))
        
        # Add to sorted index
        if timestamp not in self.sorted_timestamps:
            self.sorted_timestamps[timestamp] = set()
        self.sorted_timestamps[timestamp].add(subject)
    
    async def query_osp(self, timestamp: int) -> Set[E]:
        """Find subjects with exact timestamp."""
        return self.osp_index.get(timestamp, set()).copy()
    
    async def query_range(self, start: int, end: int) -> Set[E]:
        """Find subjects with timestamps in [start, end]."""
        results = set()
        for ts in self.sorted_timestamps.irange(start, end):
            results.update(self.sorted_timestamps[ts])
        return results
    
    async def query_pos(self, predicate: E) -> Set[tuple[E, int]]:
        """Get all (subject, timestamp) pairs."""
        return self.pos_index.copy()
    
    async def delete(self, subject: E, timestamp: Optional[int] = None) -> int:
        """Delete timestamp for subject."""
        if subject not in self.spo_index:
            return 0
        
        old_ts = self.spo_index[subject]
        
        # If timestamp specified, must match
        if timestamp is not None and old_ts != timestamp:
            return 0
        
        # Remove from all indices
        del self.spo_index[subject]
        self.osp_index[old_ts].discard(subject)
        self.pos_index.discard((subject, old_ts))
        
        # Remove from sorted index
        self.sorted_timestamps[old_ts].discard(subject)
        if not self.sorted_timestamps[old_ts]:
            del self.sorted_timestamps[old_ts]
        
        return 1
    
    async def audit_indices(self) -> dict:
        """Verify index consistency."""
        errors = []
        
        # Verify SPO ↔ POS
        spo_pairs = {(s, v) for s, v in self.spo_index.items()}
        if spo_pairs != self.pos_index:
            errors.append("SPO/POS mismatch")
        
        # Verify OSP ↔ POS
        osp_pairs = {(s, ts) for ts, subjects in self.osp_index.items() for s in subjects}
        if osp_pairs != self.pos_index:
            errors.append("OSP/POS mismatch")
        
        # Verify sorted index
        sorted_pairs = {(s, ts) for ts, subjects in self.sorted_timestamps.items() for s in subjects}
        if sorted_pairs != self.pos_index:
            errors.append("Sorted/POS mismatch")
        
        return {
            'consistent': len(errors) == 0,
            'errors': errors,
            'triple_count': len(self.pos_index)
        }
```

### Registering Your Plugin

```python
from cidstore.store import CIDStore
from cidstore.keys import E

# Initialize store
store = await CIDStore.create(path=":memory:", testing=True)

# Register your custom plugin
created_at_pred = E.from_str("R:user:created_at")
store.predicate_registry.register(created_at_pred, TimestampStore)

# Use it
user = E.from_str("E:user:alice")
await store.insert_triple(user, created_at_pred, 1672531200)

# Query
subjects_in_range = await timestamp_store.query_range(1672531200, 1672617600)
```

---

## Performance Tuning

### Memory vs Query Speed Tradeoffs

| Index Type | Memory Cost | Query Speed | When to Use |
|------------|-------------|-------------|-------------|
| SPO only | Low | Slow OSP | Write-heavy, no reverse queries |
| SPO + OSP | Medium | Fast both | Balanced read/write |
| SPO + OSP + POS | High | Fast all | Read-heavy, need full scans |
| Custom (sorted, etc.) | Highest | Domain-specific | Specialized queries |

### Optimization Patterns

#### 1. Lazy Index Creation

```python
class LazyOSPStore(SpecializedDataStructure):
    def __init__(self, predicate, store):
        self.spo_index = {}
        self.osp_index = None  # Build on first query
        self._osp_dirty = True
    
    async def insert(self, subject, value):
        self.spo_index[subject] = value
        self._osp_dirty = True
    
    async def query_osp(self, obj):
        if self._osp_dirty:
            self._rebuild_osp()
        return self.osp_index.get(obj, set())
    
    def _rebuild_osp(self):
        self.osp_index = defaultdict(set)
        for s, v in self.spo_index.items():
            self.osp_index[v].add(s)
        self._osp_dirty = False
```

#### 2. Batch Operations

```python
async def insert_batch(self, items: list[tuple[E, Any]]) -> None:
    """Insert multiple triples efficiently."""
    for subject, value in items:
        # Update indices without awaiting each
        self.spo_index[subject] = value
        self.osp_index[value].add(subject)
        self.pos_index.add((subject, value))
    
    # Single WAL write for entire batch
    if self.store._wal:
        await self.store._wal.write_batch([
            (subject, self.predicate, value) for subject, value in items
        ])
```

#### 3. Compression for Large Sets

```python
from roaring import RoaringBitmap  # pip install pyroaring

class CompressedMultiValueStore(SpecializedDataStructure):
    """Uses RoaringBitmaps for memory-efficient large sets."""
    
    def __init__(self, predicate, store):
        self.predicate = predicate
        self.store = store
        
        # Map E → int for bitmap storage
        self.e_to_int = {}
        self.int_to_e = {}
        self.next_id = 0
        
        # Compressed indices
        self.spo_index: dict[E, RoaringBitmap] = {}
        self.osp_index: dict[E, RoaringBitmap] = {}
    
    def _get_id(self, e: E) -> int:
        if e not in self.e_to_int:
            self.e_to_int[e] = self.next_id
            self.int_to_e[self.next_id] = e
            self.next_id += 1
        return self.e_to_int[e]
    
    async def insert(self, subject: E, value: E) -> None:
        s_id = self._get_id(subject)
        v_id = self._get_id(value)
        
        if subject not in self.spo_index:
            self.spo_index[subject] = RoaringBitmap()
        self.spo_index[subject].add(v_id)
        
        if value not in self.osp_index:
            self.osp_index[value] = RoaringBitmap()
        self.osp_index[value].add(s_id)
```

---

## Testing Best Practices

### Unit Test Structure

```python
import pytest
from cidstore.store import CIDStore
from cidstore.keys import E

@pytest.fixture
async def store():
    """Create in-memory test store."""
    s = await CIDStore.create(path=":memory:", testing=True)
    yield s
    await s.close()

async def test_my_plugin_insert(store):
    """Test basic insert operation."""
    pred = E.from_str("R:test:pred")
    store.predicate_registry.register(pred, MyCustomPlugin)
    
    subject = E.from_str("E:test:subject")
    value = 42
    
    await store.insert_triple(subject, pred, value)
    result = await store.get_triple(subject, pred)
    
    assert result == value

async def test_my_plugin_osp_query(store):
    """Test reverse (OSP) queries."""
    pred = E.from_str("R:test:pred")
    store.predicate_registry.register(pred, MyCustomPlugin)
    
    # Insert test data
    s1 = E.from_str("E:test:subject1")
    s2 = E.from_str("E:test:subject2")
    value = 42
    
    await store.insert_triple(s1, pred, value)
    await store.insert_triple(s2, pred, value)
    
    # Query
    plugin = store.predicate_registry.get(pred)
    subjects = await plugin.query_osp(value)
    
    assert s1 in subjects
    assert s2 in subjects

async def test_my_plugin_delete(store):
    """Test deletion."""
    pred = E.from_str("R:test:pred")
    store.predicate_registry.register(pred, MyCustomPlugin)
    
    subject = E.from_str("E:test:subject")
    value = 42
    
    await store.insert_triple(subject, pred, value)
    
    plugin = store.predicate_registry.get(pred)
    deleted = await plugin.delete(subject, value)
    
    assert deleted == 1
    assert subject not in plugin.spo_index

async def test_my_plugin_audit_indices(store):
    """Test index consistency after operations."""
    pred = E.from_str("R:test:pred")
    store.predicate_registry.register(pred, MyCustomPlugin)
    
    # Insert, update, delete operations
    subject = E.from_str("E:test:subject")
    await store.insert_triple(subject, pred, 1)
    await store.insert_triple(subject, pred, 2)  # Update
    
    plugin = store.predicate_registry.get(pred)
    result = await plugin.audit_indices()
    
    assert result['consistent'] is True
    assert len(result['errors']) == 0
```

### Edge Cases to Test

1. **Empty plugin**: Operations on newly created plugin
2. **Duplicate inserts**: Idempotency (sets) vs overwrites (counters)
3. **Delete non-existent**: Should not raise errors
4. **Large batch inserts**: Performance and memory
5. **Concurrent access**: Thread safety (if applicable)
6. **Index consistency**: After every mutation
7. **Type validation**: Invalid value types
8. **Hot-reload**: Data migration correctness

---

## Hot-Reload and Migration

### Overview

CIDStore supports **runtime plugin changes** with automatic data migration. This enables schema evolution without downtime.

### Use Cases

- **Adding indices**: Upgrade from simple storage to indexed queries
- **Changing semantics**: Counter → Set or Set → Counter
- **Performance optimization**: Swap implementations without data loss

### Migration API

```python
# Hot-reload a predicate's plugin type
result = await store.predicate_registry.hot_reload_predicate(
    predicate=score_pred,
    new_plugin_type='multivalue',  # 'counter' or 'multivalue'
    migrate_data=True  # Set to False to discard old data
)

print(result)
# {
#     'success': True,
#     'old_type': 'CounterStore',
#     'new_type': 'MultiValueSetStore',
#     'migration': {'migrated': 1000, 'skipped': 0}
# }
```

### Migration Strategies

| From | To | Strategy | Data Transformation |
|------|----|---------|--------------------|
| Counter | Counter | Direct copy | `subject → value` (unchanged) |
| MultiValue | MultiValue | Direct copy | `subject → {values}` (unchanged) |
| Counter | MultiValue | Synthetic E | `subject → {E:migration:count_{value}}` |
| MultiValue | Counter | Set size | `subject → len({values})` |

### Example: Counter to MultiValue

```python
# Original: (user, :score, 100)
score_pred = E.from_str("R:user:score")
store.predicate_registry.register(score_pred, CounterStore)

user = E.from_str("E:user:alice")
await store.insert_triple(user, score_pred, 100)

# Hot-reload to MultiValue
result = await store.predicate_registry.hot_reload_predicate(
    score_pred, 'multivalue', migrate_data=True
)

# After: (user, :score, {E:migration:count_100})
values = await store.get_triple(user, score_pred)
print(values)  # {E('㐯䨶㧈隞䐊瓁鉝哬犊')}  # This is E.from_str("E:migration:count_100")
```

### Example: MultiValue to Counter

```python
# Original: (user, :tags, {tag1, tag2, tag3})
tags_pred = E.from_str("R:user:tags")
store.predicate_registry.register(tags_pred, MultiValueSetStore)

user = E.from_str("E:user:bob")
await store.insert_triple(user, tags_pred, E.from_str("E:tag:python"))
await store.insert_triple(user, tags_pred, E.from_str("E:tag:rust"))
await store.insert_triple(user, tags_pred, E.from_str("E:tag:go"))

# Hot-reload to Counter
result = await store.predicate_registry.hot_reload_predicate(
    tags_pred, 'counter', migrate_data=True
)

# After: (user, :tags, 3)  # Count of tags
count = await store.get_triple(user, tags_pred)
print(count)  # 3
```

### Custom Migration Logic

To support hot-reload in your custom plugin, implement these methods:

```python
class MyCustomPlugin(SpecializedDataStructure):
    @classmethod
    async def migrate_from(cls, old_plugin: SpecializedDataStructure, predicate: E, store):
        """Migrate data from another plugin type."""
        new_plugin = cls(predicate, store)
        
        # Access old plugin's data
        for subject, value in old_plugin.spo_index.items():
            # Transform value to new plugin's format
            transformed_value = cls._transform_value(value)
            await new_plugin.insert(subject, transformed_value)
        
        return new_plugin
    
    @staticmethod
    def _transform_value(value: Any) -> Any:
        """Transform value from old format to new format."""
        # Your conversion logic here
        return value
```

### Migration Best Practices

1. **Test migrations**: Write tests for every migration path
2. **Backup before reload**: Hot-reload is irreversible
3. **Validate after**: Run `audit_indices()` after migration
4. **Document trade-offs**: Lossy conversions (Set→Count) lose information
5. **Monitor performance**: Large datasets may take time to migrate

---

## Advanced Topics

### Transaction Support

Plugins inherit transaction support from CIDStore's transaction manager:

```python
async def transactional_operations(store):
    await store.begin_transaction()
    try:
        await store.insert_triple(s1, pred, value1)
        await store.insert_triple(s2, pred, value2)
        await store.commit()
    except Exception:
        await store.rollback()
```

### Background Maintenance

Plugins can participate in background maintenance:

```python
class MyPlugin(SpecializedDataStructure):
    async def maintenance_task(self):
        """Optional: Run periodic maintenance."""
        # Compact indices, rebuild caches, etc.
        pass
```

### Performance Monitoring

Track plugin performance metrics:

```python
class InstrumentedPlugin(SpecializedDataStructure):
    def __init__(self, predicate, store):
        super().__init__(predicate, store)
        self.metrics = {
            'inserts': 0,
            'queries': 0,
            'deletes': 0
        }
    
    async def insert(self, subject, value):
        start = time.time()
        await super().insert(subject, value)
        self.metrics['inserts'] += 1
        self.metrics['insert_time'] = time.time() - start
```

---

## Summary

**Creating a custom plugin checklist**:

- [ ] Inherit from `SpecializedDataStructure`
- [ ] Implement required methods: `insert`, `query_osp`, `query_pos`, `delete`
- [ ] Maintain SPO, OSP, POS indices
- [ ] Implement `audit_indices()` for consistency checks
- [ ] Write comprehensive unit tests
- [ ] Document semantics and limitations
- [ ] (Optional) Implement migration logic for hot-reload

**Key takeaways**:

1. Plugins optimize storage and queries for specific predicates
2. All plugins must maintain SPO, OSP, POS indices for consistency
3. Built-in examples (CounterStore, MultiValueSetStore) cover most use cases
4. Hot-reload enables runtime schema evolution with data migration
5. Testing index consistency is critical for correctness

**Resources**:

- Source code: `src/cidstore/predicates.py`
- Built-in plugins: `CounterStore`, `MultiValueSetStore`
- Tests: `tests/test_predicate_registry_enhanced.py`, `tests/test_hot_reload.py`
- Transaction support: `tests/test_transactions.py`

For questions or issues, see the main README or open an issue on GitHub.
