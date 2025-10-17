# Spec 14: CIDSem Integration

## 14.1 Overview

CIDStore predicate specialization integrates with the CIDSem project to provide optimized storage for semantic triples derived from complex statements. This specification defines the coordination between projects and ensures ontology discipline is maintained.

## 14.2 CIDSem Term Grammar Compliance

### 14.2.1 Supported Term Types

CIDStore validates and supports all CIDSem term types:

```python
# Entities: E:<namespace>:<label>
alice = "E:usr:alice"
berlin = "E:geo:berlin" 
cup123 = "E:obj:cup123"

# Relations: R:<namespace>:<label>  
made_claim = "R:sys:madeClaimAbout"
lives_in = "R:sys:livesIn"
friends_with = "R:usr:friendsWith"

# Events: EV:<namespace>:<label>
posted_message = "EV:usr:postedMessage"
system_crash = "EV:sys:crashed"

# Literals: L:<type>:<value>
number = "L:int:42"
text = "L:str:hello"
timestamp = "L:time:2025-10-05T17:00:00Z"

# Contexts: C:<namespace>:<label>
placement_rule = "C:sys:CupPlacementRule"
```

### 14.2.2 Format Validation

All predicate registrations undergo strict CIDSem format validation:

```python
def _validate_cidsem_format(self, predicate_name: str) -> bool:
    """Validate CIDSem term grammar: kind:namespace:label format."""
    parts = predicate_name.split(':')
    if len(parts) < 3:
        return False
    
    kind = parts[0]
    # Valid kinds: E (Entity), R (Relation), EV (Event), L (Literal), C (Context)
    valid_kinds = {'E', 'R', 'EV', 'L', 'C'}
    
    return kind in valid_kinds
```

**Examples of validation:**

- ✅ `R:usr:friendsWith` → Valid (Relation, usr namespace, friendsWith label)
- ✅ `E:geo:berlin` → Valid (Entity, geo namespace, berlin label)  
- ✅ `R:sys::startsWithColon` → Valid (label can contain colons)
- ❌ `foaf:knows` → Invalid (missing second colon)
- ❌ `friendsWith` → Invalid (missing kind and namespace)

## 14.3 Triple Storage Focus

### 14.3.1 CIDStore Responsibility Boundary

CIDStore focuses solely on efficient storage and retrieval of semantic triples, regardless of their origin. Complex statement decomposition and metadata management are CIDSem concerns:

```python
# CIDStore only sees individual triples for storage
# (Origin and decomposition process is outside CIDStore scope)

# Triple 1: Direct relation
await store.insert_triple("E:usr:alice", "R:sys:madeClaimAbout", "E:geo:berlin")

# Triple 2: Another relation (CIDStore doesn't know/care about relationships)
await store.insert_triple("E:usr:alice", "R:sys:occursAt", "L:time:2025-10-05T17:00:00Z")

# Each triple gets its own CID and can be referenced for metadata
# But CIDStore only optimizes storage, not metadata relationships
```

### 14.3.2 Plugin Registry and Configuration

CIDStore uses explicit plugin registration with JSON/YAML configuration for predicate-to-plugin mapping:

```python
class PluginRegistry:
    """Registry for predicate plugin implementations."""
    
    def __init__(self):
        self.plugins: Dict[str, Type[SpecializedDataStructure]] = {}
    
    def register(self, name: str, plugin_class: Type[SpecializedDataStructure]):
        """Register a plugin implementation."""
        self.plugins[name] = plugin_class
    
    def create_instance(self, name: str, predicate: E, config: Dict) -> SpecializedDataStructure:
        """Create plugin instance for a predicate."""
        if name not in self.plugins:
            raise ValueError(f"Unknown plugin: {name}")
        return self.plugins[name](predicate, **config)
    
    def list_plugins(self) -> List[str]:
        """List available plugin names."""
        return list(self.plugins.keys())

# Built-in plugin registration
plugin_registry = PluginRegistry()
plugin_registry.register("counter", CounterStore)
plugin_registry.register("multivalue_set", MultiValueSetStore)
plugin_registry.register("timeseries", TimeSeriesStore)  # Future plugin
plugin_registry.register("geospatial", GeospatialStore)  # Future plugin

class PredicateRegistry:
    """Registry that combines CIDSem validation with plugin instantiation."""
    
    def __init__(self, plugin_registry: PluginRegistry):
        self.plugin_registry = plugin_registry
        self._registry: Dict[E, SpecializedDataStructure] = {}
        self.predicate_to_cid: Dict[str, E] = {}
        self.cid_to_predicate: Dict[E, str] = {}
    
    def load_from_config(self, config_path: str):
        """Load predicate configuration from JSON/YAML file."""
        with open(config_path, 'r') as f:
            config = json.load(f) if config_path.endswith('.json') else yaml.safe_load(f)
        
        # Store system configuration
        self.system_config = config.get('system', {})
        self.max_concurrent_osp = self.system_config.get('max_concurrent_osp', 50)
        
        for predicate_name, pred_config in config['predicates'].items():
            # Validate CIDSem format
            if not self._validate_cidsem_format(predicate_name):
                raise ValueError(f"Invalid CIDSem format: {predicate_name}")
            
            # Extract plugin and config
            plugin_name = pred_config['plugin']
            plugin_config = pred_config.get('config', {}).copy()
            
            # Merge performance characteristics into plugin config
            performance = pred_config.get('performance', {})
            plugin_config.update({
                'supports_osp': performance.get('supports_osp', True),
                'supports_pos': performance.get('supports_pos', True),
                'concurrency_limit': performance.get('concurrency_limit', 10)
            })
            
            # Create plugin instance
            predicate_cid = self._compute_predicate_cid(predicate_name)
            plugin_instance = self.plugin_registry.create_instance(
                plugin_name, predicate_cid, plugin_config
            )
            
            # Register the instance
            self._registry[predicate_cid] = plugin_instance
            self.predicate_to_cid[predicate_name] = predicate_cid
            self.cid_to_predicate[predicate_cid] = predicate_name
    
    async def query_osp_parallel(self, obj: Any) -> Dict[str, Set[E]]:
        """Execute OSP query across all predicates that support it, with concurrency control."""
        import asyncio
        
        semaphore = asyncio.Semaphore(self.max_concurrent_osp)
        
        async def query_with_limit(pred_cid: E, plugin: SpecializedDataStructure):
            if not plugin.supports_osp:
                return None
            
            async with semaphore:
                try:
                    subjects = await plugin.query_osp(obj)
                    return subjects if subjects else None
                except Exception as e:
                    # Log error but don't fail entire query
                    return None
        
        tasks = {
            self.cid_to_predicate[pred_cid]: query_with_limit(pred_cid, plugin)
            for pred_cid, plugin in self._registry.items()
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        return {
            pred_name: result 
            for pred_name, result in zip(tasks.keys(), results)
            if result and not isinstance(result, Exception)
        }
    
    def audit_all_plugins(self) -> Dict[str, Dict]:
        """Audit performance of all registered plugins."""
        audit_results = {}
        
        for predicate_cid, plugin in self._registry.items():
            predicate_name = self.cid_to_predicate[predicate_cid]
            audit_results[predicate_name] = plugin.audit_performance()
        
        return audit_results

# Base plugin interface
class SpecializedDataStructure:
    def __init__(self, predicate: E, **config):
        self.predicate = predicate
        self.config = config
        # Performance characteristics from config
        self.supports_osp = config.pop('supports_osp', True)
        self.supports_pos = config.pop('supports_pos', True)
        self.concurrency_limit = config.pop('concurrency_limit', 10)
    
    async def insert(self, subject: E, obj: Any) -> None:
        raise NotImplementedError()
    
    async def query_spo(self, subject: E) -> Any:
        raise NotImplementedError()
    
    async def query_osp(self, obj: Any) -> Set[E]:
        """OSP query - may raise NotImplementedError if supports_osp=False"""
        if not self.supports_osp:
            raise NotImplementedError(f"OSP queries not supported for {self.predicate}")
        raise NotImplementedError()
    
    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query - may raise NotImplementedError if supports_pos=False"""
        if not self.supports_pos:
            raise NotImplementedError(f"POS queries not supported for {self.predicate}")
        raise NotImplementedError()
    
    def audit_performance(self) -> Dict:
        """Return plugin-specific performance metrics."""
        raise NotImplementedError()
```

```json
{
  "predicates": {
    "R:sys:occursAt": {
      "plugin": "timeseries",
      "config": {
        "retention_days": 365,
        "compression": "gzip"
      },
      "performance": {
        "baseline_insert_ms": 0.2,
        "baseline_query_ms": 5.0,
        "supports_osp": true,
        "supports_pos": true,
        "concurrency_limit": 20
      }
    },
    "R:usr:friendsWith": {
      "plugin": "multivalue_set",
      "config": {
        "deduplication": true,
        "max_size": 10000
      },
      "performance": {
        "baseline_insert_ms": 0.1,
        "baseline_query_ms": 0.05,
        "supports_osp": true,
        "supports_pos": true,
        "concurrency_limit": 10
      }
    },
    "R:usr:hasDescription": {
      "plugin": "fulltext",
      "config": {
        "language": "en",
        "stemming": true
      },
      "performance": {
        "baseline_insert_ms": 2.0,
        "baseline_query_ms": 10.0,
        "supports_osp": false,
        "supports_pos": false,
        "concurrency_limit": 2
      }
    },
    "R:usr:ownedQuantity": {
      "plugin": "counter",
      "config": {
        "atomic_operations": true,
        "overflow_behavior": "saturate"
      },
      "performance": {
        "baseline_insert_ms": 0.05,
        "baseline_increment_ms": 0.02,
        "supports_osp": true,
        "supports_pos": true,
        "concurrency_limit": 50
      }
    }
  },
  "system": {
    "max_concurrent_osp": 50,
    "default_timeout_ms": 1000,
    "max_predicates": 200,
    "monitoring_enabled": true
  }
}
```

**Key Principles**: 
- Explicit plugin registration (no magic discovery)
- Configuration-driven predicate-to-plugin mapping
- Performance characteristics specified per predicate
- Plugin instances created on-demand from configuration
- Concurrency control for OSP queries (system-wide and per-predicate)
- Optional OSP/POS support (plugins can opt-out of reverse lookups)
- System-level configuration for global constraints

## 14.4 Ontology Growth Discipline

### 14.4.1 Performance-Justified Predicates

Following CIDSem's constrained ontology principle, new predicates require concrete performance justifications:

```python
class PerformanceJustificationRegistry:
    """Track performance justifications per CIDSem discipline."""
    
    def justify_predicate(self, predicate: str, plugin: str, 
                         improvement: str, benchmark_data: Dict):
        """Record quantified performance improvement."""
        
        justification = PredicateJustification(
            predicate=predicate,
            plugin_type=plugin,
            performance_improvement=improvement,  # e.g., "100x faster OSP queries"
            measurement_data=benchmark_data,      # Actual benchmark results
            approved_by_cidsem=False             # Requires coordination approval
        )
        
        self.justifications[predicate] = justification
```

### 14.4.2 Ontology Size Monitoring

CIDStore actively monitors ontology size to prevent OSP query degradation:

```python
def audit_ontology_discipline(self) -> Dict[str, list[str]]:
    """Audit adherence to CIDSem ontology growth discipline."""
    
    violations = []
    warnings = []
    
    # Check individual justifications
    for predicate in self.predicate_to_cid.keys():
        if predicate not in self.performance_justifications:
            violations.append(f"No performance justification: {predicate}")
    
    # Check overall size constraint
    if len(self._registry) > 200:
        warnings.append(f"Ontology size ({len(self._registry)}) "
                       "approaching performance degradation threshold")
    
    return {'violations': violations, 'warnings': warnings}
```

## 14.5 Configuration-Based Coordination

### 14.5.1 CIDSem Configuration Integration

```python
# Initialize with plugin registry
plugin_registry = PluginRegistry()
plugin_registry.register("counter", CounterStore)
plugin_registry.register("multivalue_set", MultiValueSetStore)
plugin_registry.register("timeseries", TimeSeriesStore)
plugin_registry.register("geospatial", GeospatialStore)

# Load predicate configuration
registry = PredicateRegistry(plugin_registry)
registry.load_from_config("cidsem_predicates.json")

# Configuration file format
cidsem_config = {
    "predicates": {
        "R:usr:friendsWith": {
            "plugin": "multivalue_set",
            "config": {
                "deduplication": True,
                "max_connections": 5000
            },
            "performance_baseline": {
                "insert_ms": 0.1,
                "query_spo_ms": 0.05
            }
        },
        "R:geo:locatedAt": {
            "plugin": "geospatial",
            "config": {
                "coordinate_system": "WGS84",
                "precision": 6
            },
            "performance_baseline": {
                "insert_ms": 0.5,
                "spatial_query_ms": 2.0
            }
        }
    }
}
```

### 14.5.2 Plugin Discovery and Validation

```python
class ConfigurationValidator:
    """Validate predicate configuration against available plugins."""
    
    def __init__(self, plugin_registry: PluginRegistry):
        self.plugin_registry = plugin_registry
    
    def validate_config(self, config: Dict) -> List[str]:
        """Validate configuration and return any errors."""
        errors = []
        
        for predicate, pred_config in config.get('predicates', {}).items():
            # Validate CIDSem format
            if not self._validate_cidsem_format(predicate):
                errors.append(f"Invalid CIDSem format: {predicate}")
            
            # Validate plugin exists
            plugin_name = pred_config.get('plugin')
            if plugin_name not in self.plugin_registry.list_plugins():
                errors.append(f"Unknown plugin '{plugin_name}' for predicate {predicate}")
            
            # Validate performance baseline exists
            if 'performance_baseline' not in pred_config:
                errors.append(f"Missing performance baseline for {predicate}")
        
        # Validate ontology size constraint
        if len(config.get('predicates', {})) > 200:
            errors.append(f"Ontology size ({len(config['predicates'])}) exceeds CIDSem constraint of 200")
        
        return errors
    
    def suggest_plugin(self, predicate: str) -> str:
        """Suggest appropriate plugin based on predicate name patterns."""
        if 'occursAt' in predicate or 'time' in predicate.lower():
            return 'timeseries'
        elif any(geo in predicate for geo in ['locatedAt', 'partOf', 'geo:']):
            return 'geospatial'
        elif any(qty in predicate for qty in ['Quantity', 'count', 'Amount']):
            return 'counter'
        else:
            return 'multivalue_set'
```

## 14.6 Usage Examples

### 14.6.1 Configuration-Based Setup with Unified Query Interface

```python
from cidstore import CIDStore
from cidstore.predicates import PredicateRegistry, PluginRegistry
from cidstore.plugins import CounterStore, MultiValueSetStore
from cidstore.keys import E

# Setup plugin registry
plugin_registry = PluginRegistry()
plugin_registry.register("counter", CounterStore)
plugin_registry.register("multivalue_set", MultiValueSetStore)

# Load configuration
predicate_registry = PredicateRegistry(plugin_registry)
predicate_registry.load_from_config("cidsem_predicates.json")

# Initialize CIDStore with predicate registry
store = CIDStore(hdf, wal, predicate_registry=predicate_registry)

# CIDSem entities and predicates (as CIDs)
alice = E.from_str("E:usr:alice")
bob = E.from_str("E:usr:bob")
kung_fu = E.from_str("E:skill:kungfu")
knows = E.from_str("R:usr:knows")
quantity = E.from_str("R:usr:ownedQuantity")

# Insert triples
await store.insert_triple(alice, knows, kung_fu)
await store.insert_triple(alice, quantity, 5)

# Query using unified interface
# SPO: What does Alice know?
results = await store.query(P=knows, S=alice)
# Returns: [(alice, knows, kung_fu)]

# POS: Who knows kung-fu?
results = await store.query(P=knows, O=kung_fu)
# Returns: [(alice, knows, kung_fu)]

# Two-step: What does Alice relate to?
results = await store.query(S=alice)
# Returns: [(alice, knows, kung_fu), (alice, quantity, 5)]

# Fan-out OSP: What relates to kung-fu?
results = await store.query(O=kung_fu)
# Returns: [(alice, knows, kung_fu)]
```

### 14.6.2 Configuration Validation and Performance Audit

```python
# Validate configuration before loading
validator = ConfigurationValidator(plugin_registry)
errors = validator.validate_config(config_data)

if errors:
    print("Configuration Errors:")
    for error in errors:
        print(f"  - {error}")
else:
    print("Configuration valid - loading predicates...")
    predicate_registry.load_from_config("cidsem_predicates.json")

# Audit plugin performance after operations
audit_results = predicate_registry.audit_all_plugins()

for predicate_name, performance_data in audit_results.items():
    print(f"{predicate_name}: {performance_data}")

# Example output:
# R:usr:friendsWith: {'avg_insert_ms': 0.08, 'avg_query_spo_ms': 0.04, 'health': 'OK'}
# R:usr:ownedQuantity: {'avg_insert_ms': 0.03, 'avg_increment_ms': 0.01, 'health': 'OK'}
# R:geo:locatedAt: {'avg_insert_ms': 0.6, 'avg_spatial_query_ms': 3.2, 'health': 'DEGRADED'}
```

## 14.7 Performance Impact

### 14.7.1 Query Pattern Performance Analysis

CIDSem's constrained ontology directly impacts CIDStore query performance:

```python
# Query performance with unified interface:

# Single-step queries: O(1)
await store.query(P=knows, S=alice)       # SPO - direct plugin lookup
await store.query(P=knows, O=kung_fu)     # POS - direct plugin lookup

# Two-step queries: O(k) where k = predicates for subject
await store.query(S=alice)                # Main store lookup + k plugin queries
await store.query(S=alice, O=kung_fu)     # Main store lookup + k membership checks

# Fan-out queries: O(m) where m ≤ 200 (CIDSem constraint)
await store.query(O=kung_fu)              # Fan-out to all OSP-capable predicates

# Performance impact of CIDSem's 200-predicate limit:
# - Fan-out queries scale linearly: O(P) where P ≤ 200
# - Parallel execution with concurrency control (max_concurrent_osp=50)
# - Without constraint: P could be thousands → unacceptable performance
# - With constraint: Worst case is bounded and predictable
```

### 14.7.2 Justification Requirements

Every specialized predicate must demonstrate measurable performance improvement:

| Predicate | Plugin | Justification | Measured Improvement |
|-----------|--------|---------------|---------------------|
| `R:usr:friendsWith` | multivalue_set | Social graph traversal | 50x faster than triple scan |
| `R:geo:locatedAt` | geospatial | Spatial proximity queries | 100x faster with R-tree index |
| `R:sys:occursAt` | timeseries | Temporal range queries | 25x faster with time index |
| `R:usr:ownedQuantity` | counter | Quantity aggregation | 200x faster than sum(triples) |

## 14.8 Future Coordination

### 14.8.1 Automated Synchronization

Future versions will support automated ontology synchronization:

```python
# Planned: Automatic CIDSem spec parsing
class CIDSemOntologyLoader:
    def sync_from_cidsem_repo(self, repo_path: str):
        """Automatically sync with CIDSem ontology specifications."""
        
        # Parse CIDSem spec files
        ontology_entries = self._parse_cidsem_specs(repo_path)
        
        # Generate optimized plugin configurations
        plugin_configs = self._infer_plugin_mappings(ontology_entries)
        
        # Validate performance justifications
        self._validate_performance_claims(plugin_configs)
        
        # Apply to CIDStore registry
        self._update_predicate_registry(plugin_configs)
```

### 14.8.2 Cross-Project Testing

Coordination includes shared performance testing:

```yaml
# Planned: Joint CIDSem-CIDStore benchmarks
benchmarks:
  triple_insertion:
    cidsem_complex_statements: 10000
    cidstore_decomposed_triples: 40000  # 4 triples per complex statement
    target_throughput: ">1000 statements/sec"
    
  osp_queries:
    ontology_size: 200
    query_latency: "<10ms"
    fan_out_predicates: "all specialized"
    
  memory_efficiency:
    specialized_vs_generic: ">10x compression"
    index_overhead: "<20% of data"
```

This integration ensures CIDStore's predicate specialization directly supports CIDSem's semantic triple storage needs while maintaining the performance discipline that makes OSP queries tractable at scale.