# 13. Predicate Plugin Infrastructure

## 13.1 Overview

The predicate plugin infrastructure enables explicit registration and configuration of specialized data structures for semantic predicates. This allows for extensible, ontology-driven storage optimization with clear configuration and performance management.

| Feature | Description |
|---------|-------------|
| **Explicit Registration** | Plugins are explicitly registered by name with implementation classes |
| **Configuration-Driven** | JSON/YAML configuration maps predicates to plugins with performance specs |
| **CIDSem Integration** | Validates CIDSem term grammar and enforces ontology discipline |
| **Performance Management** | Per-predicate OSP/POS support flags and concurrency limits |
| **Type Safety** | Static plugin registration with type checking |
| **Monitoring** | Plugin-specific performance audit methods |

## 13.2 CIDSem Integration

### 13.2.1 Term Grammar Compatibility

CIDStore predicate plugins integrate with CIDSem's ontology format and term grammar:

```yaml
# CIDSem ontology format support with performance characteristics
predicates:
  "R:sys:madeClaimAbout":
    plugin: "multivalue_set"
    config:
      deduplication: true
      max_size: 10000
    performance:
      baseline_insert_ms: 0.1
      baseline_query_ms: 0.05
      supports_osp: true
      supports_pos: true
      concurrency_limit: 10

  "R:usr:friendsWith":
    plugin: "multivalue_set"
    config:
      symmetric: true
      max_connections: 5000
    performance:
      baseline_insert_ms: 0.1
      baseline_query_ms: 0.05
      supports_osp: true
      supports_pos: true
      concurrency_limit: 10

  "R:geo:locatedAt":
    plugin: "geospatial"
    config:
      coordinate_system: "WGS84"
      spatial_index: "rtree"
    performance:
      baseline_insert_ms: 0.5
      baseline_spatial_query_ms: 2.0
      supports_osp: true
      supports_pos: true
      concurrency_limit: 5

system:
  max_concurrent_osp: 50
  max_predicates: 200
  monitoring_enabled: true
```

### 13.2.2 Ontology Discipline Integration

Aligns with CIDSem's constrained ontology growth policy:

- **Performance-Justified Predicates**: Only predicates that enable specialized DS optimizations
- **OSP Query Optimization**: Plugin system supports efficient enumeration across ~200 predicates
- **Specialized Indexing**: Geographic, temporal, and hierarchical predicates get optimized storage
- **Measurement-Driven**: Plugin registration requires performance justification

### 13.2.3 CIDStore Responsibility

CIDStore focuses on efficient triple storage, not complex statement decomposition:

```python
# CIDStore sees only individual triples
# (CIDSem handles complex statement decomposition)

await store.insert_triple("E:usr:alice", "R:sys:madeClaimAbout", "E:geo:berlin")
await store.insert_triple("E:usr:alice", "R:sys:occursAt", "L:time:2025-10-05T17:00:00Z")

# Each triple gets its own CID and can be referenced for metadata
# CIDStore only optimizes storage patterns, not semantic relationships
```

## 13.3 Plugin Architecture

```python
# Explicit plugin registration approach
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

# Initialize and register built-in plugins
plugin_registry = PluginRegistry()
plugin_registry.register("counter", CounterStore)
plugin_registry.register("multivalue_set", MultiValueSetStore)
plugin_registry.register("timeseries", TimeSeriesStore)
plugin_registry.register("geospatial", GeospatialStore)
plugin_registry.register("fulltext", FullTextStore)

# PredicateRegistry combines CIDSem validation with plugin instantiation
class PredicateRegistry:
    """Registry that combines CIDSem validation with plugin instantiation."""

    def __init__(self, plugin_registry: PluginRegistry):
        self.plugin_registry = plugin_registry
        self._registry: Dict[E, SpecializedDataStructure] = {}
        self.predicate_to_cid: Dict[str, E] = {}
        self.cid_to_predicate: Dict[E, str] = {}
        self.system_config: Dict = {}

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

            # Extract and merge configuration
            plugin_name = pred_config['plugin']
            plugin_config = pred_config.get('config', {}).copy()
            performance = pred_config.get('performance', {})

            # Add performance characteristics to plugin config
            plugin_config.update({
                'supports_osp': performance.get('supports_osp', True),
                'supports_pos': performance.get('supports_pos', True),
                'concurrency_limit': performance.get('concurrency_limit', 10)
            })

            # Create and register plugin instance
            predicate_cid = self._compute_predicate_cid(predicate_name)
            plugin_instance = self.plugin_registry.create_instance(
                plugin_name, predicate_cid, plugin_config
            )

            self._registry[predicate_cid] = plugin_instance
            self.predicate_to_cid[predicate_name] = predicate_cid
            self.cid_to_predicate[predicate_cid] = predicate_name
```
        H --> J[counter.py: CounterPlugin]
        H --> K[multivalue.py: SetPlugin]
        H --> L[timeseries.py: TimeSeriesPlugin]
        H --> M[custom/: CustomPlugins]
    end

    subgraph Runtime Registry
        I --> N[PredicateRegistry]
        N --> O[Active DS Instances]
    end
```

## 13.3 Plugin Interface

### 13.3.1 Base Plugin Class

```python
from abc import ABC, abstractmethod
from typing import Any, Dict, Set, Type, Optional
from cidstore.keys import E

class PredicatePlugin(ABC):
    """Base class for all predicate specialization plugins."""

    @property
    @abstractmethod
    def plugin_name(self) -> str:
        """Unique plugin identifier (e.g., 'counter', 'multivalue_set')."""
        pass

    @property
    @abstractmethod
    def supported_value_types(self) -> Set[Type]:
        """Value types this plugin can handle (e.g., {int}, {E}, {str, int})."""
        pass

    @abstractmethod
    def create_instance(self, predicate: E, config: Dict[str, Any]) -> 'SpecializedDataStructure':
        """Create a new DS instance for the given predicate with configuration."""
        pass

    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate plugin-specific configuration parameters."""
        pass

    def get_default_config(self) -> Dict[str, Any]:
        """Return default configuration for this plugin type."""
        return {}

    def get_schema(self) -> Dict[str, Any]:
        """Return JSON schema for configuration validation."""
        return {"type": "object", "properties": {}}
```

### 13.3.2 Example Plugin Implementation

```python
class CounterPlugin(PredicatePlugin):
    """Plugin for integer counter predicates."""

    @property
    def plugin_name(self) -> str:
        return "counter"

    @property
    def supported_value_types(self) -> Set[Type]:
        return {int}

    def create_instance(self, predicate: E, config: Dict[str, Any]) -> CounterStore:
        atomic = config.get("atomic_operations", True)
        overflow_behavior = config.get("overflow", "saturate")  # saturate, wrap, error
        return CounterStore(predicate, atomic=atomic, overflow=overflow_behavior)

    def validate_config(self, config: Dict[str, Any]) -> bool:
        valid_overflow = {"saturate", "wrap", "error"}
        return config.get("overflow", "saturate") in valid_overflow

    def get_default_config(self) -> Dict[str, Any]:
        return {
            "atomic_operations": True,
            "overflow": "saturate",
            "initial_value": 0
        }

    def get_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "atomic_operations": {"type": "boolean"},
                "overflow": {"type": "string", "enum": ["saturate", "wrap", "error"]},
                "initial_value": {"type": "integer"}
            }
        }
```

## 13.4 Plugin Base Interface

```python
class SpecializedDataStructure:
    """Base interface for all predicate plugins."""

    def __init__(self, predicate: E, **config):
        self.predicate = predicate
        self.config = config
        # Extract performance characteristics
        self.supports_osp = config.pop('supports_osp', True)
        self.supports_pos = config.pop('supports_pos', True)
        self.concurrency_limit = config.pop('concurrency_limit', 10)

    async def insert(self, subject: E, obj: Any) -> None:
        """Insert operation - implementation specific."""
        raise NotImplementedError()

    async def query_spo(self, subject: E) -> Any:
        """SPO query - implementation specific."""
        raise NotImplementedError()

    async def query_osp(self, obj: Any) -> Set[E]:
        """OSP query - may raise NotImplementedError if supports_osp=False."""
        if not self.supports_osp:
            raise NotImplementedError(f"OSP queries not supported for {self.predicate}")
        raise NotImplementedError()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query - may raise NotImplementedError if supports_pos=False."""
        if not self.supports_pos:
            raise NotImplementedError(f"POS queries not supported for {self.predicate}")
        raise NotImplementedError()

    def audit_performance(self) -> Dict:
        """Return plugin-specific performance metrics.

        Returns:
            Dict containing plugin-specific metrics. Format is plugin-defined
            but should include basic health indicators.
        """
        raise NotImplementedError()
```

## 13.5 Configuration Format

### 13.5.1 JSON/YAML Configuration with Performance Specs

```json
{
  "predicates": {
    "R:usr:ownedQuantity": {
      "plugin": "counter",
      "config": {
        "atomic_operations": true,
        "overflow_behavior": "saturate",
        "initial_value": 0
      },
      "performance": {
        "baseline_insert_ms": 0.05,
        "baseline_increment_ms": 0.02,
        "supports_osp": true,
        "supports_pos": true,
        "concurrency_limit": 50
      }
    },
    "R:usr:friendsWith": {
      "plugin": "multivalue_set",
      "config": {
        "deduplication": true,
        "symmetric": true,
        "max_size": 5000
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

### 13.5.2 Loading and Validation

```python
import json
from typing import Dict

def load_predicate_config(config_path: str) -> Dict:
    """Load predicate configuration from JSON/YAML file."""
    with open(config_path) as f:
        config = json.load(f)

    # Validate system constraints
    if len(config["predicates"]) > config["system"]["max_predicates"]:
        raise ValueError(f"Too many predicates: {len(config['predicates'])} > {config['system']['max_predicates']}")

    # Validate CIDSem term format
    for pred_name in config["predicates"]:
        if pred_name.count(":") != 2:
            raise ValueError(f"Invalid predicate format: {pred_name} (must have exactly 2 colons)")

        kind, namespace, label = pred_name.split(":")
        if kind not in ["R", "E", "A"]:
            raise ValueError(f"Invalid kind: {kind} (must be R, E, or A)")

    return config

# Usage
config = load_predicate_config("predicates.json")
for pred_name, spec in config["predicates"].items():
    predicate_cid = E.from_str(pred_name)
    plugin_class = plugin_registry.get(spec["plugin"])
    if not plugin_class:
        raise ValueError(f"Plugin not found: {spec['plugin']}")

    # Instantiate with configuration
    plugin_instance = plugin_class(config=spec.get("config", {}))

    # Register in predicate registry
    predicate_registry.register(predicate_cid, plugin_instance)
```

## 13.6 Plugin Performance Monitoring

### 13.6.1 Audit API

Each plugin implements the `audit_performance()` method to return plugin-specific metrics:

```python
class CounterPlugin(SpecializedDataStructure):
    def audit_performance(self) -> Dict:
        """Return counter-specific metrics."""
        return {
            "total_increments": self._total_increments,
            "total_decrements": self._total_decrements,
            "current_sum": sum(self._counters.values()),
            "counter_count": len(self._counters),
            "avg_value": sum(self._counters.values()) / len(self._counters) if self._counters else 0,
            "max_value": max(self._counters.values()) if self._counters else 0,
            "overflow_events": self._overflow_count
        }

class FulltextPlugin(SpecializedDataStructure):
    def audit_performance(self) -> Dict:
        """Return fulltext-specific metrics."""
        return {
            "total_documents": len(self._index),
            "index_size_mb": self._calculate_index_size() / (1024 * 1024),
            "avg_doc_length": sum(len(d) for d in self._index.values()) / len(self._index) if self._index else 0,
            "total_queries": self._query_count,
            "avg_query_time_ms": self._total_query_time / self._query_count if self._query_count else 0,
            "cache_hit_rate": self._cache_hits / (self._cache_hits + self._cache_misses) if (self._cache_hits + self._cache_misses) > 0 else 0
        }
```
### 13.6.2 System-Level Monitoring

```python
class SystemPerformanceMonitor:
    """Monitor performance across all predicates and plugins."""

    def __init__(self, predicate_registry: PredicateRegistry, config: Dict):
        self.predicate_registry = predicate_registry
        self.config = config
        self.monitoring_enabled = config["system"]["monitoring_enabled"]

    def audit_all_predicates(self) -> Dict[str, Dict]:
        """Collect audit data from all registered predicates."""
        if not self.monitoring_enabled:
            return {}

        audit_results = {}
        for pred_name, plugin_instance in self.predicate_registry.get_all():
            try:
                plugin_metrics = plugin_instance.audit_performance()
                audit_results[pred_name] = {
                    "plugin_type": plugin_instance.__class__.__name__,
                    "metrics": plugin_metrics,
                    "timestamp": time.time()
                }
            except Exception as e:
                audit_results[pred_name] = {
                    "error": str(e),
                    "timestamp": time.time()
                }

        return audit_results

    def check_performance_degradation(self) -> List[str]:
        """Identify predicates with performance issues."""
        warnings = []
        audit_data = self.audit_all_predicates()

        for pred_name, data in audit_data.items():
            if "error" in data:
                warnings.append(f"{pred_name}: audit failed - {data['error']}")
                continue

            # Compare against baseline from config
            pred_config = self.config["predicates"].get(pred_name, {})
            performance = pred_config.get("performance", {})

            # Plugin-specific checks can be implemented here
            # Example: check if query time exceeds baseline

        return warnings
```

## 13.7 Built-in Plugin Types

### 13.7.1 Core Plugins

| Plugin Name | Value Types | Use Cases | Configuration Options | OSP/POS Support |
|-------------|-------------|-----------|----------------------|-----------------|
| `counter` | int | Page views, quantities, scores | atomic_operations, overflow_behavior, initial_value | ✓ / ✓ |
| `multivalue_set` | E, str, int | Relationships, tags, categories | deduplication, max_size, symmetric | ✓ / ✓ |
| `single_value` | Any | Properties, attributes | last_writer_wins, versioning | ✓ / ✓ |
| `timeseries` | Any + timestamp | Events, measurements, logs | retention_days, compression, indexing | ✓ / ✓ |
| `boolean` | bool | Flags, permissions, states | default_value, null_handling | ✓ / ✓ |
| `enum` | str | Categories, statuses | allowed_values, strict_mode | ✓ / ✓ |
| `fulltext` | str | Descriptions, documents | language, stemming | ✗ / ✗ |

### 13.7.2 Extended Plugins (Optional)

| Plugin Name | Value Types | Use Cases | Requirements | OSP/POS Support |
|-------------|-------------|-----------|--------------|-----------------|
| `geospatial` | (lat, lon, alt) | Locations, boundaries | `rtree`, `shapely` | ✓ / ✓ |
| `bloom_filter` | Any | Probabilistic membership | `pybloom_live` | ✗ / ✗ |
| `bitmap` | int | Categorical data | `roaringbitmap` | ✓ / ✓ |

## 13.8 Error Handling and Validation

### 13.8.1 Plugin Validation

```python
class PluginValidator:
    """Validate plugin implementations and configurations."""

    def validate_plugin(self, plugin_class: type) -> List[str]:
        """Return list of validation errors (empty if valid)."""
        errors = []

        # Check if it's a subclass of SpecializedDataStructure
        if not issubclass(plugin_class, SpecializedDataStructure):
            errors.append(f"Plugin must inherit from SpecializedDataStructure")

        # Check required methods are implemented
        required_methods = ['insert', 'query', 'delete', 'audit_performance']
        for method in required_methods:
            if not hasattr(plugin_class, method):
                errors.append(f"Missing required method: {method}")

        # Validate supports_osp and supports_pos flags
        try:
            instance = plugin_class(config={})
            if not isinstance(instance.supports_osp, bool):
                errors.append("supports_osp must be a boolean")
            if not isinstance(instance.supports_pos, bool):
                errors.append("supports_pos must be a boolean")
        except Exception as e:
            errors.append(f"Error instantiating plugin: {e}")

        return errors

    def validate_config_schema(self, plugin_class: type, config: Dict[str, Any]) -> List[str]:
        """Validate configuration against plugin's expected schema."""
        try:
            # Try to instantiate with config
            instance = plugin_class(config=config)
            return []
        except Exception as e:
            return [f"Configuration validation error: {e}"]
```

### 13.8.2 Runtime Error Handling

```python
class SafePredicateWrapper:
    """Wrapper that provides error handling and fallback for predicate DS."""

    def __init__(self, ds: SpecializedDataStructure, fallback_behavior: str = "log_and_continue"):
        self.ds = ds
        self.fallback_behavior = fallback_behavior
        self.error_count = 0
        self.last_error = None

    async def insert(self, subject: E, obj: Any) -> None:
        try:
            await self.ds.insert(subject, obj)
        except Exception as e:
            self.error_count += 1
            self.last_error = e

            if self.fallback_behavior == "raise":
                raise
            elif self.fallback_behavior == "log_and_continue":
                logger.error(f"Error in predicate DS for {self.ds.predicate}: {e}")
            elif self.fallback_behavior == "disable_on_error":
                if self.error_count > 10:  # Disable after too many errors
                    logger.critical(f"Disabling predicate DS after {self.error_count} errors")
                    # Could replace with null implementation
```

## 13.9 CIDSem Ontology Constraints

### 13.9.1 Term Grammar Validation

CIDSem uses the term format `kind:namespace:label` with exactly two colons. CIDStore must validate predicate names against this grammar:

```python
def validate_cidsem_term(term: str) -> bool:
    """Validate CIDSem term format: kind:namespace:label."""
    if term.count(":") != 2:
        return False

    kind, namespace, label = term.split(":")

    # kind must be one of: R (relation), E (entity), A (attribute)
    if kind not in ["R", "E", "A"]:
        return False

    # namespace and label must be non-empty alphanumeric+underscore
    if not namespace or not label:
        return False
    if not (namespace.replace("_", "").isalnum() and label.replace("_", "").isalnum()):
        return False

    return True

# Example valid terms:
# R:usr:friendsWith
# E:geo:berlin
# A:sys:version
```

### 13.9.2 Ontology Discipline

CIDSem enforces an ontology discipline limiting predicates to approximately 200 terms. This constraint:

- Encourages focused, well-designed ontologies
- Allows PredicateRegistry to maintain in-memory mappings efficiently
- Simplifies monitoring and debugging
- Enforced via `max_predicates` in system configuration

```python
class PredicateRegistry:
    def __init__(self, config: Dict):
        self.max_predicates = config["system"]["max_predicates"]
        self.predicates: Dict[E, SpecializedDataStructure] = {}

    def register(self, predicate: E, plugin_instance: SpecializedDataStructure):
        """Register a predicate with ontology size limit."""
        if len(self.predicates) >= self.max_predicates:
            raise ValueError(f"Ontology size limit reached: {self.max_predicates} predicates")

        # Validate CIDSem term format
        pred_str = str(predicate)  # Assuming E has __str__
        if not validate_cidsem_term(pred_str):
            raise ValueError(f"Invalid CIDSem term format: {pred_str}")

        self.predicates[predicate] = plugin_instance
```

## 13.10 Relationship to CIDSem Project

### 13.10.1 Responsibility Boundary

**CIDStore** (this project):
- Stores triples (S, P, O) where each is a CID
- Provides specialized data structures for specific predicates
- Validates CIDSem term grammar for predicate names
- Enforces ontology size limits

**CIDSem** (separate project):
- Defines ontology structure and term grammar (kind:namespace:label)
- Handles complex statement decomposition into triples
- Manages metadata about claims (provenance, versioning, confidence)
- Creates CIDs for entities, relations, and attributes

### 13.10.2 Configuration Coordination

CIDStore predicate configurations can be generated from CIDSem ontology specifications:

```python
# CIDSem provides ontology definition
cidsem_ontology = {
    "relations": [
        {"term": "R:usr:friendsWith", "category": "social", "symmetric": True},
        {"term": "R:usr:ownedQuantity", "category": "counting"},
        {"term": "R:usr:hasDescription", "category": "text"}
    ]
}

# CIDStore plugin configuration mapper
def map_cidsem_to_cidstore_config(ontology: Dict) -> Dict:
    """Generate CIDStore configuration from CIDSem ontology."""
    config = {"predicates": {}, "system": {"max_predicates": 200}}

    for relation in ontology["relations"]:
        term = relation["term"]
        category = relation["category"]

        # Map category to plugin type
        if category == "counting":
            plugin_config = {
                "plugin": "counter",
                "config": {"atomic_operations": True},
                "performance": {"supports_osp": True, "supports_pos": True, "concurrency_limit": 50}
            }
        elif category == "social":
            plugin_config = {
                "plugin": "multivalue_set",
                "config": {"symmetric": relation.get("symmetric", False)},
                "performance": {"supports_osp": True, "supports_pos": True, "concurrency_limit": 10}
            }
        elif category == "text":
            plugin_config = {
                "plugin": "fulltext",
                "config": {"language": "en"},
                "performance": {"supports_osp": False, "supports_pos": False, "concurrency_limit": 2}
            }
        else:
            plugin_config = {
                "plugin": "single_value",
                "config": {},
                "performance": {"supports_osp": True, "supports_pos": True, "concurrency_limit": 20}
            }

        config["predicates"][term] = plugin_config

    return config
```

## 13.11 Testing and Validation

### 13.11.1 Plugin Testing Framework

```python
import pytest
from typing import Type

class PluginTestSuite:
    """Base test suite for plugin implementations."""

    @pytest.fixture
    def plugin_class(self) -> Type[SpecializedDataStructure]:
        """Override in subclass to provide plugin class to test."""
        raise NotImplementedError()

    @pytest.fixture
    def plugin_config(self) -> Dict:
        """Override in subclass to provide test configuration."""
        return {}

    @pytest.fixture
    def plugin_instance(self, plugin_class, plugin_config):
        """Create plugin instance for testing."""
        return plugin_class(config=plugin_config)

    async def test_basic_insert_query(self, plugin_instance):
        """Test basic insert and query operations."""
        subject = E.from_str("E:usr:alice")
        value = "test_value"

        await plugin_instance.insert(subject, value)
        results = await plugin_instance.query(subject)

        assert value in results

    async def test_delete(self, plugin_instance):
        """Test deletion operations."""
        subject = E.from_str("E:usr:bob")
        value = "test_value"

        await plugin_instance.insert(subject, value)
        await plugin_instance.delete(subject, value)
        results = await plugin_instance.query(subject)

        assert value not in results

    async def test_audit_performance(self, plugin_instance):
        """Test audit_performance returns valid metrics."""
        metrics = plugin_instance.audit_performance()

        assert isinstance(metrics, dict)
        assert len(metrics) > 0  # Should return something

    async def test_supports_osp(self, plugin_instance):
        """Test OSP support if declared."""
        if not plugin_instance.supports_osp:
            pytest.skip("Plugin does not support OSP")

        subject = E.from_str("E:usr:carol")
        value = E.from_str("E:usr:dave")
        predicate = E.from_str("R:usr:test")

        await plugin_instance.insert(subject, value)
        results = await plugin_instance.query_osp(value, predicate)

        assert subject in results

# Example concrete test
class TestCounterPlugin(PluginTestSuite):
    @pytest.fixture
    def plugin_class(self):
        return CounterPlugin

    @pytest.fixture
    def plugin_config(self):
        return {"atomic_operations": True, "initial_value": 0}

    async def test_increment(self, plugin_instance):
        """Test counter-specific increment."""
        subject = E.from_str("E:usr:alice")

        await plugin_instance.increment(subject, 5)
        value = await plugin_instance.query(subject)

        assert value == 5
```

### 13.11.2 Configuration Validation Tests

```python
def test_cidsem_term_validation():
    """Test CIDSem term format validation."""
    # Valid terms
    assert validate_cidsem_term("R:usr:friendsWith")
    assert validate_cidsem_term("E:geo:berlin")
    assert validate_cidsem_term("A:sys:version")

    # Invalid terms
    assert not validate_cidsem_term("InvalidFormat")  # No colons
    assert not validate_cidsem_term("R:usr")  # Only one colon
    assert not validate_cidsem_term("X:usr:test")  # Invalid kind
    assert not validate_cidsem_term("R::label")  # Empty namespace

def test_ontology_size_limit():
    """Test ontology size constraint enforcement."""
    config = {"system": {"max_predicates": 200}}
    registry = PredicateRegistry(config)

    # Should succeed
    for i in range(200):
        pred = E.from_str(f"R:test:pred{i}")
        plugin = SingleValuePlugin(config={})
        registry.register(pred, plugin)

    # Should fail
    with pytest.raises(ValueError, match="Ontology size limit reached"):
        pred = E.from_str("R:test:pred200")
        plugin = SingleValuePlugin(config={})
        registry.register(pred, plugin)
```

## 13.12 Migration and Versioning

### 13.12.1 Plugin Version Management

```python
class PluginVersionInfo:
    """Version information for plugin implementations."""

    def __init__(self, plugin_class: Type[SpecializedDataStructure]):
        self.plugin_class = plugin_class
        self.version = getattr(plugin_class, '__version__', '0.0.1')
        self.min_cidstore_version = getattr(plugin_class, '__min_cidstore_version__', '0.1.0')

    def is_compatible(self, cidstore_version: str) -> bool:
        """Check if plugin is compatible with CIDStore version."""
        # Simple version comparison
        return self._compare_versions(cidstore_version, self.min_cidstore_version) >= 0

    def _compare_versions(self, v1: str, v2: str) -> int:
        """Compare version strings."""
        parts1 = [int(x) for x in v1.split('.')]
        parts2 = [int(x) for x in v2.split('.')]

        for p1, p2 in zip(parts1, parts2):
            if p1 != p2:
                return 1 if p1 > p2 else -1
        return 0

# Usage in plugin
class MyPlugin(SpecializedDataStructure):
    __version__ = '1.2.3'
    __min_cidstore_version__ = '0.5.0'

    # ... implementation ...
```

### 13.12.2 Data Migration Support

```python
class PredicateMigrationManager:
    """Handle data migration when changing plugin types."""

    async def migrate_predicate(
        self,
        predicate: E,
        old_plugin: SpecializedDataStructure,
        new_plugin: SpecializedDataStructure
    ) -> Dict[str, Any]:
        """Migrate data from old plugin to new plugin."""

        migration_stats = {
            "total_subjects": 0,
            "total_values": 0,
            "errors": []
        }

        # Extract all data from old plugin
        try:
            all_subjects = await old_plugin.get_all_subjects()

            for subject in all_subjects:
                migration_stats["total_subjects"] += 1

                # Query all values for this subject
                values = await old_plugin.query(subject)

                for value in values:
                    try:
                        # Transform if needed
                        transformed_value = self._transform_value(
                            value,
                            old_plugin.__class__.__name__,
                            new_plugin.__class__.__name__
                        )

                        # Insert into new plugin
                        await new_plugin.insert(subject, transformed_value)
                        migration_stats["total_values"] += 1

                    except Exception as e:
                        migration_stats["errors"].append({
                            "subject": str(subject),
                            "value": str(value),
                            "error": str(e)
                        })

        except Exception as e:
            migration_stats["errors"].append({
                "stage": "extraction",
                "error": str(e)
            })

        return migration_stats

    def _transform_value(self, value: Any, old_plugin: str, new_plugin: str) -> Any:
        """Transform value from old plugin format to new plugin format."""
        # Implement transformation logic based on plugin types
        # For now, return as-is
        return value
```

## 13.13 Summary

The plugin infrastructure provides:
            plugin_type=plugin,
            performance_improvement=improvement,
            measurement_data=benchmark_data,
            cidsem_coordination=cidsem_approved
        )

    def audit_unjustified_predicates(self) -> List[str]:
        """Return list of predicates lacking performance justification."""
        return [
            pred for pred, just in self.justifications.items()
            if not just.cidsem_coordination or not just.measurement_data
        ]
```

## 13.12 Integration with CIDStore

### 13.10.1 Enhanced Store with Plugin Support

```python
class CIDStoreWithPlugins(CIDStore):
    """CIDStore extended with predicate plugin infrastructure."""

    def __init__(self, hdf, wal, plugin_config_path: Optional[str] = None):
        super().__init__(hdf, wal)

        # Initialize plugin infrastructure
        self.plugin_manager = PredicatePluginManager()
        self.plugin_monitor = PluginMonitor()

        # Discover and load plugins
        self._load_core_plugins()

        if plugin_config_path:
            self._load_configuration(plugin_config_path)

    def _load_core_plugins(self):
        """Load built-in plugins."""
        core_plugins = ["counter", "multivalue_set", "single_value", "timeseries"]
        for plugin_name in core_plugins:
            try:
                self.plugin_manager.load_plugin(plugin_name)
            except Exception as e:
                logger.warning(f"Failed to load core plugin {plugin_name}: {e}")

    def register_predicate_plugin(self, predicate_uri: str, plugin_name: str, config: Dict[str, Any] = None) -> bool:
        """Register a predicate with a specific plugin."""
        try:
            predicate_cid = E.from_str(predicate_uri)
            ds = self.plugin_manager.register_predicate(predicate_cid, plugin_name, config)

            # Wrap with monitoring
            monitored_ds = MonitoredDataStructure(ds, self.plugin_monitor)
            self.predicate_registry.predicate_mappings[predicate_cid] = monitored_ds

            return True
        except Exception as e:
            logger.error(f"Failed to register predicate {predicate_uri} with plugin {plugin_name}: {e}")
            return False
```

## 13.13 Summary

The predicate plugin infrastructure provides:

1. **Explicit Registration**: PluginRegistry with direct class registration (no dynamic imports)
2. **CIDSem Integration**: Term grammar validation (kind:namespace:label), ontology size limits (~200 predicates)
3. **Performance Management**:
   - Per-plugin `audit_performance()` API for custom metrics
   - `supports_osp` and `supports_pos` flags to opt-out of expensive operations
   - Per-predicate `concurrency_limit` and system-wide `max_concurrent_osp`
4. **Configuration-Driven**: JSON/YAML files map predicates to plugins with performance characteristics
5. **Type Safety**: Explicit class-based plugin architecture with SpecializedDataStructure base
6. **Query Optimization**: Parallel OSP with semaphore-based concurrency control
7. **Monitoring**: System-level performance monitoring with per-predicate audit collection
8. **Validation**: CIDSem term format validation, ontology size enforcement, plugin compatibility checks
9. **Testing**: Comprehensive test framework for plugin implementations
10. **Migration**: Data migration support when changing plugin types

This design balances:
- **Simplicity**: Explicit registration, clear responsibility boundaries
- **Performance**: Specialized data structures, concurrency control, opt-out flags
- **Safety**: Type checking, validation, error handling
- **Flexibility**: Plugin-specific configurations and metrics
- **Coordination**: Tight integration with CIDSem ontology format and constraints