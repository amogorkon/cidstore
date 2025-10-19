"""Predicate specialization for CIDStore with CIDSem integration.

Provides specialized data structures for predicates with performance optimizations.
Supports SPO/OSP/POS query patterns with registry-based routing.
Integrates with CIDSem ontology format and term grammar validation.

CIDSem Term Grammar:
- Entities: E:<namespace>:<label>
- Relations: R:<namespace>:<label>
- Events: EV:<namespace>:<label>
- Literals: L:<type>:<value>
- Contexts: C:<namespace>:<label>

Per Spec 14: ~200 predicate limit for performance.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from typing import Any, AsyncIterator, Dict, Optional, Set, Tuple

from .exceptions import (
    InvalidCIDSemTermError,
    PredicateLimitExceededError,
)
from .keys import E

# Per Spec 14: Enforce predicate limit to maintain performance
PREDICATE_LIMIT = 200

# CIDSem term validation regex: kind:namespace:label
# - kind: E, R, EV, L, C (uppercase letters only)
# - namespace: alphanumeric, underscore, hyphen
# - label: alphanumeric, underscore, hyphen, colon (for hierarchical labels)
CIDSEM_TERM_PATTERN = re.compile(r"^(E|R|EV|L|C):([a-zA-Z0-9_-]+):([a-zA-Z0-9_:-]+)$")


class SpecializedDataStructure:
    """Base class for specialized predicate data structures.

    Per Spec 13: All implementations must provide performance characteristics.
    """

    def __init__(
        self,
        predicate: E,
        supports_osp: bool = False,
        supports_pos: bool = False,
        concurrency_limit: Optional[int] = None,
    ):
        self.predicate = predicate
        self.supports_osp = supports_osp
        self.supports_pos = supports_pos
        self.concurrency_limit = concurrency_limit

        # Performance metrics
        self._query_count = 0
        self._insert_count = 0

    async def insert(self, subject: E, obj: Any) -> None:
        raise NotImplementedError()

    async def delete(self, subject: E, obj: Any) -> bool:
        """Delete triple (subject, predicate, obj).

        Args:
            subject: Subject entity
            obj: Object value to delete

        Returns:
            True if triple existed and was deleted, False otherwise
        """
        raise NotImplementedError()

    async def query_spo(self, subject: E) -> Any:
        raise NotImplementedError()

    async def query_osp(self, obj: Any) -> Set[E]:
        raise NotImplementedError()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query: get all subjects where (subject, predicate, obj) exists"""
        raise NotImplementedError()

    def audit_performance(self) -> Dict[str, Any]:
        """Return performance metrics for this plugin.

        Per Spec 13.4: Plugin performance monitoring.
        """
        raise NotImplementedError()


class CounterStore(SpecializedDataStructure):
    """Simple in-memory counter store with reverse index for OSP."""

    def __init__(
        self,
        predicate: E,
        supports_osp: bool = True,
        supports_pos: bool = True,
        concurrency_limit: Optional[int] = None,
    ):
        super().__init__(predicate, supports_osp, supports_pos, concurrency_limit)
        # Plugin maintains its own indices - no composite keys needed
        # SPO index: Subject -> int
        self.spo_index: Dict[E, int] = {}
        # OSP index: int -> Set[Subject]
        self.osp_index: Dict[int, Set[E]] = {}
        # POS index: int -> Set[Subject] (same as OSP for single predicate)
        self.pos_index: Dict[int, Set[E]] = {}

        # Performance tracking
        self._insert_count = 0
        self._query_count = 0
        self._total_increments = 0

    async def insert(self, subject: E, value: int) -> None:
        self._insert_count += 1
        self._total_increments += 1

        # Remove old value from indices if it exists
        old = self.spo_index.get(subject)
        if old is not None:
            # Remove from OSP index
            osp_set = self.osp_index.get(old)
            if osp_set is not None:
                osp_set.discard(subject)
                if not osp_set:
                    del self.osp_index[old]
            # Remove from POS index
            pos_set = self.pos_index.get(old)
            if pos_set is not None:
                pos_set.discard(subject)
                if not pos_set:
                    del self.pos_index[old]

        # Add new value to all indices
        self.spo_index[subject] = value
        self.osp_index.setdefault(value, set()).add(subject)
        self.pos_index.setdefault(value, set()).add(subject)

    async def delete(self, subject: E, obj: Any) -> bool:
        """Delete counter value for subject.

        Args:
            subject: Subject entity
            obj: Expected counter value (must match current value)

        Returns:
            True if value existed and was deleted, False otherwise
        """
        # Check if subject exists
        current_value = self.spo_index.get(subject)
        if current_value is None:
            return False

        # Verify value matches (for consistency)
        try:
            expected_value = int(obj)
            if current_value != expected_value:
                return False
        except Exception:
            return False

        # Remove from all indices
        del self.spo_index[subject]

        # Remove from OSP index
        if osp_set := self.osp_index.get(current_value):
            osp_set.discard(subject)
            if not osp_set:
                del self.osp_index[current_value]

        # Remove from POS index
        if pos_set := self.pos_index.get(current_value):
            pos_set.discard(subject)
            if not pos_set:
                del self.pos_index[current_value]

        return True

    async def query_spo(self, subject: E) -> int:
        self._query_count += 1
        return self.spo_index.get(subject, 0)

    async def query_osp(self, obj: Any) -> Set[E]:
        self._query_count += 1
        # obj expected to be an int for counters
        try:
            v = int(obj)
        except Exception:
            return set()
        return self.osp_index.get(v, set()).copy()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query: Which subjects have this predicate+object combination?"""
        self._query_count += 1
        try:
            v = int(obj)
        except Exception:
            return set()
        return self.pos_index.get(v, set()).copy()

    def audit_performance(self) -> Dict[str, Any]:
        """Return performance metrics for CounterStore."""
        avg_value = (
            sum(self.spo_index.values()) / len(self.spo_index) if self.spo_index else 0
        )

        return {
            "total_operations": self._insert_count + self._query_count,
            "total_increments": self._total_increments,
            "counter_count": len(self.spo_index),
            "unique_values": len(self.osp_index),
            "avg_value": avg_value,
            "data_structure_stats": {
                "spo_entries": len(self.spo_index),
                "osp_entries": len(self.osp_index),
                "pos_entries": len(self.pos_index),
            },
        }


class MultiValueSetStore(SpecializedDataStructure):
    """Simple in-memory multivalue set store with reverse index."""

    def __init__(
        self,
        predicate: E,
        supports_osp: bool = True,
        supports_pos: bool = True,
        concurrency_limit: Optional[int] = None,
    ):
        super().__init__(predicate, supports_osp, supports_pos, concurrency_limit)
        # Plugin maintains its own indices - no composite keys needed
        # SPO index: Subject -> Set[Object]
        self.spo_index: Dict[E, Set[E]] = {}
        # OSP index: Object -> Set[Subject]
        self.osp_index: Dict[E, Set[E]] = {}
        # POS index: Object -> Set[Subject] (same as OSP for single predicate)
        self.pos_index: Dict[E, Set[E]] = {}

        # Performance tracking
        self._insert_count = 0
        self._query_count = 0

    async def insert(self, subject: E, obj: E) -> None:
        self._insert_count += 1

        # Add to SPO index
        spo_set = self.spo_index.setdefault(subject, set())
        if obj in spo_set:
            return  # Already exists, no need to update other indices
        spo_set.add(obj)

        # Add to OSP index
        self.osp_index.setdefault(obj, set()).add(subject)

        # Add to POS index
        self.pos_index.setdefault(obj, set()).add(subject)

    async def delete(self, subject: E, obj: Any) -> bool:
        """Delete specific object from subject's value set.

        Args:
            subject: Subject entity
            obj: Object to remove from set

        Returns:
            True if triple existed and was deleted, False otherwise
        """
        # Check if subject exists and has this object
        spo_set = self.spo_index.get(subject)
        if spo_set is None or obj not in spo_set:
            return False

        # Remove from SPO index
        spo_set.discard(obj)
        if not spo_set:
            del self.spo_index[subject]

        # Remove from OSP index
        if osp_set := self.osp_index.get(obj):
            osp_set.discard(subject)
            if not osp_set:
                del self.osp_index[obj]

        # Remove from POS index
        if pos_set := self.pos_index.get(obj):
            pos_set.discard(subject)
            if not pos_set:
                del self.pos_index[obj]

        return True

    async def query_spo(self, subject: E) -> Set[E]:
        self._query_count += 1
        return set(self.spo_index.get(subject, set()))

    async def query_osp(self, obj: Any) -> Set[E]:
        self._query_count += 1
        return self.osp_index.get(obj, set()).copy() if isinstance(obj, E) else set()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query: Which subjects have this predicate+object combination?"""
        self._query_count += 1
        return self.pos_index.get(obj, set()).copy() if isinstance(obj, E) else set()

    def audit_performance(self) -> Dict[str, Any]:
        """Return performance metrics for MultiValueSetStore."""
        total_values = sum(len(s) for s in self.spo_index.values())
        avg_set_size = total_values / len(self.spo_index) if self.spo_index else 0

        return {
            "total_operations": self._insert_count + self._query_count,
            "set_count": len(self.spo_index),
            "total_values": total_values,
            "avg_set_size": avg_set_size,
            "unique_objects": len(self.osp_index),
            "data_structure_stats": {
                "spo_entries": len(self.spo_index),
                "osp_entries": len(self.osp_index),
                "pos_entries": len(self.pos_index),
            },
        }


class PredicateRegistry:
    """Registry for predicate-specialized data structures.

    Supports CIDSem ontology format with term grammar validation:
    - Entities: E:<namespace>:<label>
    - Relations: R:<namespace>:<label>
    - Events: EV:<namespace>:<label>
    - Literals: L:<type>:<value>
    - Contexts: C:<namespace>:<label>

    Per Spec 13: Plugin Infrastructure with config-driven specialization.
    """

    def __init__(self):
        # predicate cid -> SpecializedDataStructure
        self._registry: Dict[E, SpecializedDataStructure] = {}
        # CIDSem integration
        self.predicate_to_cid: Dict[str, E] = {}  # Fully-qualified name -> CID mapping
        self.cid_to_predicate: Dict[E, str] = {}  # Reverse mapping
        self.cidsem_ontology: Dict[str, Dict] = {}  # CIDSem ontology entries
        self.performance_justifications: Dict[
            str, str
        ] = {}  # Predicate -> justification

        # Concurrency control semaphores (per Spec 13.4)
        self._semaphores: Dict[E, asyncio.Semaphore] = {}

    def register_counter(self, p: E) -> CounterStore:
        """Register a CounterStore for predicate p.

        Raises:
            PredicateLimitExceededError: If PREDICATE_LIMIT exceeded
        """
        if len(self._registry) >= PREDICATE_LIMIT:
            raise PredicateLimitExceededError(PREDICATE_LIMIT, len(self._registry) + 1)

        ds = CounterStore(p)
        self._registry[p] = ds

        # Create semaphore if concurrency limit specified
        if ds.concurrency_limit is not None:
            self._semaphores[p] = asyncio.Semaphore(ds.concurrency_limit)

        return ds

    def register_multivalue(self, p: E) -> MultiValueSetStore:
        """Register a MultiValueSetStore for predicate p.

        Raises:
            PredicateLimitExceededError: If PREDICATE_LIMIT exceeded
        """
        if len(self._registry) >= PREDICATE_LIMIT:
            raise PredicateLimitExceededError(PREDICATE_LIMIT, len(self._registry) + 1)

        ds = MultiValueSetStore(p)
        self._registry[p] = ds

        # Create semaphore if concurrency limit specified
        if ds.concurrency_limit is not None:
            self._semaphores[p] = asyncio.Semaphore(ds.concurrency_limit)

        return ds

    def get(self, p: E) -> Optional[SpecializedDataStructure]:
        return self._registry.get(p)

    def all_predicates(self) -> Tuple[E, ...]:
        return tuple(self._registry.keys())

    def register_cidsem_counter(
        self, predicate_name: str, justification: str = ""
    ) -> CounterStore:
        """Register counter store with CIDSem term validation.

        Args:
            predicate_name: Fully-qualified predicate (e.g., 'R:usr:ownedQuantity')
            justification: Performance justification per CIDSem discipline

        Raises:
            InvalidCIDSemTermError: If predicate_name invalid
            PredicateLimitExceededError: If PREDICATE_LIMIT exceeded

        Example:
            registry.register_cidsem_counter(
                "R:usr:ownedQuantity",
                "Enables O(1) quantity aggregation vs O(n) triple enumeration"
            )
        """
        # Strict validation with exceptions
        self._validate_cidsem_strict(predicate_name)

        predicate_cid = self._compute_predicate_cid(predicate_name)
        ds = self.register_counter(predicate_cid)

        # Track CIDSem metadata
        self.predicate_to_cid[predicate_name] = predicate_cid
        self.cid_to_predicate[predicate_cid] = predicate_name
        if justification:
            self.performance_justifications[predicate_name] = justification

        return ds

    def register_cidsem_multivalue(
        self, predicate_name: str, justification: str = ""
    ) -> MultiValueSetStore:
        """Register multivalue store with CIDSem term validation.

        Args:
            predicate_name: Fully-qualified predicate (e.g., 'R:usr:friendsWith')
            justification: Performance justification per CIDSem discipline

        Raises:
            InvalidCIDSemTermError: If predicate_name invalid
            PredicateLimitExceededError: If PREDICATE_LIMIT exceeded

        Example:
            registry.register_cidsem_multivalue(
                "R:usr:friendsWith",
                "Bidirectional social graph queries require specialized indexing"
            )
        """
        # Strict validation with exceptions
        self._validate_cidsem_strict(predicate_name)

        predicate_cid = self._compute_predicate_cid(predicate_name)
        ds = self.register_multivalue(predicate_cid)

        # Track CIDSem metadata
        self.predicate_to_cid[predicate_name] = predicate_cid
        self.cid_to_predicate[predicate_cid] = predicate_name
        if justification:
            self.performance_justifications[predicate_name] = justification

        return ds

    def _validate_cidsem_format(self, predicate_name: str) -> bool:
        """Validate CIDSem term grammar: kind:namespace:label format.

        Per Spec 14, valid CIDSem terms must:
        - Match pattern: kind:namespace:label
        - kind ∈ {E, R, EV, L, C}
        - namespace and label: alphanumeric, underscore, hyphen only

        Args:
            predicate_name: Term to validate (e.g., 'R:usr:ownedQuantity')

        Returns:
            bool: True if valid, False otherwise

        Raises:
            InvalidCIDSemTermError: If term is invalid (when strict=True in callers)
        """
        if not predicate_name:
            return False

        match = CIDSEM_TERM_PATTERN.match(predicate_name)
        return match is not None

    def _validate_cidsem_strict(self, predicate_name: str) -> None:
        """Strict validation that raises exceptions for invalid terms.

        Args:
            predicate_name: Term to validate

        Raises:
            InvalidCIDSemTermError: If term format is invalid
        """
        if not predicate_name:
            raise InvalidCIDSemTermError(predicate_name, "Empty term")

        if ":" not in predicate_name:
            raise InvalidCIDSemTermError(
                predicate_name,
                "Missing colon separators (expected kind:namespace:label)",
            )

        parts = predicate_name.split(":")
        if len(parts) < 3:
            raise InvalidCIDSemTermError(
                predicate_name,
                f"Insufficient components: got {len(parts)}, expected 3 (kind:namespace:label)",
            )

        if len(parts) > 3:
            raise InvalidCIDSemTermError(
                predicate_name,
                f"Too many components: got {len(parts)}, expected 3 (kind:namespace:label)",
            )

        kind, namespace, label = parts

        valid_kinds = {"E", "R", "EV", "L", "C"}
        if kind not in valid_kinds:
            raise InvalidCIDSemTermError(
                predicate_name,
                f"Invalid kind '{kind}'. Must be one of: {', '.join(sorted(valid_kinds))}",
            )

        if not namespace:
            raise InvalidCIDSemTermError(predicate_name, "Empty namespace")

        if not label:
            raise InvalidCIDSemTermError(predicate_name, "Empty label")

        # Check allowed characters
        allowed_pattern = re.compile(r"^[a-zA-Z0-9_-]+$")

        if not allowed_pattern.match(namespace):
            raise InvalidCIDSemTermError(
                predicate_name,
                f"Namespace '{namespace}' contains invalid characters (allowed: alphanumeric, underscore, hyphen)",
            )

        if not allowed_pattern.match(label):
            raise InvalidCIDSemTermError(
                predicate_name,
                f"Label '{label}' contains invalid characters (allowed: alphanumeric, underscore, hyphen)",
            )

    def _compute_predicate_cid(self, predicate_name: str) -> E:
        """Compute deterministic CID for a predicate name.

        Uses SHA-256 hash of the fully-qualified predicate name for deterministic
        CID generation. Compatible with CIDSem ontology format.
        """
        # Ensure consistent UTF-8 encoding for CIDSem international labels
        normalized_name = predicate_name.encode("utf-8")
        hash_bytes = hashlib.sha256(normalized_name).digest()
        high = int.from_bytes(hash_bytes[:8], "big")
        low = int.from_bytes(hash_bytes[8:16], "big")
        return E(high, low)

    def load_cidsem_ontology(self, ontology_data: Dict[str, Dict]):
        """Load CIDSem ontology specification.

        Args:
            ontology_data: Dictionary mapping predicate names to their specifications
        """
        self.cidsem_ontology.update(ontology_data)

        # Validate all entries follow CIDSem discipline
        for predicate, spec in ontology_data.items():
            if not self._validate_cidsem_format(predicate):
                raise ValueError(f"Invalid CIDSem format in ontology: {predicate}")

            if "justification" not in spec:
                print(f"Warning: No performance justification for {predicate}")

    def audit_ontology_discipline(self) -> Dict[str, list[str]]:
        """Audit adherence to CIDSem ontology growth discipline.

        Returns:
            Dictionary with 'violations' and 'warnings' lists
        """
        # Check for performance justification
        violations = [
            f"No performance justification: {predicate}"
            for predicate in self.predicate_to_cid.keys()
            if predicate not in self.performance_justifications
        ]
        warnings = []

        # Check ontology size (CIDSem discipline: keep constrained)
        if len(self._registry) >= 200:
            warnings.append(
                f"Ontology size ({len(self._registry)}) "
                "approaching performance degradation threshold"
            )

        return {"violations": violations, "warnings": warnings}

    def get_predicate_name(self, predicate_cid: E) -> Optional[str]:
        """Get CIDSem predicate name from CID."""
        return self.cid_to_predicate.get(predicate_cid)

    def get_predicate_cid(self, predicate_name: str) -> Optional[E]:
        """Get predicate CID from CIDSem name."""
        return self.predicate_to_cid.get(predicate_name)

    async def load_from_config(self, config: Dict[str, Any]) -> None:
        """Load predicate specializations from configuration.

        Per Spec 13.3: Configuration-driven plugin instantiation.

        Args:
            config: Configuration dict with format:
                {
                    "predicates": [
                        {
                            "name": "R:usr:ownedQuantity",
                            "plugin": "counter",
                            "config": {},
                            "justification": "O(1) aggregation"
                        },
                        ...
                    ]
                }

        Raises:
            ValueError: If config is invalid or plugin not found
        """
        from .plugins import create_default_registry

        plugin_registry = create_default_registry()

        predicates = config.get("predicates", [])
        for predicate_spec in predicates:
            name = predicate_spec["name"]
            plugin_name = predicate_spec["plugin"]
            plugin_config = predicate_spec.get("config", {})
            justification = predicate_spec.get("justification", "")

            # Validate CIDSem format
            if not self._validate_cidsem_format(name):
                raise ValueError(f"Invalid CIDSem format: {name}")

            # Compute predicate CID
            predicate_cid = self._compute_predicate_cid(name)

            # Create plugin instance via PluginRegistry
            plugin_instance = plugin_registry.create_instance(
                plugin_name, predicate_cid, plugin_config
            )

            # Register in this registry
            self._registry[predicate_cid] = plugin_instance
            self.predicate_to_cid[name] = predicate_cid
            self.cid_to_predicate[predicate_cid] = name
            if justification:
                self.performance_justifications[name] = justification

            # Create semaphore for concurrency control
            if plugin_instance.concurrency_limit:
                self._semaphores[predicate_cid] = asyncio.Semaphore(
                    plugin_instance.concurrency_limit
                )

    def unregister(self, predicate: E) -> Optional[SpecializedDataStructure]:
        """Unregister a predicate's specialized data structure.

        This is used for hot-reload when changing a predicate's plugin type.
        Returns the old data structure for data migration purposes.

        Args:
            predicate: Predicate CID to unregister

        Returns:
            The old SpecializedDataStructure if it existed, None otherwise
        """
        old_ds = self._registry.pop(predicate, None)

        # Clean up semaphore
        if predicate in self._semaphores:
            del self._semaphores[predicate]

        # Clean up CIDSem mappings
        if predicate_name := self.cid_to_predicate.pop(predicate, None):
            self.predicate_to_cid.pop(predicate_name, None)
            self.performance_justifications.pop(predicate_name, None)

        return old_ds

    async def migrate_data(
        self,
        predicate: E,
        old_ds: SpecializedDataStructure,
        new_ds: SpecializedDataStructure,
    ) -> Dict[str, int]:
        """Migrate data from old data structure to new one during hot-reload.

        Copies all data from old_ds into new_ds. Handles different DS types:
        - CounterStore → CounterStore: Direct copy
        - MultiValueSetStore → MultiValueSetStore: Direct copy
        - CounterStore → MultiValueSetStore: Convert int values to singleton sets
        - MultiValueSetStore → CounterStore: Count set sizes

        Args:
            predicate: Predicate CID being migrated
            old_ds: Old data structure to migrate from
            new_ds: New data structure to migrate to

        Returns:
            Statistics about migration: {'migrated': count, 'skipped': count}
        """
        stats = {"migrated": 0, "skipped": 0}

        # Determine DS types
        old_type = type(old_ds).__name__
        new_type = type(new_ds).__name__

        try:
            if (
                old_type == "CounterStore"
                and new_type == "CounterStore"
                and hasattr(old_ds, "spo_index")
                and hasattr(new_ds, "insert")
            ):
                # Direct copy: Subject → int
                for subject, value in old_ds.spo_index.items():  # type: ignore
                    await new_ds.insert(subject, value)
                    stats["migrated"] += 1

            elif (
                old_type == "MultiValueSetStore"
                and new_type == "MultiValueSetStore"
                and hasattr(old_ds, "spo_index")
                and hasattr(new_ds, "insert")
            ):
                # Direct copy: Subject → Set[E]
                for subject, obj_set in old_ds.spo_index.items():  # type: ignore
                    for obj in obj_set:
                        await new_ds.insert(subject, obj)
                        stats["migrated"] += 1

            elif (
                old_type == "CounterStore"
                and new_type == "MultiValueSetStore"
                and hasattr(old_ds, "spo_index")
                and hasattr(new_ds, "insert")
            ):
                # Convert counter to multivalue: treat int as object E
                for subject, value in old_ds.spo_index.items():  # type: ignore
                    # Store the count as a synthetic E value
                    # This is lossy but best-effort migration
                    obj_e = E.from_str(f"E:migration:count_{value}")
                    await new_ds.insert(subject, obj_e)
                    stats["migrated"] += 1

            elif (
                old_type == "MultiValueSetStore"
                and new_type == "CounterStore"
                and hasattr(old_ds, "spo_index")
                and hasattr(new_ds, "insert")
            ):
                # Convert multivalue to counter: count set size
                for subject, obj_set in old_ds.spo_index.items():  # type: ignore
                    count = len(obj_set)
                    await new_ds.insert(subject, count)
                    stats["migrated"] += 1

            else:
                # Unsupported migration path
                if hasattr(old_ds, "spo_index"):
                    stats["skipped"] = len(old_ds.spo_index)  # type: ignore

        except Exception as e:
            # Migration failed - log error but don't crash
            import warnings

            warnings.warn(
                f"Data migration failed for predicate {predicate}: {e}", UserWarning
            )

        return stats

    async def hot_reload_predicate(
        self, predicate: E, new_plugin_type: str, migrate_data: bool = True
    ) -> Dict[str, Any]:
        """Hot-reload a predicate with a new plugin type.

        Changes the specialized data structure for a predicate at runtime
        without requiring system restart. Optionally migrates existing data
        to the new structure.

        Args:
            predicate: Predicate CID to reload
            new_plugin_type: New plugin type ('counter' or 'multivalue')
            migrate_data: Whether to migrate existing data (default: True)

        Returns:
            Dictionary with reload statistics:
            - 'old_type': Previous plugin type
            - 'new_type': New plugin type
            - 'migration': Migration statistics (if migrate_data=True)
            - 'success': Boolean indicating success

        Example:
            # Change from counter to multivalue
            result = await registry.hot_reload_predicate(
                E.from_str("R:test:score"),
                "multivalue",
                migrate_data=True
            )
            print(f"Migrated {result['migration']['migrated']} entries")
        """
        # Get old data structure
        old_ds = self.get(predicate)
        old_type = type(old_ds).__name__ if old_ds else None

        # Unregister old
        self.unregister(predicate)

        # Register new plugin
        if new_plugin_type == "counter":
            new_ds = self.register_counter(predicate)
        elif new_plugin_type == "multivalue":
            new_ds = self.register_multivalue(predicate)
        else:
            raise ValueError(f"Unknown plugin type: {new_plugin_type}")

        # Migrate data if requested
        migration_stats = None
        if migrate_data and old_ds is not None:
            migration_stats = await self.migrate_data(predicate, old_ds, new_ds)

        return {
            "old_type": old_type,
            "new_type": type(new_ds).__name__,
            "migration": migration_stats,
            "success": True,
        }

    async def query_osp_parallel(
        self, obj: Any, subject_filter: Optional[Set[E]] = None
    ) -> AsyncIterator[Tuple[E, E, Any]]:
        """Query OSP pattern across all registered predicates in parallel.

        Per Spec 13.4: Parallel OSP queries with concurrency control.

        Args:
            obj: Object to search for
            subject_filter: Optional set of subjects to filter results

        Yields:
            Tuples of (subject, predicate, object) matching the OSP pattern
        """

        async def query_single_predicate(
            predicate: E, plugin: SpecializedDataStructure
        ) -> list[Tuple[E, E, Any]]:
            """Query single predicate with concurrency control."""
            if not plugin.supports_osp:
                return []

            # Apply concurrency limit if configured
            semaphore = self._semaphores.get(predicate)
            if semaphore:
                async with semaphore:
                    subjects = await plugin.query_osp(obj)
            else:
                subjects = await plugin.query_osp(obj)

            # Apply subject filter if provided
            if subject_filter:
                subjects = subjects & subject_filter

            return [(s, predicate, obj) for s in subjects]

        # Query all predicates in parallel
        tasks = [
            query_single_predicate(p, plugin) for p, plugin in self._registry.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Yield all results
        for result in results:
            if isinstance(result, Exception):
                # Log error but continue
                continue
            for triple in result:
                yield triple

    def audit_all_plugins(self) -> Dict[str, Any]:
        """Audit performance metrics across all registered plugins.

        Per Spec 13.4: System-wide performance monitoring.

        Returns:
            Dictionary with aggregated metrics and per-plugin breakdown
        """
        total_operations = 0
        total_data_points = 0
        plugin_metrics = {}

        for predicate_cid, plugin in self._registry.items():
            predicate_name = self.cid_to_predicate.get(
                predicate_cid, str(predicate_cid)
            )

            try:
                metrics = plugin.audit_performance()
                plugin_metrics[predicate_name] = metrics

                total_operations += metrics.get("total_operations", 0)
                total_data_points += metrics.get("counter_count", 0) or metrics.get(
                    "set_count", 0
                )
            except Exception as e:
                plugin_metrics[predicate_name] = {"error": str(e)}

        return {
            "summary": {
                "total_plugins": len(self._registry),
                "total_operations": total_operations,
                "total_data_points": total_data_points,
            },
            "plugins": plugin_metrics,
            "ontology_audit": self.audit_ontology_discipline(),
        }


# Example usage with CIDSem ontology format:
# registry = PredicateRegistry()
#
# # Register CIDSem relations with performance justifications
# quantity_store = registry.register_cidsem_counter(
#     "R:usr:ownedQuantity",
#     "Enables O(1) quantity aggregation vs O(n) triple enumeration"
# )
#
# friends_store = registry.register_cidsem_multivalue(
#     "R:usr:friendsWith",
#     "Bidirectional social graph queries require specialized indexing"
# )
#
# # CIDSem entities
# alice = E.from_str("E:usr:alice")
# bob = E.from_str("E:usr:bob")
#
# # Insert data using specialized stores
# await quantity_store.insert(alice, 5)  # Alice owns 5 items
# await friends_store.insert(alice, bob)  # Alice friends with Bob
#
# # Query using CIDSem predicates
# alice_quantity = await quantity_store.query_spo(alice)
# alice_friends = await friends_store.query_spo(alice)
#
# # OSP queries: Who owns 5 items? Who is friends with Bob?
# owners_of_5 = await quantity_store.query_osp(5)
# friends_of_bob = await friends_store.query_osp(bob)
#
# # Audit ontology discipline
# audit = registry.audit_ontology_discipline()
# if audit['violations']:
#     print(f"Ontology violations: {audit['violations']}")
