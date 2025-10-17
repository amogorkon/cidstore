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
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Optional, Set, Tuple

from .keys import E


class SpecializedDataStructure:
    def __init__(self, predicate: E):
        self.predicate = predicate

    async def insert(self, subject: E, obj: Any) -> None:
        raise NotImplementedError()

    async def query_spo(self, subject: E) -> Any:
        raise NotImplementedError()

    async def query_osp(self, obj: Any) -> Set[E]:
        raise NotImplementedError()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query: get all subjects where (subject, predicate, obj) exists"""
        raise NotImplementedError()


class CounterStore(SpecializedDataStructure):
    """Simple in-memory counter store with reverse index for OSP."""

    def __init__(self, predicate: E):
        super().__init__(predicate)
        # Plugin maintains its own indices - no composite keys needed
        # SPO index: Subject -> int
        self.spo_index: Dict[E, int] = {}
        # OSP index: int -> Set[Subject]
        self.osp_index: Dict[int, Set[E]] = {}
        # POS index: int -> Set[Subject] (same as OSP for single predicate)
        self.pos_index: Dict[int, Set[E]] = {}

    async def insert(self, subject: E, value: int) -> None:
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
        self.spo_index[subject] = int(value)
        self.osp_index.setdefault(int(value), set()).add(subject)
        self.pos_index.setdefault(int(value), set()).add(subject)

    async def query_spo(self, subject: E) -> int:
        return self.spo_index.get(subject, 0)

    async def query_osp(self, obj: Any) -> Set[E]:
        # obj expected to be an int for counters
        try:
            v = int(obj)
        except Exception:
            return set()
        return self.osp_index.get(v, set()).copy()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query: Which subjects have this predicate+object combination?"""
        try:
            v = int(obj)
        except Exception:
            return set()
        return self.pos_index.get(v, set()).copy()


class MultiValueSetStore(SpecializedDataStructure):
    """Simple in-memory multivalue set store with reverse index."""

    def __init__(self, predicate: E):
        super().__init__(predicate)
        # Plugin maintains its own indices - no composite keys needed
        # SPO index: Subject -> Set[Object]
        self.spo_index: Dict[E, Set[E]] = {}
        # OSP index: Object -> Set[Subject]
        self.osp_index: Dict[E, Set[E]] = {}
        # POS index: Object -> Set[Subject] (same as OSP for single predicate)
        self.pos_index: Dict[E, Set[E]] = {}

    async def insert(self, subject: E, obj: E) -> None:
        # Add to SPO index
        spo_set = self.spo_index.setdefault(subject, set())
        if obj in spo_set:
            return  # Already exists, no need to update other indices
        spo_set.add(obj)

        # Add to OSP index
        self.osp_index.setdefault(obj, set()).add(subject)

        # Add to POS index
        self.pos_index.setdefault(obj, set()).add(subject)

    async def query_spo(self, subject: E) -> Set[E]:
        return set(self.spo_index.get(subject, set()))

    async def query_osp(self, obj: Any) -> Set[E]:
        if not isinstance(obj, E):
            return set()
        return self.osp_index.get(obj, set()).copy()

    async def query_pos(self, obj: Any) -> Set[E]:
        """POS query: Which subjects have this predicate+object combination?"""
        if not isinstance(obj, E):
            return set()
        return self.pos_index.get(obj, set()).copy()


class PredicateRegistry:
    """Registry for predicate-specialized data structures.

    Supports CIDSem ontology format with term grammar validation:
    - Entities: E:<namespace>:<label>
    - Relations: R:<namespace>:<label>
    - Events: EV:<namespace>:<label>
    - Literals: L:<type>:<value>
    - Contexts: C:<namespace>:<label>
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

    def register_counter(self, p: E) -> CounterStore:
        ds = CounterStore(p)
        self._registry[p] = ds
        return ds

    def register_multivalue(self, p: E) -> MultiValueSetStore:
        ds = MultiValueSetStore(p)
        self._registry[p] = ds
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

        Example:
            registry.register_cidsem_counter(
                "R:usr:ownedQuantity",
                "Enables O(1) quantity aggregation vs O(n) triple enumeration"
            )
        """
        if not self._validate_cidsem_format(predicate_name):
            raise ValueError(
                f"Invalid CIDSem format: {predicate_name}. "
                "Must use format 'kind:namespace:label' with exactly two colons."
            )

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

        Example:
            registry.register_cidsem_multivalue(
                "R:usr:friendsWith",
                "Bidirectional social graph queries require specialized indexing"
            )
        """
        if not self._validate_cidsem_format(predicate_name):
            raise ValueError(
                f"Invalid CIDSem format: {predicate_name}. "
                "Must use format 'kind:namespace:label' with exactly two colons."
            )

        predicate_cid = self._compute_predicate_cid(predicate_name)
        ds = self.register_multivalue(predicate_cid)

        # Track CIDSem metadata
        self.predicate_to_cid[predicate_name] = predicate_cid
        self.cid_to_predicate[predicate_cid] = predicate_name
        if justification:
            self.performance_justifications[predicate_name] = justification

        return ds

    def _validate_cidsem_format(self, predicate_name: str) -> bool:
        """Validate CIDSem term grammar: kind:namespace:label format."""
        parts = predicate_name.split(":")
        if len(parts) < 3:
            return False

        kind = parts[0]
        # Valid kinds: E (Entity), R (Relation), EV (Event), L (Literal), C (Context)
        valid_kinds = {"E", "R", "EV", "L", "C"}

        return kind in valid_kinds

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
        violations = []
        warnings = []

        for predicate in self.predicate_to_cid.keys():
            # Check for performance justification
            if predicate not in self.performance_justifications:
                violations.append(f"No performance justification: {predicate}")

        # Check ontology size (CIDSem discipline: keep constrained)
        if len(self._registry) > 200:
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
