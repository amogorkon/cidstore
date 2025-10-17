"""exceptions.py - Custom exception hierarchy for CIDStore triple operations.

Defines exceptions for:
- Plugin configuration and registration errors
- CIDSem term validation failures
- Predicate specialization errors
- Query and triple operation errors

Per Spec 14 (CIDSem validation) and error handling standards.
"""

from __future__ import annotations


class CIDStoreError(Exception):
    """Base exception for all CIDStore errors."""

    pass


class PluginError(CIDStoreError):
    """Base exception for plugin-related errors."""

    pass


class PluginConfigError(PluginError):
    """Raised when plugin configuration is invalid or missing required fields.

    Examples:
        - Missing required config keys
        - Invalid plugin class name
        - Config type mismatch (expected dict, got str)
    """

    pass


class PluginRegistrationError(PluginError):
    """Raised when plugin registration fails.

    Examples:
        - Duplicate plugin name
        - Plugin class does not implement required interface
        - Attempting to register non-callable as plugin
    """

    pass


class PluginNotFoundError(PluginError):
    """Raised when attempting to use an unregistered plugin."""

    pass


class CIDSemError(CIDStoreError):
    """Base exception for CIDSem term validation errors."""

    pass


class InvalidCIDSemTermError(CIDSemError):
    """Raised when a CIDSem term does not match required grammar: kind:namespace:label.

    Per Spec 14, valid terms must:
    - Have exactly 3 colon-separated components
    - Non-empty kind, namespace, and label
    - Use only allowed characters (alphanumeric, underscore, hyphen)
    """

    def __init__(self, term: str, reason: str):
        self.term = term
        self.reason = reason
        super().__init__(f"Invalid CIDSem term '{term}': {reason}")


class PredicateLimitExceededError(CIDSemError):
    """Raised when attempting to register more than ~200 predicates.

    Per Spec 14, the system enforces a predicate limit to ensure
    performance and manageable complexity.
    """

    def __init__(self, limit: int, attempted: int):
        self.limit = limit
        self.attempted = attempted
        super().__init__(
            f"Predicate limit exceeded: {attempted} predicates registered, limit is {limit}"
        )


class PredicateError(CIDStoreError):
    """Base exception for predicate-related errors."""

    pass


class PredicateNotSpecializedException(PredicateError):
    """Raised when a query requires a specialized predicate but none is registered.

    Indicates that the predicate is not handled by a plugin and must
    fall back to composite key system.
    """

    def __init__(self, predicate):
        self.predicate = predicate
        super().__init__(
            f"Predicate {predicate} is not specialized - no plugin registered"
        )


class UnsupportedQueryPatternError(PredicateError):
    """Raised when a query pattern is not supported by the specialized data structure.

    Example: Attempting OSP query on a plugin that only supports SPO.
    """

    def __init__(self, predicate, pattern: str, supported_patterns: list[str]):
        self.predicate = predicate
        self.pattern = pattern
        self.supported_patterns = supported_patterns
        super().__init__(
            f"Predicate {predicate} does not support {pattern} queries. "
            f"Supported patterns: {', '.join(supported_patterns)}"
        )


class TripleOperationError(CIDStoreError):
    """Base exception for triple insert/delete/query errors."""

    pass


class InvalidTripleError(TripleOperationError):
    """Raised when a triple has invalid components (S, P, O).

    Examples:
        - Subject is not an E instance
        - Predicate is not an E instance
        - Object type not compatible with predicate's data structure
    """

    pass


class CompositeKeyError(CIDStoreError):
    """Raised when composite key encoding/decoding fails."""

    pass
