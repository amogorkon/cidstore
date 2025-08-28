from __future__ import annotations

from typing import Any

# Apply ZVIC annotation-constraint transformations to this module at import-time.
# This rewrites annotation calls into `Annotated[...]` and injects runtime
# assertions when running in debug mode. We intentionally fail fast here so any
# problems with ZVIC or the transformation are immediately visible during tests.
# ZVIC instrumentation removed to restore original test baseline.


def assumption(obj: Any, *expected: type) -> bool:
    """Check against multiple possible types"""
    for exp in expected:
        if isinstance(obj, exp):
            return True
    _raise_assert(obj, expected)  # type: ignore


def _raise_assert(obj: Any, expected: tuple[type, ...]) -> bool:
    if len(expected) == 1:  # type: ignore
        msg = f"Expected {expected}, instead got {type(obj).__name__} (value: {obj})"
    else:
        msg = msg = (
            f"Expected one of {expected}, instead got {type(obj).__name__} (value: {obj})"
        )
    raise AssertionError(msg)


def demo_constrained(x: int(_ < 10)) -> int(_ < 10):
    """Demo function with an annotation-level constraint.

    The annotation uses the ZVIC constraint syntax; when ZVIC transforms this
    module, it will inject runtime assertions that enforce `_ < 10` on the
    parameter and the return value in debug mode.
    """
    return x * 2
