from __future__ import annotations

from typing import Any


def sum_ints(a: int, b: int) -> int:
    """Sum two non-negative integers (used for ZVIC enforcement testing)"""
    return a + b


def assumption(obj: Any, *expected: type) -> bool:
    """Check against multiple possible types"""
    for exp in expected:
        if isinstance(obj, exp):
            return True
    # If no expected type matched, raise an assertion to preserve previous behavior
    _raise_assert(obj, expected)  # type: ignore
    return False


def _raise_assert(obj: Any, expected: tuple[type, ...]) -> bool:
    if len(expected) == 1:  # type: ignore
        msg = f"Expected {expected}, instead got {type(obj).__name__} (value: {obj})"
    else:
        msg = msg = (
            f"Expected one of {expected}, instead got {type(obj).__name__} (value: {obj})"
        )
    raise AssertionError(msg)
