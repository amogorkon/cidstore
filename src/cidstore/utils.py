from typing import Any


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
