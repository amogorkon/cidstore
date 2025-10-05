from __future__ import annotations

from typing import Any


# Avoid importing `zvic` (and running its import-time module transform) by
# default during test runs. Import-time transforms have caused recursive
# import/exec loops in CI and tests. If you need ZvIC enforcement, call
# `constrain_this_module()` explicitly from a controlled entrypoint.
def constrain_this_module():
    """No-op placeholder for ZvIC runtime constraint injector.

    This keeps the module import side-effect free for tests and CI where
    ZvIC may not be available or its import-time transforms are unsafe.
    """
    return None


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


# Experiment: small API to verify ZVIC runtime enforcement without inline asserts.
# We intentionally do not perform any `assert` or explicit type checks here;
# the `constrain_this_module()` call at module import should transform this
# function and inject runtime checks based on the annotations.
def sum_ints(a: int, b: int) -> int:
    """Return sum of two integers.

    This is a simple, well-typed helper retained for tests and examples.
    It does not enforce ZvIC-style refinements at import time to avoid
    import-side-effects during test runs.
    """
    return a + b
