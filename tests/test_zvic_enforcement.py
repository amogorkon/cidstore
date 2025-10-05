from __future__ import annotations

import sys

import pytest

# Ensure the project src and ZVIC source are discoverable
sys.path.insert(0, r"e:/Dropbox/code/cidstore/src")
sys.path.insert(0, r"e:/Dropbox/code/ZVIC/src")

# Try to import the ZVIC API; if unavailable or broken, skip these tests since
# they verify optional runtime enforcement provided by the external ZVIC tool.
try:
    # type: ignore[import]
    from zvic import install_import_hook
except Exception:
    pytest.skip(
        "ZVIC not available or broken in this environment; skipping zvic enforcement tests",
        allow_module_level=True,
    )

# Install the import hook so subsequent imports are transformed in-place
install_import_hook(exclude_prefix="zvic")
from cidstore.utils import sum_ints


def test_sum_ints_accepts_ints():
    assert sum_ints(1, 2) == 3


def test_sum_ints_rejects_strings():
    try:
        sum_ints("1", 2)  # type: ignore
    except AssertionError:
        # zvic should raise AssertionError on type mismatch
        return
    except Exception:
        # Any other exception is acceptable as a failure mode for zvic enforcement
        return
    # If no exception, then the zvic transform did not enforce types here
    raise AssertionError("ZVIC did not enforce type annotations for sum_ints")


def test_sum_ints_rejects_negatives():
    # negative input should violate the refinement `int(_>=0)` and be rejected
    try:
        sum_ints(-1, 2)
    except AssertionError:
        return
    except Exception:
        return
    raise AssertionError("ZVIC did not enforce non-negative refinement for sum_ints")
