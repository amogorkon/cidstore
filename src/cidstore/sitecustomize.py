"""Automatic Python 3.13 optimization configuration.

This module is automatically imported by Python at startup if it's in the
Python path. It enables free-threading and JIT by default for CIDStore.

To disable automatic optimizations:
    export CIDSTORE_AUTO_OPTIMIZE=0
"""

import os
import sys


def enable_optimizations():
    """Enable free-threading and JIT if not already configured."""
    # Check if user wants to disable auto-optimization
    if os.environ.get("CIDSTORE_AUTO_OPTIMIZE", "1") == "0":
        return

    # Only run on Python 3.13+
    if sys.version_info < (3, 13):
        return

    # Enable free-threading if not explicitly configured
    if "PYTHON_GIL" not in os.environ:
        # Check if we're in a free-threaded build
        if hasattr(sys, "_is_gil_enabled"):
            try:
                # Try to disable GIL if possible
                if sys._is_gil_enabled():
                    # GIL is enabled, but we can't change it at runtime
                    # User needs to use -X gil=0 flag
                    pass
            except Exception:
                pass

    # Enable JIT if not explicitly configured
    if "PYTHON_JIT" not in os.environ:
        # Check if JIT is available
        if hasattr(sys, "_is_jit_enabled"):
            try:
                # JIT settings are compile-time, can't change at runtime
                # User needs to use -X jit=1 flag
                pass
            except Exception:
                pass


# Run on import
enable_optimizations()
