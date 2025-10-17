#!/usr/bin/env python
"""Wrapper script to run CIDStore with optimizations enabled.

This script automatically restarts Python with free-threading and JIT
enabled if they are not already active.

Usage:
    python cidstore_optimized.py [arguments]

Or make it executable:
    chmod +x cidstore_optimized.py
    ./cidstore_optimized.py [arguments]
"""

import os
import subprocess
import sys


def is_optimized():
    """Check if optimizations are already enabled."""
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    jit_enabled = hasattr(sys, "_is_jit_enabled") and sys._is_jit_enabled()
    return not gil_enabled and jit_enabled


def main():
    """Run with optimizations, restarting if necessary."""
    # Check if we're already optimized
    if is_optimized():
        print("✓ Running with free-threading and JIT enabled")
        # Import and run your main application here
        # For example:
        # from cidstore.cli import main as cli_main
        # cli_main()
        return 0

    # Check if we're on Python 3.13+
    if sys.version_info < (3, 13):
        print("ERROR: Python 3.13+ required for optimizations", file=sys.stderr)
        return 1

    # Restart with optimizations
    print("Restarting with optimizations enabled...")
    env = os.environ.copy()
    env["PYTHON_GIL"] = "0"
    env["PYTHON_JIT"] = "1"

    # Restart Python with the same script and arguments
    args = [sys.executable, "-X", "gil=0", "-X", "jit=1"] + sys.argv
    try:
        result = subprocess.run(args, env=env)
        return result.returncode
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
