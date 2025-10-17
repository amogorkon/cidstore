"""Python 3.13 Runtime Configuration for CIDStore

This module configures Python 3.13 enhancements:
- Free-threaded CPython (PEP 703): Enable true parallelism
- JIT Compiler (PEP 744): Enable experimental JIT for performance

To run with free-threading:
    python -X gil=0 -m cidstore.cli

To run with JIT enabled:
    python -X jit=1 -m cidstore.cli

To run with both:
    python -X gil=0 -X jit=1 -m cidstore.cli

Environment variables:
    PYTHON_GIL=0          # Disable GIL for free-threading
    PYTHON_JIT=1          # Enable JIT compilation
"""

import sys

# Check Python version
if sys.version_info < (3, 13):
    raise RuntimeError(
        f"CIDStore requires Python 3.13+. Current version: {sys.version_info.major}.{sys.version_info.minor}"
    )


# Check for free-threading support
def is_free_threaded():
    """Check if Python is running with free-threading enabled."""
    return getattr(sys, "_is_gil_enabled", lambda: True)() is False


def supports_free_threading():
    """Check if this Python build supports free-threading."""
    return hasattr(sys, "_is_gil_enabled")


# Check for JIT support
def is_jit_enabled():
    """Check if JIT compilation is enabled."""
    return hasattr(sys, "_is_jit_enabled") and sys._is_jit_enabled()


def supports_jit():
    """Check if this Python build supports JIT."""
    return hasattr(sys, "_is_jit_enabled")


# Report configuration at import time
if __name__ == "__main__":
    print(f"Python version: {sys.version}")

    # Check build support
    gil_support = supports_free_threading()
    jit_support = supports_jit()

    print(f"Build supports free-threading: {gil_support}")
    print(f"Build supports JIT: {jit_support}")
    print()

    # Check runtime status
    if gil_support:
        print(f"Free-threading (GIL disabled): {is_free_threaded()}")
        if not is_free_threaded():
            print("\n⚠️  Free-threading is NOT enabled.")
            print("   Run with: python -X gil=0 your_script.py")
            print("   Or set: export PYTHON_GIL=0")
    else:
        print("Free-threading: NOT SUPPORTED by this build")
        print("\n⚠️  This Python was not built with --disable-gil support.")
        print("   To enable: rebuild Python from source:")
        print("   ./configure --disable-gil")
        print("   make && make install")

    print()

    if jit_support:
        print(f"JIT enabled: {is_jit_enabled()}")
        if not is_jit_enabled():
            print("\n⚠️  JIT is NOT enabled.")
            print("   Run with: python -X jit=1 your_script.py")
            print("   Or set: export PYTHON_JIT=1")
    else:
        print("JIT: NOT SUPPORTED by this build")
        print("\nℹ️  JIT support is experimental in Python 3.13")

    if gil_support and jit_support and is_free_threaded() and is_jit_enabled():
        print("\n✅ All Python 3.13 enhancements are active!")
