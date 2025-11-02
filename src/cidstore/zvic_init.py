"""ZVIC integration for CIDStore runtime contract enforcement.

This module enables ZVIC (Zero-Version Interface Contracts) runtime checking
throughout the CIDStore codebase. When assertions are enabled (development mode),
ZVIC validates function signatures, types, and constraints at runtime.

Per ZVIC paradigm:
- Contracts are enforced via assert blocks (removed in production with -O)
- Type and constraint violations are caught immediately
- Interface stability is verified dynamically

Usage:
    Call enforce_contracts() early in module initialization to enable
    ZVIC checking for that module's public callables.
"""

from __future__ import annotations

import os

# Check if ZVIC should be active
ZVIC_ENABLED = (
    __debug__  # True unless running with python -O
    and os.getenv("CIDSTORE_ZVIC_ENABLED", "1") != "0"
)


def enforce_contracts(module_name: str | None = None) -> None:
    """Enable ZVIC runtime contract enforcement for a module.

    This must be called from within the module you want to constrain.
    Only active when __debug__ is True (assertions enabled).

    Args:
        module_name: Module name to constrain. If None, uses caller's module.

    Example:
        # At end of your module:
        from cidstore.zvic_init import enforce_contracts
        enforce_contracts()
    """
    if not ZVIC_ENABLED:
        return

    try:
        from zvic import constrain_this_module

        # constrain_this_module() automatically constrains the caller's module
        constrain_this_module()

    except ImportError:
        # ZVIC not available, silently skip
        pass
    except Exception as e:
        # Log but don't crash on ZVIC errors
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(f"ZVIC constraint enforcement failed: {e}")


def get_zvic_info() -> dict[str, bool | str]:
    """Get ZVIC integration status information.

    Returns:
        Dict with ZVIC status: enabled, debug_mode, env_override
    """
    return {
        "enabled": ZVIC_ENABLED,
        "debug_mode": __debug__,
        "env_override": os.getenv("CIDSTORE_ZVIC_ENABLED", "1"),
        "zvic_available": _check_zvic_available(),
    }


def _check_zvic_available() -> bool:
    """Check if ZVIC is importable."""
    try:
        import importlib.util

        return importlib.util.find_spec("zvic") is not None
    except (ImportError, ValueError):
        return False
