"""Smoke test: load a cidstore module via the local ZVIC src and print canonical form.

This script intentionally does not install ZVIC; it adds the local `ZVIC/src` to
`sys.path` so the package can be imported in-place for testing.
"""

from pathlib import Path
import sys
import pprint
import traceback


def main():
    repo_root = Path(__file__).parent
    zvic_src = Path(r"e:\Dropbox\code\ZVIC\src")
    if not zvic_src.exists():
        print(f"ZVIC source not found at {zvic_src!s}")
        sys.exit(2)
    # Prefer local ZVIC src on sys.path
    sys.path.insert(0, str(zvic_src))

    try:
        import zvic
    except Exception:
        print("Failed to import zvic from local source:")
        traceback.print_exc()
        sys.exit(3)

    from zvic.main import load_module, canonicalize

    # Target module file: use cidstore/src/cidstore/utils.py (low side-effect)
    target = repo_root / "src" / "cidstore" / "utils.py"
    if not target.exists():
        print(f"Target module not found: {target}")
        sys.exit(4)

    try:
        mod = load_module(target, "cidstore_utils_for_zvic")
        print(f"Loaded module: {mod.__name__}")
        canonical = getattr(mod, "_zvic_canonical", None)
        if canonical is None:
            canonical = canonicalize(mod)
        print("Canonical representation:")
        pprint.pprint(canonical)
        # If the module defines the demo_constrained function, call it with a
        # value that violates the declared constraint to demonstrate fail-fast
        # runtime checking inserted by ZVIC.
        if hasattr(mod, "demo_constrained"):
            print("Calling demo_constrained(20) to observe constraint enforcement...")
            try:
                res = mod.demo_constrained(20)
                print("demo_constrained returned:", res)
            except AssertionError as ae:
                print("Constraint assertion raised as expected:")
                print(ae)
    except Exception as e:
        print("Compatibility/canonicalization failed:")
        traceback.print_exc()
        # If it's a SignatureIncompatible, try to print JSON form
        try:
            from zvic.exception import SignatureIncompatible

            if isinstance(e, SignatureIncompatible):
                print(e.to_json())
        except Exception:
            pass
        sys.exit(5)


if __name__ == "__main__":
    main()
