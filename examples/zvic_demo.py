"""Example demonstrating ZVIC contract enforcement in CIDStore.

This script shows how ZVIC catches incompatible interface changes at runtime.
Run without -O to see ZVIC in action.
"""

import sys
print(f"Python version: {sys.version}")
print(f"Debug mode (__debug__): {__debug__}")
print()

from cidstore.zvic_init import get_zvic_info

# Show ZVIC status
info = get_zvic_info()
print("ZVIC Status:")
for key, value in info.items():
    print(f"  {key}: {value}")
print()

if not info['enabled']:
    print("⚠️  ZVIC is disabled. To enable:")
    print("  - Remove -O flag from python command")
    print("  - Unset CIDSTORE_ZVIC_ENABLED if it's set to 0")
    sys.exit(0)

print("✅ ZVIC runtime enforcement is ACTIVE")
print()

# Example 1: Plugin Registry with ZVIC
print("Example 1: Plugin Registry Interface")
print("-" * 50)

from cidstore.plugins import create_default_registry
from cidstore.keys import E

registry = create_default_registry()
print(f"✓ Created registry with plugins: {registry.list_plugins()}")

# Create a counter plugin instance
pred = E.from_str("test:counter")
counter = registry.create_instance("counter", pred, {})
print(f"✓ Created counter instance: {counter}")
print()

# Example 2: E Key Interface with ZVIC
print("Example 2: E Key Interface")
print("-" * 50)

# E construction methods are validated by ZVIC
key1 = E.from_str("example:key")
print(f"✓ E.from_str('example:key') = {key1}")

key2 = E.from_int(12345)
print(f"✓ E.from_int(12345) = {key2}")

key3 = E(1, 2, 3, 4)
print(f"✓ E(1, 2, 3, 4) = {key3}")

# Properties are validated
print(f"✓ key1.high = {key1.high}")
print(f"✓ key1.high_mid = {key1.high_mid}")
print(f"✓ key1.low_mid = {key1.low_mid}")
print(f"✓ key1.low = {key1.low}")
print()

# Example 3: Composite Key Functions with ZVIC
print("Example 3: Composite Key Functions")
print("-" * 50)

from cidstore.keys import composite_key, composite_value

subject = E.from_str("person:alice")
predicate = E.from_str("rel:knows")
obj = E.from_str("person:bob")

comp_key = composite_key(subject, predicate, obj)
print(f"✓ composite_key(subject, predicate, obj) = {comp_key}")

comp_val = composite_value(predicate, obj)
print(f"✓ composite_value(predicate, obj) = {comp_val}")
print()

# Example 4: Storage Interface with ZVIC
print("Example 4: Storage Interface")
print("-" * 50)

import io
from cidstore.storage import Storage

# Storage init signature is validated
f = io.BytesIO()
storage = Storage(f)
print(f"✓ Created Storage with BytesIO: {storage}")
print(f"✓ Storage.file: {type(storage.file)}")

storage.close()
print("✓ Storage closed successfully")
print()

# Example 5: WAL Interface with ZVIC
print("Example 5: WAL Interface")
print("-" * 50)

from cidstore.wal import WAL

# WAL accepts None for in-memory mode
wal = WAL(None)
print(f"✓ Created in-memory WAL: {wal}")
print(f"✓ WAL path: {wal.path}")

wal.close()
print("✓ WAL closed successfully")
print()

print("=" * 50)
print("✅ All examples completed successfully!")
print("=" * 50)
print()
print("What ZVIC validated:")
print("  • Plugin registry interface contracts")
print("  • E key construction methods")
print("  • Composite key function signatures")
print("  • Storage initialization")
print("  • WAL initialization")
print()
print("If any interface changes incompatibly, ZVIC will")
print("raise a detailed exception showing what changed.")
print()
print("Try it:")
print("  1. Run: python -O examples/zvic_demo.py")
print("     (ZVIC disabled, zero overhead)")
print("  2. Run: python examples/zvic_demo.py")
print("     (ZVIC enabled, contracts validated)")
