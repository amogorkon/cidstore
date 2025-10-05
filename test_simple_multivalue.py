#!/usr/bin/env python3

"""
Simple test to check if multi-value insertion works in single-threaded mode
"""

import asyncio
import io

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


async def test_simple_multivalue():
    # Create in-memory store
    storage = Storage(io.BytesIO())
    wal = WAL(None)  # In-memory WAL
    store = CIDStore(storage, wal=wal, testing=True)

    await store.async_init()

    try:
        key = E.from_str("test_key")

        print("=== Testing single-threaded multi-value insertion ===")

        # Insert multiple NON-ZERO values for the same key (avoid encoded-empty marker 0)
        for i in range(1, 6):
            print(f"Inserting value {i} for key 'test_key'")
            await store.insert(key, E.from_int(i))

            # Check what we can get back immediately
            values = await store.get(key)
            print(f"After insert {i}: get() returned {[int(v) for v in values]}")

        print("\n=== Final check ===")
        final_values = await store.get(key)
        final_ints = [int(v) for v in final_values]
        print(f"Final values: {final_ints}")
        print(f"Expected: {list(range(1, 6))}")

        if set(final_ints) == set(range(1, 6)):
            print("✓ Multi-value insertion works correctly!")
            return True
        else:
            print("✗ Multi-value insertion failed!")
            return False

    finally:
        await store.aclose()


if __name__ == "__main__":
    result = asyncio.run(test_simple_multivalue())
    exit(0 if result else 1)
