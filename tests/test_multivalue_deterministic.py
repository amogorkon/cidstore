import asyncio
import io

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


def test_multivalue_deterministic():
    async def inner():
        storage = Storage(io.BytesIO())
        wal = WAL(None)  # in-memory WAL
        store = CIDStore(storage, wal=wal, testing=True)
        await store.async_init()
        try:
            key = E.from_str("test_key")

            # Insert multiple NON-ZERO values for the same key (avoid encoded-empty marker 0)
            inserted = list(range(1, 6))
            for i in inserted:
                await store.insert(key, E.from_int(i))

                values = await store.get(key)
                ints = [int(v) for v in values]
                assert all(v != 0 for v in ints), "No stored value should be 0"
                # Each inserted value should be present in the read set after insertion
                assert i in ints

            final_values = await store.get(key)
            final_ints = [int(v) for v in final_values]
            assert set(final_ints) == set(inserted)

        finally:
            await store.aclose()

    asyncio.run(inner())
