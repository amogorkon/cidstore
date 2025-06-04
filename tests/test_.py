"""Core tests to quickly check the basic functionality of the library on every edit."""

import pytest

from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


@pytest.mark.asyncio
async def test_standard_workflow():
    storage = Storage(None)
    wal = WAL(None)
    store = CIDStore(storage, wal, testing=True)
    await store.async_init()

    k = E.from_str("multi")
    print(k)
    a, b, c, d = E(1), E(2), E(3), E(4)
    await store.insert(k, a)
    print(list(await store.get(k)))
    await store.insert(k, b)
    print(list(await store.get(k)))
    await store.insert(k, c)
    print(list(await store.get(k)))
    await store.insert(k, d)
    print(list(await store.get(k)))
    values = list(await store.get(k))
    assert values == [E(1), E(2), E(3), E(4)], values


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
