"""WIP. This is where code is tested as it is developed. It is not a test, but a scratchpad for testing code snippets and ideas. It is not meant to be run as a test suite, but rather as a playground for experimentation."""

import asyncio
import inspect

# ===========================================
from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


async def test_standard_workflow():
    storage = Storage(None)
    wal = WAL(None)
    store = CIDStore(storage, wal)
    await store.async_init()

    k = E.from_str("multi")
    a, b, c = E(1), E(2), E(3)
    await store.insert(k, a)
    await asyncio.sleep(0.1)
    print(list(await store.get(k)))
    await store.insert(k, b)
    print(list(await store.get(k)))
    await store.insert(k, c)
    print(list(await store.get(k)))
    values = list(await store.get(k))
    assert values == [E(1), E(2), E(3)]
    await store.close()


# ===========================================


if __name__ == "__main__":
    for name, func in globals().copy().items():
        if name.startswith("test_"):
            print(f" ↓↓↓↓↓↓↓ {name} ↓↓↓↓↓↓")
            print(inspect.getsource(func))
            print("----------------------------")
            if inspect.iscoroutinefunction(func):
                asyncio.run(func())
            else:
                func()
            print(f"↑↑↑↑↑↑ {name} ↑↑↑↑↑↑")
            print()
