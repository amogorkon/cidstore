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
    tree = CIDStore(storage, wal)

    k = E.from_str("multi")
    a, b, c = E(1), E(2), E(3)
    await tree.insert(k, a)
    print(list(await tree.get(k)))
    await tree.insert(k, b)
    print(list(await tree.get(k)))
    await tree.insert(k, c)
    print(list(await tree.get(k)))
    values = list(await tree.get(k))
    assert values == [E(1), E(2), E(3)]

    tree.hdf.close()
    storage.close()


# ===========================================


if __name__ == "__main__":
    for name, func in globals().copy().items():
        if name.startswith("test_"):
            print(f" ↓↓↓↓↓↓↓ {name} ↓↓↓↓↓↓")
            print(inspect.getsource(func))
            if inspect.iscoroutinefunction(func):
                asyncio.run(func())
            else:
                func()
            print(f"↑↑↑↑↑↑ {name} ↑↑↑↑↑↑")
            print()
