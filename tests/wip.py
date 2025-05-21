"""WIP tests."""

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
    await tree.insert(k, E(1))
    print(await tree.get(k))
    await tree.insert(k, E(2))
    print(await tree.get(k))
    await tree.insert(k, E(3))
    print(await tree.get(k))
    values = await list(tree.get(k))
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
