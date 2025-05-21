"""WIP tests."""

import inspect
import io

# ===========================================
from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


async def test_standard_workflow():
    storage = Storage(path=io.BytesIO())
    wal = WAL(None)
    tree = CIDStore(storage, wal)

    k_multi = E.from_str("multi")
    await tree.insert(k_multi, E(1))
    for i in range(1, 151):
        v = E(i)
        await tree.insert(k_multi, v)
    values = await tree.get(k_multi)
    assert values == [E(i) for i in range(1, 151)]

    tree.hdf.close()
    storage.close()


# ===========================================

if __name__ == "__main__":
    for name, func in globals().copy().items():
        if name.startswith("test_"):
            print(f" ↓↓↓↓↓↓↓ {name} ↓↓↓↓↓↓")
            print(inspect.getsource(func))
            func()
            print(f"↑↑↑↑↑↑ {name} ↑↑↑↑↑↑")
            print()
