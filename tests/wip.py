"""WIP tests."""

import inspect
import io

# ===========================================
from cidtree.keys import E
from cidtree.storage import Storage
from cidtree.tree import CIDTree
from cidtree.wal import WAL


def test_standard_workflow():
    storage = Storage(path=io.BytesIO())
    wal = WAL(None)
    tree = CIDTree(storage, wal)

    k_multi = E.from_str("multi")
    tree.insert(k_multi, E(1))
    for i in range(1, 151):
        v = E(i)
        tree.insert(k_multi, v)
    values = list(tree.get(k_multi))
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
