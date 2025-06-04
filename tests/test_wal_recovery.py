import pytest
from pathlib import Path
from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL

@pytest.mark.asyncio
async def test_wal_recovery(tmp_path):
    # Use file-backed WAL for persistence
    file = Path(tmp_path) / "walrec.h5"
    walfile = Path(tmp_path) / "walrec.wal"
    storage1 = Storage(path=file)
    wal1 = WAL(path=walfile)
    tree1 = CIDStore(storage1, wal1)
    k = E.from_str("recov")
    await tree1.insert(k, E(1))
    if storage1.file:
        storage1.file.close()
    wal1.close()

    # Reopen storage and tree -> WAL should replay
    storage2 = Storage(path=file)
    wal2 = WAL(path=walfile)
    tree2 = CIDStore(storage2, wal2)
    result = await tree2.get(k)
    assert list(result) == [E(1)]
    storage2.close()
    wal2.close()
