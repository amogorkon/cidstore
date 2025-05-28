import pytest

from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL


@pytest.fixture
async def cidstore(tmp_path):
    f = tmp_path / "test.h5"
    storage = Storage(str(f))
    wal = WAL(None)
    store = CIDStore(storage, wal=wal)
    # If async init is needed, call it here
    if hasattr(store, "async_init"):
        await store.async_init()
    yield store


@pytest.fixture
async def directory(cidstore):
    return cidstore


@pytest.fixture
async def bucket(cidstore):
    return cidstore
