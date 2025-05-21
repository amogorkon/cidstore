import io

import pytest

from cidstore.store import CIDStore


@pytest.fixture
def tree():
    # Use an in-memory HDF5 file via BytesIO and StorageManager
    f = io.BytesIO()
    from cidstore.storage import Storage
    from cidstore.wal import WAL

    storage = Storage(f)
    wal = WAL(None)  # In-memory WAL, not HDF5-backed
    return CIDStore(storage, wal=wal)


@pytest.fixture
def directory(tree):
    return tree


@pytest.fixture
def bucket(tree):
    return tree
