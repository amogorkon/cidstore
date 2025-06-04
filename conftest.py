import io

import pytest
import pytest_asyncio

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager, WALAnalyzer
from cidstore.store import CIDStore


@pytest_asyncio.fixture
async def tree():
    # Use an in-memory HDF5 file via BytesIO and StorageManager
    f = io.BytesIO()
    from cidstore.storage import Storage
    from cidstore.wal import WAL

    storage = Storage(f)
    wal = WAL(None)  # In-memory WAL, not HDF5-backed
    store = CIDStore(storage, wal=wal)
    await store.async_init()
    try:
        yield store
    finally:
        await store.aclose()


@pytest_asyncio.fixture
async def directory(tree):
    return tree


@pytest_asyncio.fixture
async def bucket(tree):
    return tree


@pytest.fixture
def wal_analyzer(tree):
    config = getattr(tree, "config", None) or MaintenanceConfig()
    analyzer = WALAnalyzer(tree, config)
    try:
        yield analyzer
    finally:
        analyzer.stop()
        analyzer.join()


@pytest.fixture
def maintenance_manager(tree):
    config = getattr(tree, "config", None) or MaintenanceConfig()
    manager = MaintenanceManager(tree, config)
    try:
        yield manager
    finally:
        manager.stop()
        # Threads are joined in manager.stop()
