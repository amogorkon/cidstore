import io

import pytest
import pytest_asyncio

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager, WALAnalyzer
from cidstore.store import CIDStore


@pytest_asyncio.fixture
async def store():
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




@pytest.fixture
def wal_analyzer(store):
    config = getattr(store, "config", None) or MaintenanceConfig()
    analyzer = WALAnalyzer(store, config)
    try:
        yield analyzer
    finally:
        analyzer.stop()
        analyzer.join()


@pytest.fixture
def maintenance_manager(store):
    config = getattr(store, "config", None) or MaintenanceConfig()
    manager = MaintenanceManager(store, config)
    try:
        yield manager
    finally:
        manager.stop()
        # Threads are joined in manager.stop()
