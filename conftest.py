import io
import sys
import os

import pytest
import asyncio

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager, WALAnalyzer
from cidstore.store import CIDStore
import inspect


@pytest.fixture
def store():
    # Provide a synchronous fixture that initializes the async components
    # via asyncio.run so tests do not require pytest-asyncio plugin.
    f = io.BytesIO()
    from cidstore.storage import Storage
    from cidstore.wal import WAL

    storage = Storage(f)
    wal = WAL(None)  # In-memory WAL
    store = CIDStore(storage, wal=wal, testing=True)

    # Run async_init synchronously for tests
    asyncio.run(store.async_init())
    try:
        yield store
    finally:
        # Close async resources synchronously
        try:
            asyncio.run(store.aclose())
        except Exception:
            # fallback to close
            store.close()


def pytest_pyfunc_call(pyfuncitem):
    """
    Minimal compatibility layer for async test functions when pytest-asyncio
    is not available: run coroutine test functions using a fresh event loop.
    """
    testfunction = pyfuncitem.obj
    if inspect.iscoroutinefunction(testfunction):
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(testfunction(**pyfuncitem.funcargs))
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()
        return True
    return None




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
