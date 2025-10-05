"""Module `cidstore.main` - synchronous test-friendly facade.

Some tests import `CIDStore` from `cidstore.main` and expect a
callable with synchronous `insert` and `get` methods. The real
implementation lives in `src/cidstore/store.py` and exposes async
APIs. This file provides a minimal synchronous wrapper that runs the
async methods on a dedicated event loop thread so existing tests can
use the sync API without changes.

This wrapper is intentionally small and intended for test compatibility
only.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

from .storage import Storage
from .store import CIDStore as _AsyncCIDStore
from .wal import WAL


class _LoopThread:
    """Run an asyncio event loop in a dedicated thread and allow
    submitting coroutines to it synchronously.
    """

    def __init__(self) -> None:
        self._loop = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started = threading.Event()
        self._thread.start()
        self._started.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        loop.run_forever()

    def run_coro(self, coro: Any):
        """Submit a coroutine to the background loop and wait for result."""
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result()


class CIDStore:
    """Synchronous facade around async CIDStore used by tests.

    Example:
        storage = Storage(path)
        wal = WAL(None)
        tree = CIDStore(storage, wal)
        tree.insert(key, value)
        vals = list(tree.get(key))
    """

    def __init__(self, storage: Storage, wal: WAL, testing: bool = True) -> None:
        self._loop_thread = _LoopThread()

        # Instantiate the async store inside the background loop so its
        # background tasks and event-loop-dependent code attach to that
        # loop. Use run_coro to synchronously wait for construction to
        # complete (the async CIDStore constructor may run replay
        # synchronously when not created inside a running loop).
        async def _make():
            store = _AsyncCIDStore(storage, wal, testing=testing)
            # If the async interface exposes async_init, run it.
            if hasattr(store, "async_init"):
                await store.async_init()
            return store

        self._store = self._loop_thread.run_coro(_make())

    def insert(self, key: Any, value: Any) -> None:
        """Run async insert synchronously."""
        return self._loop_thread.run_coro(self._store.insert(key, value))

    def get(self, key: Any):
        """Run async get synchronously and return an iterator/list-like result."""
        return self._loop_thread.run_coro(self._store.get(key))

    def close(self) -> None:
        try:
            self._loop_thread.run_coro(self._store.aclose())
        except Exception:
            try:
                self._loop_thread.run_coro(self._store.close())
            except Exception:
                pass


"""main.py - Example usage and entry point for CIDTree"""
