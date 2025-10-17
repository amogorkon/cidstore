import asyncio
import inspect
import io
import logging
import threading
import time
from contextlib import suppress
from typing import Any, Coroutine

import pytest

# Avoid importing cidstore modules at top-level to prevent early numpy
# native initialization during pytest collection which can fail on some
# environments. Import modules lazily inside fixtures where needed.


@pytest.fixture
def store():
    # Provide a synchronous fixture that initializes the async components
    # via asyncio.run so tests do not require pytest-asyncio plugin.
    f = io.BytesIO()
    # Import lazily to avoid importing numpy at test-collection time
    from cidstore.storage import Storage
    from cidstore.store import CIDStore
    from cidstore.wal import WAL

    storage = Storage(f)
    wal = WAL(None)  # In-memory WAL
    # Keep CIDStore in testing/debugging mode so WAL operations are applied
    # synchronously during tests, but start the maintenance threads manually
    # so tests that expect running background threads can observe them.
    store = CIDStore(storage, wal=wal, testing=True)

    # Run async_init synchronously for tests
    asyncio.run(store.async_init())
    try:
        if hasattr(store, "maintenance_manager"):
            with suppress(Exception):
                store.maintenance_manager.start()
        yield store
    finally:
        # Stop background threads and close resources
        with suppress(Exception):
            if hasattr(store, "maintenance_manager"):
                store.maintenance_manager.stop()
        try:
            asyncio.run(store.aclose())
        except Exception:
            store.close()


@pytest.fixture
def wal_analyzer(store):
    # Import lazily to avoid numpy import during collection
    from cidstore.maintenance import MaintenanceConfig, WALAnalyzer

    config = getattr(store, "config", None) or MaintenanceConfig()
    analyzer = WALAnalyzer(store, config)
    try:
        yield analyzer
    finally:
        analyzer.stop()
        analyzer.join()


@pytest.fixture
def maintenance_manager(store):
    # Import lazily to avoid numpy import during collection
    from cidstore.maintenance import MaintenanceConfig, MaintenanceManager

    config = getattr(store, "config", None) or MaintenanceConfig()
    manager = MaintenanceManager(store, config)
    try:
        yield manager
    finally:
        manager.stop()


@pytest.fixture
def directory():
    """Provide a lightweight directory fixture backed by the same in-memory
    Storage/WAL setup used in the `store` fixture but named `directory` for
    tests that expect a separate fixture.
    """
    import io

    # Import lazily to avoid numpy import during collection
    from cidstore.storage import Storage
    from cidstore.store import CIDStore
    from cidstore.wal import WAL

    storage = Storage(io.BytesIO())
    wal = WAL(None)
    directory = CIDStore(storage, wal=wal, testing=True)
    try:
        # Initialize async parts synchronously
        asyncio.run(directory.async_init())
        if hasattr(directory, "maintenance_manager"):
            with suppress(Exception):
                directory.maintenance_manager.start()
        yield directory
    finally:
        with suppress(Exception):
            if hasattr(directory, "maintenance_manager"):
                directory.maintenance_manager.stop()
        try:
            asyncio.run(directory.aclose())
        except Exception:
            directory.close()


@pytest.fixture
def in_memory_hdf5(tmp_path):
    """Provide an HDF5 File object backed by a temporary file for tests that
    expect an "in-memory" HDF5 file. We use a tmp_path file to avoid relying
    on h5py core-driver availability across environments.
    """
    import os

    import h5py

    p = tmp_path / "inmem.h5"
    f = h5py.File(str(p), "w")
    try:
        yield f
    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            os.unlink(str(p))
        except Exception:
            pass


@pytest.fixture
def tree(store, request):
    """Alias fixture for older tests that expect a `tree` fixture name.

    Return a thin synchronous wrapper that maps simple `insert`/`get`
    calls to the async `CIDStore` implementation so legacy tests that
    call these methods from threads (non-async) continue to work.
    """

    class _LoopThread:
        def __init__(self):
            self.loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

        def _run(self):
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()

        def run_coro(self, coro: Coroutine) -> Any:
            tid = threading.get_ident()
            # Use cidstore.store logger so messages appear with the same config
            logger = logging.getLogger("cidstore.store")
            logger.info(
                "[LoopThread.run_coro] submitting coro to loop (thread_id=%s)", tid
            )
            start = time.time()
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            logger.info(
                "[LoopThread.run_coro] submitted, waiting for result (thread_id=%s)",
                tid,
            )
            try:
                res = future.result()
                duration = time.time() - start
                logger.info(
                    "[LoopThread.run_coro] future.result() returned (thread_id=%s, duration=%.6fs)",
                    tid,
                    duration,
                )
                return res
            except Exception:
                duration = time.time() - start
                logger.exception(
                    "[LoopThread.run_coro] future.result() raised (thread_id=%s, duration=%.6fs)",
                    tid,
                    duration,
                )
                raise

        def stop(self):
            from contextlib import suppress as _suppress

            with _suppress(Exception):
                self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(timeout=1)
            with _suppress(Exception):
                self.loop.close()

    class SyncCIDStoreWrapper:
        def __init__(self, store_obj):
            self._store = store_obj
            self._loop_thread = _LoopThread()
            self._logger = logging.getLogger("cidstore.store")

        def _ensure_e(self, k):
            from cidstore.keys import E

            if isinstance(k, E):
                return k
            try:
                return E.from_str(str(k))
            except Exception:
                return E(k)

        def _ensure_e_value(self, v):
            from cidstore.keys import E

            if isinstance(v, E):
                return v
            try:
                return E.from_int(int(v))
            except Exception:
                return E.from_str(str(v))

        def insert(self, key, value):
            k = self._ensure_e(key)
            v = self._ensure_e_value(value)
            tid = threading.get_ident()
            self._logger.info(
                "[SyncWrapper.insert] calling insert (thread_id=%s, key=%s)", tid, k
            )
            # The underlying store.insert may be async (coroutine function)
            # or a synchronous test-only wrapper. Handle both cases:
            # Prefer to detect an async function rather than calling it and
            # inspecting the return value. If it's async, call to obtain
            # a coroutine and submit to the loop thread. If it's sync,
            # call directly (it's intended for test-only sync behaviour).
            try:
                if inspect.iscoroutinefunction(self._store.insert):
                    coro = self._store.insert(k, v)
                    res = self._loop_thread.run_coro(coro)
                else:
                    res = self._store.insert(k, v)
            except Exception:
                self._logger.exception("[SyncWrapper.insert] underlying insert raised")
                raise
            self._logger.info(
                "[SyncWrapper.insert] returned (thread_id=%s, key=%s)", tid, k
            )
            return res

        def get(self, key):
            k = self._ensure_e(key)
            tid = threading.get_ident()
            self._logger.info(
                "[SyncWrapper.get] calling get (thread_id=%s, key=%s)", tid, k
            )
            try:
                if inspect.iscoroutinefunction(self._store.get):
                    coro = self._store.get(k)
                    vals = self._loop_thread.run_coro(coro)
                else:
                    vals = self._store.get(k)
            except Exception:
                self._logger.exception("[SyncWrapper.get] underlying get raised")
                raise
            self._logger.info(
                "[SyncWrapper.get] returned (thread_id=%s, key=%s, len=%s)",
                tid,
                k,
                len(vals),
            )
            # Return plain Python ints for legacy tests
            return [int(x) for x in vals]

        # Provide simple sync helpers used by some tests
        def get_entry(self, key):
            k = self._ensure_e(key)
            return self._loop_thread.run_coro(self._store.get_entry(k))

        # Transaction support methods
        def begin_transaction(self):
            if inspect.iscoroutinefunction(self._store.begin_transaction):
                coro = self._store.begin_transaction()
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.begin_transaction()

        def commit(self):
            if inspect.iscoroutinefunction(self._store.commit):
                coro = self._store.commit()
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.commit()

        def rollback(self):
            if inspect.iscoroutinefunction(self._store.rollback):
                coro = self._store.rollback()
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.rollback()

        def in_transaction(self):
            return self._store.in_transaction()

        def insert_triple(self, subject, predicate, obj):
            s = self._ensure_e(subject)
            p = self._ensure_e(predicate)
            if inspect.iscoroutinefunction(self._store.insert_triple):
                coro = self._store.insert_triple(s, p, obj)
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.insert_triple(s, p, obj)

        def delete_triple(self, subject, predicate, obj):
            s = self._ensure_e(subject)
            p = self._ensure_e(predicate)
            if inspect.iscoroutinefunction(self._store.delete_triple):
                coro = self._store.delete_triple(s, p, obj)
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.delete_triple(s, p, obj)

        def get_triple(self, subject, predicate):
            s = self._ensure_e(subject)
            p = self._ensure_e(predicate)
            if inspect.iscoroutinefunction(self._store.get_triple):
                coro = self._store.get_triple(s, p)
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.get_triple(s, p)

        def insert_triple_transactional(self, subject, predicate, obj):
            s = self._ensure_e(subject)
            p = self._ensure_e(predicate)
            if inspect.iscoroutinefunction(self._store.insert_triple_transactional):
                coro = self._store.insert_triple_transactional(s, p, obj)
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.insert_triple_transactional(s, p, obj)

        def delete_triple_transactional(self, subject, predicate, obj):
            s = self._ensure_e(subject)
            p = self._ensure_e(predicate)
            if inspect.iscoroutinefunction(self._store.delete_triple_transactional):
                coro = self._store.delete_triple_transactional(s, p, obj)
                return self._loop_thread.run_coro(coro)
            else:
                return self._store.delete_triple_transactional(s, p, obj)

        def register_predicate(self, predicate, data_structure_class):
            """Register a specialized data structure for a predicate.

            Args:
                predicate: The predicate (E or convertible)
                data_structure_class: The DS class (CounterStore or MultiValueSetStore)
            """
            from cidstore.predicates import CounterStore, MultiValueSetStore

            p = self._ensure_e(predicate)

            # Route to appropriate registry method
            if (
                data_structure_class == CounterStore
                or data_structure_class.__name__ == "CounterStore"
            ):
                return self._store.predicate_registry.register_counter(p)
            elif (
                data_structure_class == MultiValueSetStore
                or data_structure_class.__name__ == "MultiValueSetStore"
            ):
                return self._store.predicate_registry.register_multivalue(p)
            else:
                raise ValueError(
                    f"Unsupported data structure class: {data_structure_class}"
                )

        def audit_indices(self):
            """Audit index consistency (synchronous wrapper)."""
            from inspect import iscoroutinefunction

            func = self._store.audit_indices
            if iscoroutinefunction(func):
                return self._loop_thread.run_coro(func())
            else:
                return func()

        def hot_reload_predicate(self, predicate, new_plugin_type, migrate_data=True):
            """Hot-reload predicate with new plugin type (synchronous wrapper)."""
            p = self._ensure_e(predicate)
            coro = self._store.predicate_registry.hot_reload_predicate(
                p, new_plugin_type, migrate_data
            )
            return self._loop_thread.run_coro(coro)

        def close(self):
            from contextlib import suppress

            with suppress(Exception):
                self._loop_thread.stop()

    wrapper = SyncCIDStoreWrapper(store)

    # Ensure the wrapper's event loop thread is stopped when the fixture is
    # torn down. Use pytest's request finalizer to reliably stop the background
    # loop thread even when tests forget to call `close()`.
    def _teardown():
        from contextlib import suppress as _suppress

        with _suppress(Exception):
            wrapper.close()

    request.addfinalizer(_teardown)
    return wrapper
