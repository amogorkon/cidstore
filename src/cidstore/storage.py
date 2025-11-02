"""HDF5 storage layer for CIDStore - manager and raw hooks for CIDTree.

Note: This module interacts heavily with h5py's dynamic types (Group/Dataset/Datatype)
which can confuse static analyzers and mypy in some environments. To reduce
editor/CI noise we add a file-level mypy ignore. This is a conservative, reversible
change that does not affect runtime behavior.
"""

# mypy: ignore-errors
# pyright: reportUnknownMemberType=false, reportGeneralTypeIssues=false, reportOptionalMemberAccess=false

import asyncio
import inspect
import io
import os
import queue
import tempfile
import threading
from pathlib import Path
from time import time
from typing import Any, cast

import numpy as np
from h5py import Dataset, File, Group, version

from .config import CONFIG_GROUP, NODES_GROUP, VALUES_GROUP
from .constants import (
    BUCKS,
    HASH_ENTRY_DTYPE,
    HDF5_NOT_OPEN_MSG,
    OP,
    ROOT,
)
from .keys import E
from .logger import get_logger
from .utils import assumption


# Helper to cast h5py dynamic objects (Group/Dataset/Datatype) to Any
# for static analyzers. This is a conservative, local-only helper that
# does not change runtime behavior but reduces editor/CI false-positives.
def _h5(obj: object) -> Any:
    return cast(Any, obj)


logger = get_logger(__name__)


class Storage:
    def __init__(self, path: str | Path | io.BytesIO | None) -> None:
        """
        Initialize the StorageManager with the given file path or in-memory buffer.
        Accepts a string path, pathlib.Path, or an io.BytesIO object for in-memory operation.
        """
        self._tempfile_path: Path | None = None
        self.path: Path | io.BytesIO
        # Prefer a filesystem-backed temporary file for storage when the
        # caller passed an in-memory buffer (io.BytesIO). Many h5py/HDF5
        # builds do not support reopening the same BytesIO buffer across
        # independent File handles in different threads which breaks our
        # worker/reader reopening strategy. Convert BytesIO to a real
        # temporary file so per-operation opens, flush, and fsync work as
        # expected across threads.
        match path:
            case None:
                tf = tempfile.NamedTemporaryFile(delete=False)
                tf.close()
                self._tempfile_path = Path(tf.name)
                self.path = self._tempfile_path
            case io.BytesIO():
                # Dump BytesIO contents to a tempfile and use that path.
                tf = tempfile.NamedTemporaryFile(delete=False)
                try:
                    # Write current buffer content to disk
                    data = path.getvalue()
                    tf.write(data)
                    tf.flush()
                finally:
                    tf.close()
                self._tempfile_path = Path(tf.name)
                self.path = self._tempfile_path
            case str() | Path():
                self.path = Path(path) if isinstance(path, str) else path
            case _:
                raise TypeError("path must be str, Path, or io.BytesIO or None")

        self.file: File = self.open()
        # Lock to serialize access to the underlying HDF5 file/datasets
        # across threads. h5py objects are not always thread-safe when
        # resizing/reading concurrently; using a reentrant lock here
        # prevents races observed in multi-threaded tests.
        # Additionally marshal all HDF5 operations to a single dedicated
        # worker thread. Using our own queue/thread avoids interactions
        # between ThreadPoolExecutor and asyncio loop callbacks that
        # previously caused deadlocks on Windows.
        self._lock = threading.RLock()
        # Worker queue/ thread for all HDF5 I/O. Items are tuples of
        # (callable, args, kwargs, loop, future)
        self._worker_queue: "queue.Queue" = queue.Queue()
        self._worker_thread = threading.Thread(
            target=self._worker_loop, name="cidstore-hdf-worker", daemon=True
        )
        # Queue for results: worker will enqueue completed task results here
        # and a dispatcher thread will call into asyncio loops in batches to
        # avoid frequent small `call_soon_threadsafe` calls that caused
        # blocking on Windows (socket send in proactor).
        self._result_queue: "queue.Queue" = queue.Queue()
        self._result_dispatcher_thread = threading.Thread(
            target=self._result_dispatcher_loop,
            name="cidstore-hdf-result-dispatcher",
            daemon=True,
        )
        # Shutdown coordination event to allow timeouts in queue.get loops
        # This avoids threads hanging indefinitely if a sentinel is not
        # delivered due to a race during close(). Using an Event makes the
        # shutdown cooperative and robust across platforms.
        self._shutdown_event = threading.Event()
        # Initialize on-disk layout before starting the worker thread so the
        # worker's persistent file handle observes the canonical layout. If
        # we start the worker early it may open the file before groups/dsets
        # are created which leads to visibility issues on some platforms.
        self._init_hdf5_layout()
        self._init_root()
        self._worker_thread.start()
        self._result_dispatcher_thread.start()
        # Instrumentation counters for diagnostics
        self._submitted_count = 0
        self._worker_executed_count = 0
        self._dispatched_count = 0
        # In-memory worker-local index to make writes immediately visible to
        # subsequent reads executed on the worker thread. This avoids relying
        # solely on filesystem/HDF5 propagation semantics which can vary by
        # platform and VFD. Key: (bucket_name, key_high, key_high_mid, key_low_mid, key_low) -> numpy entry
        self._mem_index: dict[tuple[str, int, int, int, int], object] = {}
        # Separate lock to guard the in-memory mem-index. Using a dedicated
        # lock avoids circular wait between the HDF5-worker (which may need
        # the _lock to perform filesystem operations) and callers that only
        # need to inspect/publish the mem-index. Keep this an RLock so the
        # same thread may re-enter safely.
        self._mem_index_lock = threading.RLock()
        # Track WAL/hint timestamps for entries published into the mem-index
        # This helps avoid overwriting newer worker-published entries with
        # stale reads from separate file handles. Keyed by the same tuple as
        # _mem_index; value is either a WAL-time tag or None when unavailable.
        self._mem_index_wal_times: dict[tuple[str, int, int, int, int], object] = {}

        # Monotonic publish sequence counter for diagnostics
        # Incremented while holding _mem_index_lock so logs can be correlated
        # with the actual ordering of atomic mem-index publications.
        self._publish_seq = 0

        # Log storage initialization info
        from contextlib import suppress

        with suppress(Exception):
            mode = getattr(self.file, "mode", "unknown")
            swmr = getattr(self.file, "swmr_mode", False)
            path_str = None if isinstance(self.path, io.BytesIO) else str(self.path)
            logger.info(f"Storage initialized:, {path_str=}, {mode=}, {swmr=}")

    def _init_root(self) -> None:
        """
        Initialize the root node in the HDF5 file.
        """
        if ROOT not in self.file:
            self.file.create_group(ROOT)
        # Initialize with empty data
        self.file[ROOT].attrs["data"] = b""
        self.file[ROOT].attrs["type"] = b""
        self.file[ROOT].attrs["size"] = 0
        self.file[ROOT].attrs["count"] = 0
        self.file[ROOT].attrs["next"] = 0
        self.file[ROOT].attrs["prev"] = 0
        self.file[ROOT].attrs["flags"] = 0
        self.file[ROOT].attrs["padding"] = b""
        self.file[ROOT].attrs["checksum"] = 0

    def _init_hdf5_layout(self) -> None:
        """
        Ensure all groups, datasets, and attributes match canonical layout (Spec 9).
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG

        cfg = self.file.require_group("/config")
        cfg.attrs.setdefault("format_version", 1)
        cfg.attrs.setdefault("created_by", "CIDStore")
        cfg.attrs.setdefault("swmr", True)
        cfg.attrs.setdefault("last_opened", int(time()))
        cfg.attrs.setdefault("last_modified", int(time()))
        if "dir" not in cfg.attrs:
            cfg.attrs["dir"] = "{}"
        self.file.attrs.setdefault("cidstore_version", "1.0")
        self.file.attrs.setdefault("hdf5_version", version.hdf5_version)
        self.file.attrs.setdefault("swmr", True)
        if "/values" not in self.file:
            self.file.create_group("/values")
        if "/values/sp" not in self.file:
            self.file["/values"].file.create_group("sp")
        if "/nodes" not in self.file:
            self.file.create_group("/nodes")
        self.file.flush()
        # Ensure data is persisted to the OS so other file handles (opened
        # after this point) observe the created groups/datasets. This is
        # important on platforms where an h5py File opened on a different
        # handle may not see updates until the underlying file is fsynced.
        from contextlib import suppress

        with suppress(Exception):
            if not isinstance(self.path, io.BytesIO):
                fd = os.open(str(self.path), os.O_RDWR)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)

    def _ensure_core_groups(self) -> None:
        """
        Ensure that the CONFIG, NODES, VALUES, and BUCKETS groups exist.
        Note: WAL_DATASET is managed by the WAL class itself.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        for grp in (CONFIG_GROUP, NODES_GROUP, VALUES_GROUP, BUCKS):
            g = self.file.require_group(grp)
            if grp == CONFIG_GROUP:
                if "format_version" not in g.attrs:
                    g.attrs["format_version"] = 1
                if "version_string" not in g.attrs:
                    g.attrs["version_string"] = "1.0.0"

    def __getitem__(self, item):
        """
        Allow subscript access to delegate to the underlying HDF5 file.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        return self.file[item]

    def __contains__(self, item):
        """
        Allow 'in' checks to delegate to the underlying HDF5 file.
        Always open in append mode to allow creation if needed.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        return item in self.file

    def create_group(self, name):
        """
        Allow create_group to delegate to the underlying HDF5 file.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        return self.file.create_group(name)

    def open(self) -> File:
        """
        Open the HDF5 file (create if needed), enable SWMR if desired,
        and ensure the core groups (config, nodes, values) exist.
        Supports both file paths and in-memory io.BytesIO buffers.
        """
        try:
            # When given a path, open in 'w' (write) mode if path is io.BytesIO, otherwise 'a' (append).
            mode = "w" if isinstance(self.path, io.BytesIO) else "a"
            swmr_flag = getattr(self, "_swmr_enabled", False)
            # Only pass swmr flag when the attribute exists; some h5py builds
            # may not accept the swmr keyword in File() constructor on all
            # platforms. Passing False is safe.
            self.file = File(self.path, mode, libver="latest", swmr=swmr_flag)
            self._ensure_core_groups()
            # Try to enable SWMR mode on the writer file handle when running on
            # a filesystem-backed file. SWMR makes updates from the writer visible
            # to readers that open with `swmr=True` and call `refresh()`.
            # Track whether SWMR was successfully enabled on the writer
            self._swmr_enabled = False
            if mode == "a" and not isinstance(self.path, io.BytesIO):
                try:
                    self.file.swmr_mode = True
                    self._swmr_enabled = True
                    logger.debug("Storage.open: enabled SWMR mode on writer file")
                except Exception:
                    # SWMR may not be supported by the HDF5 build or VFD
                    logger.debug("Storage.open: SWMR not supported on this HDF5 build")
            return self.file
        except (OSError, RuntimeError) as e:
            raise RuntimeError(f"Failed to open HDF5 file '{self.path}': {e}") from e

    def flush(self) -> None:
        """
        Flush all in-memory buffers to disk. Useful after batched
        operations or before handing off to readers in SWMR mode.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        self.file.flush()

    def close(self) -> None:
        """
        Close the HDF5 file, flushing first. Safe to call multiple times.
        """
        # file can be None, but if not, must be File
        # Perform a cooperative shutdown of worker/dispatcher threads so
        # test harnesses and short-lived processes don't hang waiting on
        # background threads. This will attempt a graceful shutdown but
        # tolerate failures during interpreter finalization.
        try:
            # Signal shutdown to any waiting loops
            try:
                self._shutdown_event.set()
            except Exception:
                pass

            # Enqueue sentinels for worker/dispatcher to exit if they are
            # still running. Use non-blocking puts guarded by try/except to
            # avoid raising during interpreter shutdown.
            try:
                self._worker_queue.put(None)
            except Exception:
                pass
            try:
                # Put sentinel for result dispatcher as well; worker also
                # attempts to put a sentinel on exit but ensure here too.
                self._result_queue.put(None)
            except Exception:
                pass

            # Flush file contents to disk before closing
            from contextlib import suppress

            with suppress(Exception):
                self.flush()

            # Join worker threads with a short timeout to avoid blocking
            # indefinitely in test environments. If join fails or threads
            # are still alive, log a warning and continue closing resources.
            try:
                if getattr(self, "_worker_thread", None) is not None:
                    self._worker_thread.join(timeout=2.0)
            except Exception:
                pass
            try:
                if getattr(self, "_result_dispatcher_thread", None) is not None:
                    self._result_dispatcher_thread.join(timeout=2.0)
            except Exception:
                pass
        except Exception:
            # Best-effort: swallow any shutdown coordination errors
            pass

        # Finally close the HDF5 file if present and remove any temporary file
        try:
            if getattr(self, "file", None) is not None:
                try:
                    self.file.flush()
                except Exception:
                    pass
                try:
                    self.file.close()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if getattr(self, "_tempfile_path", None) is not None:
                try:
                    os.unlink(str(self._tempfile_path))
                except Exception:
                    pass
                self._tempfile_path = None
        except Exception:
            pass

    def _worker_loop(self):
        """Worker thread main loop: execute queued HDF5 tasks.

        Each queue item is either None (shutdown) or a tuple
        (callable, args, kwargs, asyncio_loop, asyncio_future).
        The worker executes callable(*args, **kwargs) and uses
        asyncio_loop.call_soon_threadsafe to set the result/exception
        on the provided asyncio future.
        """

        # Open a persistent HDF5 File in the worker thread so all
        # synchronous HDF5 operations executed here reuse the same
        # file handle. This avoids multiple writer/reader handles and
        # improves visibility between operations.
        # Do not open a persistent worker-local File here. Using multiple
        # simultaneously-open h5py File objects on the same file has caused
        # 'addr overflow' errors on some h5py/HDF5 builds. Prefer opening
        # per-operation File handles inside the worker functions and rely on
        # flush+fsync to make writer updates visible to subsequent opens.
        self._worker_file = None

        while True:
            try:
                item = self._worker_queue.get(timeout=0.5)
            except queue.Empty:
                # Periodically wake up to check for shutdown in case a
                # sentinel was not delivered due to a race during finalization.
                if self._shutdown_event.is_set():
                    logger.info("_worker_loop: shutdown_event set, exiting")
                    break
                continue
            if item is None:
                logger.info("_worker_loop: received shutdown signal")
                break
            func, args, kwargs, loop, afut = item
            # Diagnostic: attempt to extract future tag and wal_time for correlation
            try:
                cur_tag = getattr(afut, "_cidstore_tag", None)
            except Exception:
                cur_tag = None
            try:
                cur_wal_time = getattr(afut, "_cidstore_wal_time", None)
            except Exception:
                cur_wal_time = None
            try:
                print(
                    f"WKR_START: func={getattr(func, '__name__', str(func))} tag={cur_tag} wal_time={cur_wal_time}"
                )
            except Exception:
                pass
            t0 = time()
            # Instrumentation: record execution count and worker queue size
            try:
                self._worker_executed_count += 1
                try:
                    wq = self._worker_queue.qsize()
                except Exception:
                    wq = -1
                logger.debug(
                    f"_worker_loop: executing task {getattr(func, '__name__', str(func))} on thread {threading.current_thread().name} at {t0} worker_executed_count={self._worker_executed_count} worker_queue_qsize={wq}"
                )
            except Exception:
                logger.debug(
                    f"_worker_loop: executing task {getattr(func, '__name__', str(func))} on thread {threading.current_thread().name} at {t0}"
                )
            try:
                res = func(*args, **(kwargs or {}))
            except Exception as e:
                t1 = time()
                logger.exception(
                    f"_worker_loop: task {getattr(func, '__name__', str(func))} raised after {t1 - t0:.3f}s: {e}"
                )
                # Enqueue the exception for the dispatcher to set on the future
                try:
                    # Enqueue the exception for batched dispatch by the
                    # result dispatcher thread. Avoid calling into the
                    # asyncio loop from the worker thread to prevent any
                    # potential blocking on platform-specific loop internals.
                    try:
                        rq = self._result_queue.qsize()
                    except Exception:
                        rq = -1
                    try:
                        tag = getattr(afut, "_cidstore_tag", None)
                    except Exception:
                        tag = None
                    logger.debug(
                        f"_worker_loop: enqueueing exception for task {getattr(func, '__name__', str(func))} tag={tag} result_queue_qsize_before_put={rq}"
                    )
                    try:
                        self._result_queue.put((loop, afut, False, e))
                    except Exception:
                        # If enqueue fails, as a last resort try to set the
                        # future here (best-effort) to avoid leaving callers
                        # waiting indefinitely.
                        try:
                            if not afut.done():
                                afut.set_exception(e)
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                t1 = time()
                logger.debug(
                    f"_worker_loop: task {getattr(func, '__name__', str(func))} completed in {t1 - t0:.3f}s"
                )
                try:
                    # Enqueue the successful result for batched dispatch by
                    # the dispatcher thread. This prevents the worker from
                    # ever calling into the asyncio loop and risking a
                    # platform-specific blocking behavior when scheduling
                    # callbacks.
                    try:
                        rq = -1
                        try:
                            rq = self._result_queue.qsize()
                        except Exception:
                            pass
                        try:
                            tag = getattr(afut, "_cidstore_tag", None)
                        except Exception:
                            tag = None
                        try:
                            wal_time = getattr(afut, "_cidstore_wal_time", None)
                        except Exception:
                            wal_time = None
                        logger.debug(
                            f"_worker_loop: enqueueing result for task {getattr(func, '__name__', str(func))} tag={tag} wal_time={wal_time} result_queue_qsize_before_put={rq}"
                        )
                        try:
                            print(
                                f"WKR_ENQUEUE: task={getattr(func, '__name__', str(func))} tag={tag} wal_time={wal_time} rq_before={rq}"
                            )
                        except Exception:
                            pass
                        # For insert-related worker functions, preferentially
                        # schedule the result directly on the target asyncio
                        # loop to reduce the time between mem-index
                        # publication and future resolution. This makes HDF5
                        # visibility deterministic for timing-sensitive
                        # concurrency tests. If scheduling fails, fall back to
                        # enqueuing into the result dispatcher as before.
                        scheduled_directly = False
                        try:
                            func_name = getattr(func, "__name__", None)
                        except Exception:
                            func_name = None

                        try:
                            if func_name in (
                                "_apply_insert_sync",
                                "_apply_insert_promote_sync",
                                "_apply_insert_spill_sync",
                            ):
                                # For insert operations, use regular dispatcher queue to ensure
                                # proper ordering. The direct scheduling approach was causing
                                # visibility issues where get() operations could read stale
                                # mem_index entries due to timing races.
                                # Let the result fall through to normal dispatcher handling.
                                scheduled_directly = False

                        except Exception:
                            scheduled_directly = False

                        if not scheduled_directly:
                            try:
                                self._result_queue.put((loop, afut, True, res))
                                logger.debug(
                                    f"_worker_loop: enqueued result for tag={tag} wal_time={wal_time}"
                                )
                                try:
                                    print(
                                        f"WKR_ENQUEUED: tag={tag} wal_time={wal_time}"
                                    )
                                except Exception:
                                    pass
                                try:
                                    print(
                                        f"WKR_FINISH: tag={tag} wal_time={wal_time} duration={t1 - t0:.3f}s"
                                    )
                                except Exception:
                                    pass
                            except Exception as e_put:
                                logger.exception(
                                    f"_worker_loop: result_queue.put failed: {e_put}"
                                )
                                # If enqueue fails, as a last resort try to set the
                                # future here (best-effort) to avoid leaving callers
                                # waiting indefinitely.
                                try:
                                    if not afut.done():
                                        afut.set_result(res)
                                        logger.debug(
                                            f"_worker_loop: directly set future result for tag={tag}"
                                        )
                                except Exception:
                                    pass
                    except Exception:
                        pass
                except Exception:
                    pass

        # Worker loop exiting; signal dispatcher to shutdown
        from contextlib import suppress

        with suppress(Exception):
            self._result_queue.put(None)
        # Close persistent worker file if one was opened
        try:
            if getattr(self, "_worker_file", None) is not None:
                try:
                    _h5(self._worker_file).flush()
                except Exception:
                    pass
                try:
                    _h5(self._worker_file).close()
                except Exception:
                    pass
        except Exception:
            pass

        logger.info("_worker_loop: exiting")

    async def _submit_task(self, func, *args, **kwargs):
        """Submit a synchronous function to the HDF5 worker and await its result.

        The function runs in the dedicated HDF5 worker thread and the
        awaiting asyncio task gets the result or exception.
        """
        loop = asyncio.get_running_loop()
        afut = loop.create_future()
        # Attach a small debug tag on the future so later logs (worker/dispatcher)
        # can correlate this future with the original task and key args.
        try:
            func_name = getattr(func, "__name__", str(func))
            try:
                tag_args = tuple(args[:3])
            except Exception:
                tag_args = tuple(args)
            try:
                setattr(cast(Any, afut), "_cidstore_tag", f"{func_name}:{tag_args}")
            except Exception:
                pass
        except Exception:
            pass
        # Instrumentation: increment submitted counter and log worker queue size
        try:
            self._submitted_count += 1
            try:
                qsize = self._worker_queue.qsize()
            except Exception:
                qsize = -1
            logger.debug(
                f"_submit_task: submitting {getattr(func, '__name__', str(func))} to worker queue for loop {id(loop)} submitted_count={self._submitted_count} worker_queue_qsize={qsize}"
            )
        except Exception:
            pass
        # Put the callable and the asyncio future onto the worker queue
        try:
            logger.debug(
                f"_submit_task: enqueueing task {getattr(func, '__name__', str(func))} at {time()} to worker queue for loop {id(loop)}"
            )
        except Exception:
            pass
        # If the caller passed a WAL op-like object somewhere in args or kwargs,
        # try to extract a hybrid time tuple and attach it to the future so
        # worker/dispatcher logs can correlate operations end-to-end.
        try:
            wal_time = None

            def _extract_wal_time_from_obj(a):
                try:
                    if a is None:
                        return None
                    # Mapping-like objects: dict or dict-like with .get
                    if isinstance(a, dict):
                        if "_wal_time" in a or "wal_time" in a:
                            return a.get("_wal_time") or a.get("wal_time")
                    if hasattr(a, "get") and callable(getattr(a, "get")):
                        try:
                            # Some mapping-like objects support 'in'
                            if ("_wal_time" in a) or ("wal_time" in a):
                                return a.get("_wal_time") or a.get("wal_time")
                        except Exception:
                            # Fallback to calling get directly
                            try:
                                return a.get("_wal_time") or a.get("wal_time")
                            except Exception:
                                pass
                    # Attribute-style access
                    if hasattr(a, "_wal_time"):
                        return getattr(a, "_wal_time")
                    if hasattr(a, "wal_time"):
                        return getattr(a, "wal_time")
                    # Fallback: objects exposing nanos/seq/shard_id
                    if hasattr(a, "nanos") or hasattr(a, "seq"):
                        return (
                            getattr(a, "nanos", None),
                            getattr(a, "seq", None),
                            getattr(a, "shard_id", None),
                        )
                except Exception:
                    return None
                return None

            # Search positional args first
            for a in args:
                try:
                    wal_time = _extract_wal_time_from_obj(a)
                except Exception:
                    wal_time = None
                if wal_time is not None:
                    break

            # If not found, search keyword args as well
            if wal_time is None and kwargs:
                for _, a in kwargs.items():
                    try:
                        wal_time = _extract_wal_time_from_obj(a)
                    except Exception:
                        wal_time = None
                    if wal_time is not None:
                        break

            if wal_time is not None:
                try:
                    try:
                        setattr(cast(Any, afut), "_cidstore_wal_time", wal_time)
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        self._worker_queue.put((func, args, kwargs, loop, afut))
        # Await the asyncio future; caller will resume when the dispatcher
        # schedules the result. Log when the awaiting begins for tracing.
        try:
            logger.debug(
                f"_submit_task: awaiting result for {getattr(func, '__name__', str(func))} at {time()} loop={id(loop)}"
            )
        except Exception:
            pass
        return await afut

    def _result_dispatcher_loop(self):
        """Dispatcher thread that batches results per asyncio event loop.

        The dispatcher reads completed task results from `self._result_queue` and
        groups them by their target asyncio loop. For each loop it schedules a
        single `call_soon_threadsafe` callback that sets all pending futures in
        that batch. This prevents the HDF5 worker thread from blocking on
        frequent tiny `call_soon_threadsafe` calls (observed as socket sends on
        Windows proactor implementation).
        """
        while True:
            try:
                item = self._result_queue.get(timeout=0.5)
            except queue.Empty:
                if self._shutdown_event.is_set():
                    logger.info("_result_dispatcher_loop: shutdown_event set, exiting")
                    break
                continue
            if item is None:
                break
            # Collect a batch (drain available items)
            batches = {}
            try:
                loop, afut, ok, payload = item
            except Exception:
                continue
            batches.setdefault(loop, []).append((afut, ok, payload))
            # drain quickly without blocking
            while True:
                try:
                    it = self._result_queue.get_nowait()
                except queue.Empty:
                    break
                if it is None:
                    # put back sentinel for outer loop to handle shutdown
                    self._result_queue.put(None)
                    break
                loop2, afut2, ok2, payload2 = it
                batches.setdefault(loop2, []).append((afut2, ok2, payload2))

            # For each loop, schedule a single call_soon_threadsafe to set results
            for loop, lst in list(batches.items()):
                # copy the list to avoid mutation issues
                batch_copy = list(lst)
                # Attach diagnostic tag list for logging: collect future tags
                try:
                    tags = []
                    for afut, ok, payload in batch_copy:
                        try:
                            t = getattr(afut, "_cidstore_tag", None)
                            wt = getattr(afut, "_cidstore_wal_time", None)
                            tags.append((t, wt))
                        except Exception:
                            tags.append((None, None))
                except Exception:
                    tags = None
                # Instrumentation: increment dispatch counter and log result queue size
                try:
                    self._dispatched_count += 1
                    try:
                        rq = self._result_queue.qsize()
                    except Exception:
                        rq = -1
                    logger.debug(
                        f"_result_dispatcher_loop: dispatching batch to loop id={id(loop)} batch_count={len(batch_copy)} dispatched_count={self._dispatched_count} result_queue_qsize={rq}"
                    )
                except Exception:
                    pass
                # Print batch-level diagnostic tags for correlation
                try:
                    try:
                        batch_tags = [
                            (
                                getattr(afut, "_cidstore_tag", None),
                                getattr(afut, "_cidstore_wal_time", None),
                            )
                            for afut, _ok, _payload in batch_copy
                        ]
                    except Exception:
                        batch_tags = None
                    print(
                        f"DISPATCH_BATCH: loop_id={id(loop)} batch_count={len(batch_copy)} tags={batch_tags}"
                    )
                except Exception:
                    pass

                def make_cb(batch):
                    def cb():
                        # This callback runs inside the target asyncio loop.
                        for afut, ok, payload in batch:
                            try:
                                if afut.done():
                                    continue
                                try:
                                    logger.debug(
                                        f"_result_dispatcher_loop.cb: setting future for loop id={id(loop)} ok={ok} payload_type={type(payload)} at {time()}"
                                    )
                                except Exception:
                                    pass
                                if ok:
                                    afut.set_result(payload)
                                else:
                                    afut.set_exception(payload)
                            except Exception:
                                # If one future fails to be set, continue with others
                                pass

                    return cb

                try:
                    logger.debug(
                        f"_result_dispatcher_loop: scheduling batch to loop id={id(loop)} batch_count={len(batch_copy)} tags={tags}"
                    )
                    loop.call_soon_threadsafe(make_cb(batch_copy))
                except Exception:
                    # If scheduling failed, try to set directly (best-effort)
                    for afut, ok, payload in batch_copy:
                        try:
                            if afut.done():
                                continue
                            if ok:
                                afut.set_result(payload)
                            else:
                                afut.set_exception(payload)
                        except Exception:
                            pass

        logger.info("_result_dispatcher_loop: exiting")

    # Support context manager used in some tests
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.close()
        except Exception:
            pass

    async def apply_delete(self, op: OP, bucket_name: str):
        """Apply a delete operation from the WAL to the storage (async wrapper)."""
        # Submit delete task to the HDF5 worker and await result
        return await self._submit_task(self._apply_delete_sync, op, bucket_name)

    def _apply_delete_sync(self, op: OP, bucket_name: str):
        """Synchronous delete implementation (runs on HDF5 worker thread)."""
        # Prefer the persistent worker-local file handle when available to
        # avoid opening multiple file handles. If not present, open per-op.
        f = getattr(self, "_worker_file", None)
        opened_here = False
        if f is None:
            f = File(self.path, "a", libver="latest")
            opened_here = True
        # We want to avoid holding the storage-level _lock while calling
        # _publish_mem_index (which acquires _mem_index_lock). To prevent
        # potential lock-order inversions, capture the modified entry and
        # wal tag while holding the storage lock, then perform the
        # publication after releasing the lock and after flushing/fsync so
        # readers observe the canonical state.
        published_entry = None
        published_tag = None
        try:
            with self._lock:
                bucket_group = f[BUCKS]
                assert assumption(bucket_group, Group)
                if bucket_name not in bucket_group:
                    return
                bucket_ds = bucket_group[bucket_name]
                assert assumption(bucket_ds, Dataset)
                total = bucket_ds.shape[0]
                for i in range(total):
                    entry = bucket_ds[i]
                    if entry["key_high"] == op.k_high and entry["key_low"] == op.k_low:
                        # zero out slots
                        entry["slots"][0]["high"] = 0
                        entry["slots"][0]["low"] = 0
                        entry["slots"][1]["high"] = 0
                        entry["slots"][1]["low"] = 0
                        bucket_ds[i] = entry
                        try:
                            published_entry = np.copy(entry)
                        except Exception:
                            published_entry = entry
                        try:
                            published_tag = (
                                getattr(op, "_wal_time", None)
                                or getattr(op, "nanos", None)
                                or getattr(op, "seq", None)
                            )
                        except Exception:
                            published_tag = None
                        break
        finally:
            if opened_here:
                try:
                    f.flush()
                except Exception:
                    pass
                else:
                    # Ensure OS-level sync so other freshly-opened reader
                    # handles observe the deletion updates.
                    try:
                        fd = os.open(str(self.path), os.O_RDWR)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                    except Exception:
                        pass
                try:
                    f.close()
                except Exception:
                    pass
        # Publish mem-index after releasing the storage lock (and after
        # flushing/fsync above) to avoid nested lock acquisitions.
        if published_entry is not None:
            try:
                self._publish_mem_index(
                    bucket_name,
                    int(op.k_high),
                    int(getattr(op, "k_high_mid", 0)),
                    int(getattr(op, "k_low_mid", 0)),
                    int(op.k_low),
                    published_entry,
                    published_tag,
                    origin="apply_delete",
                )
            except Exception:
                pass

    @property
    def buckets(self):
        """
        Return the buckets group from the HDF5 file as a dict-like object.
        Ensures the file is open if it was closed.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        return self.file[BUCKS]

    async def apply_insert(self, op: OP, bucket_name: str) -> Dataset:
        """Apply an insert operation from the WAL to the storage."""
        # Run the entire insert logic on the HDF5 worker thread to avoid
        # any cross-thread HDF5 calls. The synchronous helper performs all
        # needed dataset reads/writes and promotions/spills inline.
        return await self._submit_task(self._apply_insert_sync, op, bucket_name)

    def _apply_insert_sync(self, op: OP, bucket_name: str):
        """Synchronous insert implementation (runs on HDF5 worker thread)."""
        # synchronous worker method; no asyncio awaits here
        # operate entirely synchronously on HDF5 objects
        # Prefer persistent worker-local file handle if present
        f = getattr(self, "_worker_file", None)
        opened_here = False
        if f is None:
            f = File(self.path, "a", libver="latest")
            opened_here = True
        try:
            with self._lock:
                bucket_group = f[BUCKS]
                assert assumption(bucket_group, Group)
                bucket_ds = bucket_group[bucket_name]
                assert assumption(bucket_ds, Dataset)

            # Read the entire bucket dataset into memory once to avoid
            # repeated h5py dataset reads which can block or race when
            # concurrent threads resize the dataset. Work on the in-memory
            # numpy view to find existing entries, then write back as needed.
            try:
                logger.debug(
                    f"_apply_insert_sync: reading entire bucket '{bucket_name}' dataset (shape={bucket_ds.shape}) on thread {threading.current_thread().name}"
                )
                t_read0 = time()
                arr = bucket_ds[:]
                t_read1 = time()
                logger.debug(
                    f"_apply_insert_sync: read entire bucket '{bucket_name}' in {t_read1 - t_read0:.3f}s"
                )
            except Exception:
                # If reading the entire dataset fails, fall back to per-item reads
                logger.exception(
                    f"_apply_insert_sync: failed to read entire bucket '{bucket_name}', will fallback to per-item reads"
                )
                arr = None

            if arr is not None:
                total = arr.shape[0]
                entry_idx = None
                # Python loop for structured dtype
                for i in range(total):
                    e = arr[i]
                    if e["key_high"] == op.k_high and e["key_low"] == op.k_low:
                        entry_idx = i
                        break

                if entry_idx is not None:
                    entry = arr[entry_idx]
                    logger.info(
                        f"_apply_insert_sync: found existing entry at idx={entry_idx} for key=({op.k_high},{op.k_low}), slots={entry['slots']}"
                    )
                else:
                    logger.info(
                        f"_apply_insert_sync: creating new entry for key=({op.k_high},{op.k_low})"
                    )
                    entry = new_entry(op.k_high, op.k_high_mid, op.k_low_mid, op.k_low)
            else:
                # Fallback: per-item read
                total = bucket_ds.shape[0]
                entry_idx = None
                for i in range(total):
                    entry = bucket_ds[i]
                    if entry["key_high"] == op.k_high and entry["key_low"] == op.k_low:
                        entry_idx = i
                        break

                if entry_idx is not None:
                    entry = bucket_ds[entry_idx]
                    logger.info(
                        f"_apply_insert_sync: found existing entry at idx={entry_idx} for key=({op.k_high},{op.k_low}) (fallback path), slots={entry['slots']}"
                    )
                else:
                    logger.info(
                        f"_apply_insert_sync: creating new entry for key=({op.k_high},{op.k_low}) (fallback path)"
                    )
                    entry = new_entry(op.k_high, op.k_high_mid, op.k_low_mid, op.k_low)

            assert entry.dtype == HASH_ENTRY_DTYPE, "Entry not HASH_ENTRY_DTYPE"
            # Extract WAL hybrid-time tag from the op if available so
            # we can propagate it into promote/spill helpers and the
            # mem-index publication. Compute early so it's available
            # for decision branches below.
            try:
                tag = (
                    getattr(op, "_wal_time", None)
                    or getattr(op, "nanos", None)
                    or getattr(op, "seq", None)
                )
            except Exception:
                tag = None

            slot0, slot1 = entry["slots"]
            # Handle non-awaiting inline updates immediately
            # Check if slots are empty by comparing all components to zero.
            # Previous implementation only checked the first two components
            # which could treat a slot with low/non-zero component as empty
            # (e.g. (0,0,0,1)). Ensure we check all four fields.
            try:
                slot0_empty = (
                    int(slot0["high"]) == 0
                    and int(slot0.get("high_mid", 0)) == 0
                    and int(slot0.get("low_mid", 0)) == 0
                    and int(slot0.get("low", 0)) == 0
                )
            except Exception:
                # Fallback for array-index style access
                slot0_empty = (
                    int(slot0[0]) == 0
                    and int(slot0[1]) == 0
                    and int(slot0[2]) == 0
                    and int(slot0[3]) == 0
                )

            try:
                slot1_empty = (
                    int(slot1["high"]) == 0
                    and int(slot1.get("high_mid", 0)) == 0
                    and int(slot1.get("low_mid", 0)) == 0
                    and int(slot1.get("low", 0)) == 0
                )
            except Exception:
                slot1_empty = (
                    int(slot1[0]) == 0
                    and int(slot1[1]) == 0
                    and int(slot1[2]) == 0
                    and int(slot1[3]) == 0
                )
            logger.info(
                f"_apply_insert_sync: slot0={slot0} slot1={slot1} slot0_empty={slot0_empty} slot1_empty={slot1_empty}"
            )

            if slot0_empty and slot1_empty:
                # Inline mode, both slots empty, new entry
                logger.info(
                    "_apply_insert_sync: using slot0 for new entry (both slots empty)"
                )
                entry["slots"][0]["high"] = op.v_high
                entry["slots"][0]["high_mid"] = op.v_high_mid
                entry["slots"][0]["low_mid"] = op.v_low_mid
                entry["slots"][0]["low"] = op.v_low
                # If we found an existing slot index for this key, update it in-place
                if entry_idx is not None:
                    logger.debug(
                        f"_apply_insert_sync: updating existing entry at index {entry_idx} for key=({op.k_high},{op.k_low}) value=({op.v_high},{op.v_low})"
                    )
                    try:
                        with self._lock:
                            bucket_ds[entry_idx] = entry
                    except Exception:
                        try:
                            bucket_ds[entry_idx] = entry
                        except Exception:
                            pass
                else:
                    # No existing entry: append new entry by resizing the dataset
                    logger.debug(
                        f"_apply_insert_sync: appending new inline entry to bucket '{bucket_name}', current total={total}"
                    )
                    t_resize0 = time()
                    # Perform dataset resize and write while holding storage lock
                    try:
                        with self._lock:
                            bucket_ds.resize((total + 1,))
                            bucket_ds[total] = entry
                            bucket_ds.attrs["entry_count"] = total + 1
                    except Exception:
                        # Best-effort: if locking or write fails, attempt without lock
                        try:
                            bucket_ds.resize((total + 1,))
                            bucket_ds[total] = entry
                            bucket_ds.attrs["entry_count"] = total + 1
                        except Exception:
                            pass
                    t_resize1 = time()
                    logger.debug(
                        f"_apply_insert_sync: resize took {t_resize1 - t_resize0:.3f}s"
                    )
                    logger.debug(
                        f"_apply_insert_sync: writing new entry at index {total} for key=({op.k_high},{op.k_low}) value=({op.v_high},{op.v_low})"
                    )

            elif not slot0_empty and slot1_empty:
                # Inline mode, first slot used, put in second slot
                logger.info(
                    "_apply_insert_sync: using slot1 for existing entry (slot0 occupied)"
                )
                entry["slots"][1]["high"] = op.v_high
                entry["slots"][1]["high_mid"] = op.v_high_mid
                entry["slots"][1]["low_mid"] = op.v_low_mid
                entry["slots"][1]["low"] = op.v_low
                logger.debug(
                    f"_apply_insert_sync: updating existing entry at idx={entry_idx} to set slot1 for key=({op.k_high},{op.k_low}) value=({op.v_high},{op.v_low})"
                )
                # Update the dataset while holding storage lock to serialize writes
                try:
                    with self._lock:
                        bucket_ds[entry_idx] = entry
                except Exception:
                    try:
                        bucket_ds[entry_idx] = entry
                    except Exception:
                        pass

            else:
                # Cases that require promotion or spill
                if not slot0_empty and not slot1_empty:
                    # Promote to spill: perform synchronously here
                    bucket_id = int(bucket_name.split("_")[-1])
                    assert entry_idx is not None, "Entry index must be set"
                    entry = bucket_ds[entry_idx]
                    # Call synchronous promote helper (file-local)
                    # Pass wal_time tag into promote so the mem-index can
                    # record the same WAL hybrid-time as the original op.
                    self._apply_insert_promote_sync(
                        f,
                        bucket_ds,
                        entry_idx,
                        entry,
                        bucket_id,
                        op.k_high,
                        op.k_low,
                        op.v_high,
                        op.v_high_mid,
                        op.v_low_mid,
                        op.v_low,
                        tag,
                    )
                else:
                    # Spill append: perform synchronously
                    # _apply_insert_spill_sync operates on the entry and value parts;
                    # do not pass the File handle `f` which this helper does not expect.
                    # Pass wal_time tag into spill append helper when available
                    self._apply_insert_spill_sync(
                        entry,
                        op.v_high,
                        op.v_high_mid,
                        op.v_low_mid,
                        op.v_low,
                        tag,
                    )

            # Ensure changes are flushed so separate File handles (opened
            # by readers on other threads) observe the updates.
            try:
                f.flush()
            except Exception:
                logger.debug("_apply_insert_sync: flush failed or not supported")
            else:
                # Ensure OS-level sync so other handles see changes. Do a
                # best-effort fsync regardless of whether we opened the
                # file here to reduce visibility races on some platforms.
                try:
                    fd = os.open(str(self.path), os.O_RDWR)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                except Exception:
                    pass
            # Update in-memory index to reflect the latest entry state so
            # subsequent reads executed on the worker see the change
            try:
                try:
                    tag = (
                        getattr(op, "_wal_time", None)
                        or getattr(op, "nanos", None)
                        or getattr(op, "seq", None)
                    )
                except Exception:
                    tag = None
                logger.info(
                    f"_apply_insert_sync: updating mem_index for key=({bucket_name}, {int(op.k_high)}, {int(op.k_high_mid)}, {int(op.k_low_mid)}, {int(op.k_low)}) with slots={entry['slots']} wal_tag={tag}"
                )
                # Synchronously publish mem-index under mem_index_lock to ensure
                # deterministic visibility to other threads. Acquiring the
                # lock here may block briefly but avoids races from
                # unsynchronized writes.
                try:
                    entry_copy = np.copy(entry)
                    logger.info(
                        f"_apply_insert_sync: storing in mem_index slots={entry_copy['slots']}"
                    )
                    try:
                        self._publish_mem_index(
                            bucket_name,
                            int(op.k_high),
                            int(op.k_high_mid),
                            int(op.k_low_mid),
                            int(op.k_low),
                            entry_copy,
                            tag,
                            origin="apply_insert",
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
            except Exception:
                pass
        finally:
            if opened_here:
                from contextlib import suppress

                with suppress(Exception):
                    f.close()

        assert assumption(bucket_ds, Dataset)
        return bucket_ds

    def find_entry_in_bucket_sync(
        self,
        bucket_name: str,
        key_high: int,
        key_high_mid: int,
        key_low_mid: int,
        key_low: int,
    ):
        """Find and return a (copied) entry in the given bucket or None."""
        # First consult the worker-local in-memory index for deterministic
        # in-process visibility of recent writes executed on the worker.
        try:
            with self._mem_index_lock:
                m = self._mem_index.get((
                    bucket_name,
                    int(key_high),
                    int(key_high_mid),
                    int(key_low_mid),
                    int(key_low),
                ))
                if m is not None:
                    try:
                        logger.debug(
                            f"find_entry_in_bucket_sync: mem_index HIT for {(bucket_name, int(key_high), int(key_high_mid), int(key_low_mid), int(key_low))}, entry={m}"
                        )
                    except Exception:
                        pass
                    return np.copy(m)
        except Exception:
            # If mem-index is not available or fails, fall back to file scan
            pass

        # Open the file inside the worker thread to avoid using self.file
        # (which may have been opened on another thread).
        # Open reader with SWMR enabled so it can observe writer updates.
        # Call `refresh()` to update metadata before scanning datasets.
        # Open reader in this worker thread. If SWMR was enabled on the writer
        # the Storage.open() call sets `self._swmr_enabled` and readers can
        # request `swmr=True` and call `refresh()` to observe updates. If SWMR
        # is not enabled or not supported, open without swmr.
        # Prefer using the persistent worker-local file handle when available
        f = getattr(self, "_worker_file", None)
        opened_here = False
        if f is None:
            swmr_flag = getattr(self, "_swmr_enabled", False)
            f = File(self.path, "r", libver="latest", swmr=swmr_flag)
            opened_here = True
        try:
            # Ensure reader refresh to see latest changes from writer
            from contextlib import suppress

            with suppress(Exception):
                f.refresh()

            # Attempt to read and scan the bucket. If not found, perform a
            # short retry loop (reopen or refresh) to handle platforms where
            # metadata propagation to new handles can lag slightly.
            attempts = 0
            # Reduce attempts to avoid long blocking in the worker thread.
            # The async callers already retry/await the worker; keep
            # a small number of local retries here to handle transient
            # visibility windows but avoid multi-second sleeps.
            max_attempts = 10
            while True:
                # Read/scan the HDF5 structures without holding the storage lock
                # to avoid blocking other threads for long-running I/O. Only
                # touch _mem_index under the lock.
                try:
                    bucket_group = f[BUCKS]
                    assert assumption(bucket_group, Group)
                    if bucket_name not in bucket_group:
                        found = None
                    else:
                        bucket_ds = bucket_group[bucket_name]
                        assert assumption(bucket_ds, Dataset)
                        try:
                            logger.debug(
                                f"find_entry_in_bucket_sync: reading entire bucket '{bucket_name}' dataset (shape={bucket_ds.shape}) on thread {threading.current_thread().name}"
                            )
                            t0 = time()
                            arr = bucket_ds[:]
                            t1 = time()
                            logger.debug(
                                f"find_entry_in_bucket_sync: read entire bucket '{bucket_name}' in {t1 - t0:.3f}s"
                            )
                        except Exception:
                            logger.exception(
                                f"find_entry_in_bucket_sync: failed to read entire bucket '{bucket_name}', will fallback to per-item reads"
                            )
                            arr = None

                        if arr is not None:
                            found = None
                            for i in range(arr.shape[0]):
                                e = arr[i]
                                if (
                                    e["key_high"] == key_high
                                    and e["key_low"] == key_low
                                ):
                                    found = np.copy(e)
                                    break
                        else:
                            total = bucket_ds.shape[0]
                            found = None
                            for i in range(total):
                                e = bucket_ds[i]
                                if (
                                    e["key_high"] == key_high
                                    and e["key_low"] == key_low
                                ):
                                    found = np.copy(e)
                                    break
                except Exception:
                    # If any file access error occurs treat as not found this
                    # attempt and continue with the retry/backoff loop.
                    found = None

                if found is not None:
                    return found

                # Mem-index lookup missed; log a compact snapshot of mem-index
                try:
                    with self._mem_index_lock:
                        # Collect up to 8 keys from mem-index that share the bucket
                        nearby = [
                            k
                            for k in list(self._mem_index.keys())
                            if k[0] == bucket_name
                        ][:8]
                        logger.debug(
                            f"find_entry_in_bucket_sync: mem_index MISS for {(bucket_name, int(key_high), int(key_low))}; nearby_mem_index_keys={nearby}"
                        )
                except Exception:
                    pass

                # Not found: retry a few times to allow writer visibility.
                attempts += 1
                if attempts > max_attempts:
                    return None

                # Exponential backoff sleep before retrying; reopen reader if
                # we opened the file here to ensure a fresh handle sees
                # persisted metadata. Keep sleeps short to avoid blocking
                # the worker thread for long periods; async callers may
                # perform additional retries.
                from contextlib import suppress

                with suppress(Exception):
                    sleep_for = min(0.002 * attempts, 0.02)
                    time.sleep(sleep_for)

                from contextlib import suppress

                with suppress(Exception):
                    if opened_here:
                        # Close and reopen a fresh reader handle
                        try:
                            f.close()
                        except Exception:
                            pass
                        swmr_flag = getattr(self, "_swmr_enabled", False)
                        f = File(self.path, "r", libver="latest", swmr=swmr_flag)
                        opened_here = True
                    else:
                        try:
                            f.refresh()
                        except Exception:
                            pass
        finally:
            # Close per-operation file handles opened here to avoid leaking
            # HDF5 File objects created on the worker thread.
            if opened_here:
                try:
                    f.close()
                except Exception:
                    pass

    def get_values_sync(self, entry) -> list[E]:
        """Synchronous get_values (safe to call on HDF5 worker thread)."""
        slots = entry["slots"]
        # Spill mode: slot0 is all zeros and slot1 points to a valueset dataset
        if (
            int(slots[0]["high"]) == 0
            and int(slots[0]["high_mid"]) == 0
            and int(slots[0]["low_mid"]) == 0
            and int(slots[0]["low"]) == 0
            and (
                int(slots[1]["high"]) != 0
                or int(slots[1]["high_mid"]) != 0
                or int(slots[1]["low_mid"]) != 0
                or int(slots[1]["low"]) != 0
            )
        ):
            slot = slots[1]
            bucket_id = int(slot["high"])
            key_high = int(slot["low"])
            key_low = int(entry["key_low"])
            ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)

            # Prefer the persistent worker file when available; otherwise
            # open a per-operation reader and close it afterwards.
            f = getattr(self, "_worker_file", None)
            opened_here = False
            if f is None:
                swmr_flag = getattr(self, "_swmr_enabled", False)
                f = File(self.path, "r", libver="latest", swmr=swmr_flag)
                opened_here = True
            try:
                try:
                    f.refresh()
                except Exception:
                    pass

                if "/values" not in f or "sp" not in f["/values"]:
                    return []
                sp_group = f["/values/sp"]
                if ds_name not in sp_group:
                    return []
                valueset_ds = sp_group[ds_name]
                raw = valueset_ds[:]

                import collections.abc

                if not isinstance(raw, collections.abc.Iterable) or isinstance(
                    raw, (str, bytes)
                ):
                    raw = [raw]

                # Each element in 'raw' is structured with fields high, high_mid, low_mid, low
                decoded = [
                    E(
                        int(v["high"]),
                        int(v["high_mid"]),
                        int(v["low_mid"]),
                        int(v["low"]),
                    )
                    for v in raw
                ]
                try:
                    logger.debug(
                        f"get_values_sync: returning {len(decoded)} values for entry key=({int(entry['key_high'])},{int(entry['key_low'])}) from spill {ds_name}"
                    )
                except Exception:
                    pass
                return decoded
            finally:
                if opened_here:
                    try:
                        f.close()
                    except Exception:
                        pass

        values = []
        values = []
        for slot in slots:
            high = int(slot["high"])
            high_mid = int(slot["high_mid"])
            low_mid = int(slot["low_mid"])
            low = int(slot["low"])
            # Skip empty slots (all components zero) which represent
            # unused inline value locations. Returning E(0) for empty
            # slots produces spurious values after recovery.
            if high == 0 and high_mid == 0 and low_mid == 0 and low == 0:
                continue
            values.append(E(high, high_mid, low_mid, low))
        try:
            logger.debug(
                f"get_values_sync: returning {len(values)} inline values for entry key=({int(entry['key_high'])},{int(entry['key_low'])})"
            )
        except Exception:
            pass
        return values

    async def find_entry(
        self,
        bucket_name: str,
        key_high: int,
        key_high_mid: int,
        key_low_mid: int,
        key_low: int,
    ):
        return await self._submit_task(
            self.find_entry_in_bucket_sync,
            bucket_name,
            key_high,
            key_high_mid,
            key_low_mid,
            key_low,
        )

    async def get_values_async(self, entry):
        return await self._submit_task(self.get_values_sync, entry)

    async def ensure_mem_index(
        self,
        bucket_name: str,
        key_high: int,
        key_high_mid: int,
        key_low_mid: int,
        key_low: int,
    ):
        """Ensure the worker-local mem-index contains the canonical entry for the given key.

        This runs on the HDF5 worker thread and deterministically reads the canonical
        bucket dataset entry and stores a numpy.copy into self._mem_index under lock.
        """
        return await self._submit_task(
            self._ensure_mem_index_sync,
            bucket_name,
            key_high,
            key_high_mid,
            key_low_mid,
            key_low,
        )

    def _worker_noop_sync(self):
        """A tiny no-op function executed on the worker thread used to drain the worker queue."""
        return True

    async def drain_worker(self):
        """Submit a no-op to the worker and await its completion. Useful to ensure
        previously-submitted tasks have completed (testing/determinism helper)."""
        return await self._submit_task(self._worker_noop_sync)

    def _ensure_mem_index_sync(
        self,
        bucket_name: str,
        key_high: int,
        key_high_mid: int,
        key_low_mid: int,
        key_low: int,
    ):
        """Synchronous helper executed on the worker thread to update mem-index."""
        f = getattr(self, "_worker_file", None)
        opened_here = False
        if f is None:
            # Open a reader so we can inspect the canonical bucket dataset
            swmr_flag = getattr(self, "_swmr_enabled", False)
            f = File(self.path, "r", libver="latest", swmr=swmr_flag)
            opened_here = True
        try:
            try:
                f.refresh()
            except Exception:
                pass
            # Try to read and publish the canonical entry. Use a few attempts
            # with refresh/reopen/backoff to tolerate file visibility delays
            # on some platforms or VFDs.
            attempts = 0
            # Increase attempts to tolerate short filesystem/VFD visibility
            # delays under heavy concurrency. Keep sleeps bounded so the
            # worker thread is not blocked for long periods.
            max_attempts = 10
            while True:
                # Scan the canonical bucket dataset without holding the storage
                # lock to avoid blocking other threads. Acquire the lock only
                # when updating the in-memory index.
                try:
                    bucket_group = f[BUCKS]
                    if bucket_name not in bucket_group:
                        return
                    bucket_ds = bucket_group[bucket_name]
                    total = bucket_ds.shape[0]
                    found_entry = None
                    for i in range(total):
                        e = bucket_ds[i]
                        if (
                            int(e["key_high"]) == int(key_high)
                            and int(e.get("key_high_mid", 0))
                            == int(
                                getattr(key_high, "high_mid", 0)
                                if hasattr(key_high, "high_mid")
                                else 0
                            )
                            and int(e.get("key_low_mid", 0))
                            == int(
                                getattr(key_low, "low_mid", 0)
                                if hasattr(key_low, "low_mid")
                                else 0
                            )
                            and int(e["key_low"]) == int(key_low)
                        ):
                            found_entry = np.copy(e)
                            break
                    # Lightweight instrumentation: log scan progress occasionally
                    try:
                        if (
                            attempts == 0
                            or (attempts < 5 and i % 10 == 0)
                            or (i % 50 == 0)
                        ):
                            logger.debug(
                                f"_ensure_mem_index_sync: scanning bucket={bucket_name} idx={i}/{total} attempts={attempts}"
                            )
                    except Exception:
                        pass
                    if found_entry is not None:
                        try:
                            # found_entry was already copied from the dataset above
                            # Avoid doing another np.copy() while holding the lock.
                            published_entry = found_entry
                            # Log diagnostic information about the publication
                            try:
                                swmr_flag = getattr(
                                    f,
                                    "swmr_mode",
                                    getattr(self, "_swmr_enabled", False),
                                )
                            except Exception:
                                swmr_flag = getattr(self, "_swmr_enabled", False)
                            try:
                                # Capture a brief snapshot of the bucket around the found index
                                snap = None
                                try:
                                    # attempt to read a small window of the bucket for context
                                    start = max(0, i - 2)
                                    end = min(total, i + 3)
                                    window = [bucket_ds[j] for j in range(start, end)]
                                    snap = [tuple(w["slots"]) for w in window]
                                except Exception:
                                    snap = None
                                logger.info(
                                    f"_ensure_mem_index_sync: publishing mem_index for key={(bucket_name, int(key_high), int(getattr(key_high, 'high_mid', 0) if hasattr(key_high, 'high_mid') else 0), int(getattr(key_low, 'low_mid', 0) if hasattr(key_low, 'low_mid') else 0), int(key_low))} attempts={attempts} swmr={swmr_flag} snapshot={snap}"
                                )
                            except Exception:
                                pass

                            # Update mem-index under lock for deterministic publication
                            # Only update if the key is not already in mem_index to avoid
                            # overwriting more recent data from _apply_insert_sync
                            try:
                                with self._mem_index_lock:
                                    existing = self._mem_index.get((
                                        bucket_name,
                                        int(key_high),
                                        int(
                                            getattr(key_high, "high_mid", 0)
                                            if hasattr(key_high, "high_mid")
                                            else 0
                                        ),
                                        int(
                                            getattr(key_low, "low_mid", 0)
                                            if hasattr(key_low, "low_mid")
                                            else 0
                                        ),
                                        int(key_low),
                                    ))
                                    # If there is no existing in-memory entry, publish
                                    # the canonical file entry into the mem-index.
                                    if existing is None:
                                        try:
                                            # Use centralized helper; no WAL-time for file-read
                                            self._publish_mem_index(
                                                bucket_name,
                                                int(key_high),
                                                int(
                                                    getattr(key_high, "high_mid", 0)
                                                    if hasattr(key_high, "high_mid")
                                                    else 0
                                                ),
                                                int(
                                                    getattr(key_low, "low_mid", 0)
                                                    if hasattr(key_low, "low_mid")
                                                    else 0
                                                ),
                                                int(key_low),
                                                published_entry,
                                                None,
                                                origin="ensure_mem_index",
                                            )
                                            logger.debug(
                                                "_ensure_mem_index_sync: added new entry to mem_index via _publish_mem_index"
                                            )
                                        except Exception:
                                            # Fallback: assign under lock (we already hold it)
                                            try:
                                                self._mem_index[
                                                    (
                                                        bucket_name,
                                                        int(key_high),
                                                        int(
                                                            getattr(
                                                                key_high, "high_mid", 0
                                                            )
                                                            if hasattr(
                                                                key_high, "high_mid"
                                                            )
                                                            else 0
                                                        ),
                                                        int(
                                                            getattr(
                                                                key_low, "low_mid", 0
                                                            )
                                                            if hasattr(
                                                                key_low, "low_mid"
                                                            )
                                                            else 0
                                                        ),
                                                        int(key_low),
                                                    )
                                                ] = published_entry
                                            except Exception:
                                                pass
                                            try:
                                                self._mem_index_wal_times[
                                                    (
                                                        bucket_name,
                                                        int(key_high),
                                                        int(
                                                            getattr(
                                                                key_high, "high_mid", 0
                                                            )
                                                            if hasattr(
                                                                key_high, "high_mid"
                                                            )
                                                            else 0
                                                        ),
                                                        int(
                                                            getattr(
                                                                key_low, "low_mid", 0
                                                            )
                                                            if hasattr(
                                                                key_low, "low_mid"
                                                            )
                                                            else 0
                                                        ),
                                                        int(key_low),
                                                    )
                                                ] = None
                                            except Exception:
                                                pass
                                            logger.debug(
                                                "_ensure_mem_index_sync: added new entry to mem_index via fallback atomic assignment"
                                            )
                                    else:
                                        # Existing entry present; to avoid races between
                                        # concurrent worker-driven publishes and file-read
                                        # visibility updates, do not overwrite an existing
                                        # mem-index entry here. Worker paths that modify
                                        # the file are responsible for publishing the
                                        # canonical state via _publish_mem_index after
                                        # they flush/fsync. Skipping the overwrite here
                                        # prevents file-read races from introducing
                                        # duplicate or stale visible values.
                                        try:
                                            logger.debug(
                                                "_ensure_mem_index_sync: existing mem_index entry present; skipping file-read overwrite"
                                            )
                                        except Exception:
                                            pass
                            except Exception:
                                # Fallback: try to publish via the centralized helper
                                # which itself will attempt locked assignment and
                                # best-effort fallbacks. This avoids performing
                                # unsynchronized assignments here.
                                try:
                                    try:
                                        self._publish_mem_index(
                                            bucket_name,
                                            int(key_high),
                                            int(key_low),
                                            published_entry,
                                            None,
                                            origin="ensure_mem_index",
                                        )
                                    except Exception:
                                        # If helper fails for any reason, swallow and continue
                                        pass
                                except Exception:
                                    pass

                            try:
                                logger.debug(
                                    f"_ensure_mem_index_sync: published mem_index key={(bucket_name, int(key_high), int(key_low))} entry={found_entry} attempts={attempts}"
                                )
                            except Exception:
                                pass
                        except Exception:
                            # best-effort: swallow errors while publishing
                            pass
                        return
                except Exception:
                    # best-effort: do not raise on file access errors
                    pass

                attempts += 1
                if attempts > max_attempts:
                    return

                # Backoff and refresh/reopen reader to increase chance of seeing
                # writer-visible updates on slow filesystems/VFDs. Keep sleeps
                # short to avoid excessively blocking the worker thread.
                try:
                    # Slightly larger backoff multiplier to give the OS
                    # and filesystem more time to make writer-visible
                    # updates available to freshly-opened reader handles.
                    time.sleep(min(0.005 * attempts, 0.05))
                except Exception:
                    pass
                try:
                    if opened_here:
                        try:
                            f.close()
                        except Exception:
                            pass
                        swmr_flag = getattr(self, "_swmr_enabled", False)
                        f = File(self.path, "r", libver="latest", swmr=swmr_flag)
                        opened_here = True
                    else:
                        try:
                            f.refresh()
                        except Exception:
                            pass
                except Exception:
                    pass
        finally:
            if opened_here:
                try:
                    f.close()
                except Exception:
                    pass

    async def apply_insert_promote(
        self,
        bucket_ds: Dataset,
        entry_idx: int,
        entry,
        bucket_id: int,
        key_high: int,
        key_low: int,
        value_high: int,
        value_high_mid: int,
        value_low_mid: int,
        value_low: int,
        wal_time=None,
    ) -> None:
        """Promote inline entry to ValueSet (spill) mode when both slots are full."""
        # Delegate to synchronous worker which performs HDF5 I/O
        await self._submit_task(
            self._apply_insert_promote_sync,
            self.file,
            bucket_ds,
            entry_idx,
            entry,
            bucket_id,
            key_high,
            key_low,
            value_high,
            value_high_mid,
            value_low_mid,
            value_low,
            wal_time,
        )

    def _apply_insert_promote_sync(
        self,
        f: File,
        bucket_ds: Dataset,
        entry_idx: int,
        entry,
        bucket_id: int,
        key_high: int,
        key_low: int,
        value_high: int,
        value_high_mid: int,
        value_low_mid: int,
        value_low: int,
        wal_time=None,
    ) -> None:
        """Synchronous promote implementation (runs on HDF5 worker thread)."""
        # Perform file operations while holding storage lock to serialize
        # dataset creation and updates. Capture the canonical promoted entry
        # and wal_time while under the lock, but publish to the mem-index
        # after releasing the lock to avoid nested lock acquisition.
        published_entry = None
        try:
            with self._lock:
                slots = bucket_ds[entry_idx]["slots"]

                def slot_to_tuple(slot):
                    return (
                        int(slot["high"]),
                        int(slot["high_mid"]),
                        int(slot["low_mid"]),
                        int(slot["low"]),
                    )

                value1 = slot_to_tuple(slots[0])
                value2 = slot_to_tuple(slots[1])
                new_value = (
                    int(value_high),
                    int(value_high_mid),
                    int(value_low_mid),
                    int(value_low),
                )
                values = [value1, value2, new_value]

                sp_group = self._get_valueset_group_file(f)
                ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)

                if ds_name in sp_group:
                    del sp_group[ds_name]

                spill_dtype = np.dtype([
                    ("high", "<u8"),
                    ("high_mid", "<u8"),
                    ("low_mid", "<u8"),
                    ("low", "<u8"),
                ])

                ds = sp_group.create_dataset(
                    ds_name, shape=(len(values),), maxshape=(None,), dtype=spill_dtype
                )
                encoded = []
                for v in values:
                    try:
                        if isinstance(v, tuple) and len(v) == 4:
                            h, hm, lm, l = (int(x) for x in v)
                        elif isinstance(v, tuple) and len(v) == 2:
                            h = int(v[0])
                            hm = 0
                            lm = 0
                            l = int(v[1])
                        else:
                            iv = int(v)
                            mask = (1 << 64) - 1
                            l = iv & mask
                            lm = (iv >> 64) & mask
                            hm = (iv >> 128) & mask
                            h = (iv >> 192) & mask
                    except Exception:
                        h = 0
                        hm = 0
                        lm = 0
                        l = 0
                    encoded.append((h, hm, lm, l))

                ds[:] = encoded

                # Store the dataset name as the spill pointer (compact compatibility):
                entry["slots"][0]["high"] = 0
                entry["slots"][0]["high_mid"] = 0
                entry["slots"][0]["low_mid"] = 0
                entry["slots"][0]["low"] = 0
                entry["slots"][1]["high"] = bucket_id
                entry["slots"][1]["high_mid"] = 0
                entry["slots"][1]["low_mid"] = 0
                entry["slots"][1]["low"] = key_high

                bucket_ds[entry_idx] = entry
                try:
                    f.flush()
                except Exception:
                    logger.debug(
                        "_apply_insert_promote_sync: flush failed or not supported"
                    )
                else:
                    # Best-effort OS-level sync so fresh readers observe dataset
                    try:
                        fd = os.open(str(self.path), os.O_RDWR)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                    except Exception:
                        pass
                logger.debug(
                    f"_apply_insert_promote_sync: created spill ds {ds_name} len={len(ds)} entry_idx={entry_idx} entry={entry}"
                )
                try:
                    published_entry = np.copy(entry)
                except Exception:
                    published_entry = entry
        except Exception:
            # best-effort: if file ops fail, continue without publishing
            published_entry = None

        # Publish mem-index after releasing the storage lock
        if published_entry is not None:
            try:
                try:
                    bucket_name = bucket_ds.name.split("/")[-1]
                except Exception:
                    bucket_name = None
                if bucket_name:
                    try:
                        self._publish_mem_index(
                            bucket_name,
                            int(key_high),
                            int(
                                getattr(key_high, "high_mid", 0)
                                if hasattr(key_high, "high_mid")
                                else 0
                            ),
                            int(
                                getattr(key_low, "low_mid", 0)
                                if hasattr(key_low, "low_mid")
                                else 0
                            ),
                            int(key_low),
                            published_entry,
                            wal_time,
                            origin="apply_insert_promote",
                        )
                    except Exception:
                        pass
            except Exception:
                pass

    def _get_valueset_group_file(self, f: File):
        """Get or create the valueset group for spill datasets on provided file."""
        with self._lock:
            if "/values" in f and not isinstance(f["/values"], Group):
                del f["/values"]
            if "/values" not in f:
                f.create_group("/values")
            values_group = f["/values"]
            assert assumption(values_group, Group)
            # Ensure /values/sp is a group
            if "sp" in values_group and not isinstance(values_group["sp"], Group):
                del values_group["sp"]
            if "sp" not in values_group:
                values_group.create_group("sp")
            return values_group["sp"]

    def _publish_mem_index(
        self,
        bucket_name: str,
        k_high: int,
        k_high_mid: int,
        k_low_mid: int,
        k_low: int,
        entry,
        wal_time=None,
        origin: str | None = None,
        force: bool = False,
    ):
        """Helper to publish an entry into the worker-local mem-index.

        Ensures the assignment and the corresponding WAL time are set while
        holding the mem-index lock. Performs best-effort fallbacks if locking
        or copy operations fail so we don't drop visibility updates in the
        worker thread.
        """
        try:
            entry_copy = np.copy(entry)
        except Exception:
            entry_copy = entry

        # Primary: set under the mem-index lock for atomicity
        try:
            with self._mem_index_lock:
                key = (
                    bucket_name,
                    int(k_high),
                    int(k_high_mid),
                    int(k_low_mid),
                    int(k_low),
                )
                # Existing WAL time (if any) for this in-memory entry
                existing_time = self._mem_index_wal_times.get(key)
                # Extra diagnostics: capture existing entry
                try:
                    existing_entry = self._mem_index.get(key)
                except Exception:
                    existing_entry = None
                try:
                    logger.debug(
                        f"PUBLISH_MEM_INDEX.DEBUG: key={key} origin={origin} incoming_wal_time={wal_time} existing_wal_time={existing_time} existing_entry_slots={getattr(existing_entry, 'slots', None)}"
                    )
                except Exception:
                    try:
                        logger.debug(
                            f"PUBLISH_MEM_INDEX.DEBUG: key={key} origin={origin} incoming_wal_time={wal_time} existing_wal_time={existing_time}"
                        )
                    except Exception:
                        pass

                # If not forcing, and incoming wal_time is None (a file-read),
                # avoid overwriting an existing worker-published entry.
                if not force and wal_time is None and key in self._mem_index:
                    try:
                        self._publish_seq += 1
                        seq = int(self._publish_seq)
                    except Exception:
                        seq = None
                    try:
                        caller = inspect.stack()[1]
                        caller_info = (
                            f"{caller.filename}:{caller.lineno}:{caller.function}"
                        )
                    except Exception:
                        caller_info = None
                    try:
                        logger.info(
                            f"PUBLISH_MEM_INDEX: seq={seq} key={key} wal_time={wal_time} origin={origin} under_lock=True SKIPPED_OVERWRITE existing_wal_time={existing_time} caller={caller_info} slots={entry_copy.get('slots', None)}"
                        )
                    except Exception:
                        try:
                            logger.info(
                                f"PUBLISH_MEM_INDEX: key={key} wal_time={wal_time} origin={origin} under_lock=True SKIPPED_OVERWRITE existing_wal_time={existing_time} caller={caller_info}"
                            )
                        except Exception:
                            pass
                    return

                # Log previous entry for correlation before overwrite
                try:
                    prev = self._mem_index.get(key)
                except Exception:
                    prev = None
                try:
                    logger.debug(
                        f"PUBLISH_MEM_INDEX.DEBUG_WRITE: key={key} origin={origin} prev_wal_time={existing_time} prev_slots={getattr(prev, 'slots', None)} -> new_wal_time={wal_time} new_slots={entry_copy.get('slots', None)}"
                    )
                except Exception:
                    try:
                        logger.debug(
                            f"PUBLISH_MEM_INDEX.DEBUG_WRITE: key={key} origin={origin} prev_wal_time={existing_time} -> new_wal_time={wal_time}"
                        )
                    except Exception:
                        pass

                self._mem_index[key] = entry_copy
                try:
                    self._mem_index_wal_times[key] = wal_time
                except Exception:
                    pass
                try:
                    self._publish_seq += 1
                    seq = int(self._publish_seq)
                except Exception:
                    seq = None
                try:
                    caller = inspect.stack()[1]
                    caller_info = f"{caller.filename}:{caller.lineno}:{caller.function}"
                except Exception:
                    caller_info = None
                try:
                    logger.info(
                        f"PUBLISH_MEM_INDEX: seq={seq} key={key} wal_time={wal_time} origin={origin} under_lock=True caller={caller_info} slots={entry_copy.get('slots', None)}"
                    )
                except Exception:
                    try:
                        logger.info(
                            f"PUBLISH_MEM_INDEX: key={key} wal_time={wal_time} origin={origin} under_lock=True caller={caller_info}"
                        )
                    except Exception:
                        pass
            return
        except Exception:
            # Retry acquiring the lock in case of transient errors
            try:
                with self._mem_index_lock:
                    key = (
                        bucket_name,
                        int(k_high),
                        int(k_high_mid),
                        int(k_low_mid),
                        int(k_low),
                    )
                    self._mem_index[key] = entry_copy
                    try:
                        self._mem_index_wal_times[key] = wal_time
                    except Exception:
                        pass
                try:
                    self._publish_seq += 1
                    seq = int(self._publish_seq)
                except Exception:
                    seq = None
                try:
                    logger.info(
                        f"PUBLISH_MEM_INDEX: seq={seq} key={key} wal_time={wal_time} origin={origin} under_lock=True (retry) slots={entry_copy.get('slots', None)}"
                    )
                except Exception:
                    try:
                        logger.info(
                            f"PUBLISH_MEM_INDEX: key={key} wal_time={wal_time} origin={origin} under_lock=True (retry)"
                        )
                    except Exception:
                        pass
                return
            except Exception:
                try:
                    logger.warning(
                        f"PUBLISH_MEM_INDEX: key={(bucket_name, int(k_high), int(k_high_mid), int(k_low_mid), int(k_low))} wal_time={wal_time} origin={origin} under_lock=False SKIPPED_WRITE"
                    )
                except Exception:
                    try:
                        logger.warning(
                            f"PUBLISH_MEM_INDEX: key={(bucket_name, int(k_high), int(k_high_mid), int(k_low_mid), int(k_low))} wal_time={wal_time} origin={origin} under_lock=False SKIPPED_WRITE"
                        )
                    except Exception:
                        pass

    def get_values(self, entry) -> list[E]:
        """
        Given a bucket entry, return all values for that entry (inline or spilled).
        Handles both inline and spill mode.
        """
        slots = entry["slots"]
        # Check for spill mode: slot0 is zero and slot1 points to a valueset dataset
        if (
            int(slots[0]["high"]) == 0
            and int(slots[0]["low"]) == 0
            and (int(slots[1]["high"]) != 0 or int(slots[1]["low"]) != 0)
        ):
            slot = slots[1]
            bucket_id = int(slot["high"])
            key_high = int(slot["low"])
            key_low = int(entry["key_low"])
            ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)

            # Prefer the persistent worker file when available; otherwise
            # open a per-operation reader and close it afterwards.
            f = getattr(self, "_worker_file", None)
            opened_here = False
            if f is None:
                swmr_flag = getattr(self, "_swmr_enabled", False)
                f = File(self.path, "r", libver="latest", swmr=swmr_flag)
                opened_here = True
            try:
                try:
                    f.refresh()
                except Exception:
                    pass

                if "/values" not in f or "sp" not in f["/values"]:
                    return []
                sp_group = f["/values/sp"]
                if ds_name not in sp_group:
                    return []
                valueset_ds = sp_group[ds_name]
                raw = valueset_ds[:]

                import collections.abc

                if not isinstance(raw, collections.abc.Iterable) or isinstance(
                    raw, (str, bytes)
                ):
                    raw = [raw]

                decoded = [
                    E.from_int(
                        (int(v["high"]) << 192)
                        | (int(v.get("high_mid", 0)) << 128)
                        | (int(v.get("low_mid", 0)) << 64)
                        | int(v["low"])
                    )
                    for v in raw
                    if int(v["high"]) != 0 or int(v["low"]) != 0
                ]
                try:
                    logger.debug(
                        f"get_values_sync: returning {len(decoded)} values for entry key=({int(entry['key_high'])},{int(entry['key_low'])}) from spill {ds_name}"
                    )
                except Exception:
                    pass
                return decoded
            finally:
                if opened_here:
                    try:
                        f.close()
                    except Exception:
                        pass
        # Inline mode: slots are structured arrays with 4 components each
        values = []
        for slot in slots:
            high = int(slot["high"])
            high_mid = int(slot.get("high_mid", 0))
            low_mid = int(slot.get("low_mid", 0))
            low = int(slot["low"])
            # Skip empty slots (all zeros)
            if high == 0 and high_mid == 0 and low_mid == 0 and low == 0:
                continue
            # Reconstruct 256-bit value from 4 components
            value_int = (high << 192) | (high_mid << 128) | (low_mid << 64) | low
            values.append(E.from_int(value_int))
        return values

    async def apply_insert_spill(
        self,
        entry,
        value_high: int,
        value_high_mid: int,
        value_low_mid: int,
        value_low: int,
    ) -> None:
        """
        Insert a new value into an existing spill (valueset) dataset for the given entry.
        """
        # Delegate the actual HDF5 operations to the worker thread so
        # we do not touch h5py objects on the asyncio/main thread.
        await self._submit_task(
            self._apply_insert_spill_sync,
            entry,
            value_high,
            value_high_mid,
            value_low_mid,
            value_low,
            None,
        )

    def _apply_insert_spill_sync(
        self,
        entry,
        value_high: int,
        value_high_mid: int,
        value_low_mid: int,
        value_low: int,
        wal_time=None,
    ) -> None:
        """Synchronous spill-append implementation (runs on HDF5 worker thread)."""
        slot = entry["slots"][1]
        bucket_id = int(slot["high"])
        key_high = int(slot["low"])
        key_low = int(entry["key_low"])
        ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)
        # Open the HDF5 file in this worker thread and operate on file-local
        # group/dataset objects. Use the file-based helper to get/create the
        # values/sp group.
        from h5py import Dataset, Group

        f = getattr(self, "_worker_file", None)
        opened_here = False
        if f is None:
            f = File(self.path, "a", libver="latest")
            opened_here = True
        try:
            sp_group = self._get_valueset_group_file(f)

            assert isinstance(sp_group, Group), (
                f"sp_group is not a Group: {type(sp_group)}"
            )
            if ds_name not in sp_group:
                raise RuntimeError(
                    f"Spill dataset {ds_name} not found for entry {entry}"
                )
            ds = sp_group[ds_name]
            assert isinstance(ds, Dataset), (
                f"Spill dataset {ds_name} is not a Dataset: {type(ds)}"
            )
            # Read current values and append under storage lock to serialize
            # with other potential concurrent writes to the same dataset.
            with self._lock:
                current = ds[:]
                # Prepare new value as 4-tuple
                new_value = (
                    int(value_high),
                    int(value_high_mid),
                    int(value_low_mid),
                    int(value_low),
                )
                # Append new value
                new_len = len(current) + 1
                ds.resize((new_len,))
                ds[-1] = new_value
                try:
                    f.flush()
                except Exception:
                    logger.debug(
                        "_apply_insert_spill_sync: flush failed or not supported"
                    )
                else:
                    # Best-effort OS-level fsync so fresh readers observe the
                    # appended value regardless of file open origin.
                    try:
                        fd = os.open(str(self.path), os.O_RDWR)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                    except Exception:
                        pass
            # Debug: log the new length of the spill dataset and entry state
            try:
                logger.debug(
                    f"_apply_insert_spill_sync: appended to {ds_name}, new_len={len(ds)}, entry_key=({key_high},{key_low}), entry={entry}"
                )
            except Exception:
                pass
            # Update mem-index for spill-append modified entry
            try:
                # Deterministically update the worker-local mem-index by
                # reading the canonical entry from the bucket dataset we
                # just modified. Derive the bucket name from the bucket_id.
                k_high = int(entry["key_high"])
                k_low = int(entry["key_low"])
                bucket_name = f"bucket_{bucket_id:04d}"
                # Try to locate the canonical bucket entry and store it
                # into the mem-index so subsequent reads on the worker
                # observe the persisted state.
                updated = False
                try:
                    buckets_group = f[BUCKS]
                    if bucket_name in buckets_group:
                        bucket_ds = buckets_group[bucket_name]
                        # Scan bucket for the matching key (small loops)
                        total = bucket_ds.shape[0]
                        for i in range(total):
                            e = bucket_ds[i]
                            if (
                                int(e["key_high"]) == k_high
                                and int(e["key_low"]) == k_low
                            ):
                                logger.info(
                                    f"_apply_insert_spill_sync: updating mem_index for {(bucket_name, k_high, k_low)} after spill append; canonical_entry_index={i}"
                                )
                                try:
                                    # Prefer to publish using the canonical mids stored
                                    # in the bucket dataset entry (if present).
                                    self._publish_mem_index(
                                        bucket_name,
                                        int(e.get("key_high", k_high)),
                                        int(e.get("key_high_mid", 0)),
                                        int(e.get("key_low_mid", 0)),
                                        int(e.get("key_low", k_low)),
                                        e,
                                        wal_time,
                                        origin="apply_insert_spill",
                                    )
                                except Exception:
                                    try:
                                        # best-effort fallback: publish without mids
                                        self._publish_mem_index(
                                            bucket_name,
                                            k_high,
                                            0,
                                            0,
                                            k_low,
                                            e,
                                            wal_time,
                                            origin="apply_insert_spill",
                                        )
                                    except Exception:
                                        pass
                                updated = True
                                break
                except Exception:
                    updated = False

                # Fallback: if we couldn't read the canonical entry, ensure
                # mem-index is updated for the key explicitly so subsequent
                # reads on the worker see the appended value.
                if not updated:
                    try:
                        try:
                            # Publish without mids as a best-effort fallback
                            self._publish_mem_index(
                                bucket_name,
                                k_high,
                                0,
                                0,
                                k_low,
                                entry,
                                wal_time,
                                origin="apply_insert_spill",
                            )
                        except Exception:
                            pass
                    except Exception:
                        try:
                            # Final fallback: try to find any existing mem_index
                            # keys that match high/low and re-use their mids.
                            for k in list(self._mem_index.keys()):
                                if k[1] == k_high and k[4] == k_low:
                                    try:
                                        self._publish_mem_index(
                                            k[0],
                                            k[1],
                                            k[2],
                                            k[3],
                                            k[4],
                                            entry,
                                            wal_time,
                                        )
                                    except Exception:
                                        pass
                        except Exception:
                            pass
            except Exception:
                pass
        finally:
            if opened_here:
                try:
                    f.close()
                except Exception:
                    pass


# STANDALONE FUNCTIONS


def new_entry(k_high, k_high_mid, k_low_mid, k_low):
    """Make a new entry to the bucket dataset and return the Dataset."""
    entry = np.zeros((), dtype=HASH_ENTRY_DTYPE)
    entry["key_high"] = k_high
    entry["key_high_mid"] = k_high_mid
    entry["key_low_mid"] = k_low_mid
    entry["key_low"] = k_low
    return entry


def _get_spill_ds_name(bucket_id: int, key_high: int, key_low: int) -> str:
    """Generate the spill dataset name for a key."""
    return f"sp_{bucket_id}_{key_high}_{key_low}"


# Enable ZVIC runtime contract enforcement when assertions are on
if __debug__:
    from cidstore.zvic_init import enforce_contracts

    enforce_contracts()
