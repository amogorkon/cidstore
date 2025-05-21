"""deletion_log.py - Dedicated deletion log and background GC for CIDTree (Spec 7)"""

import threading
import time
from typing import Any, List

import numpy as np

# Canonical deletion log dtype: key_high, key_low, value_high, value_low, timestamp
DELETION_LOG_DATASET = "/deletion_log"
delete_log_dtype = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("value_high", "<u8"),
    ("value_low", "<u8"),
    ("timestamp", "<f8"),
])


class DeletionLog:
    """
    DeletionLog: Dedicated deletion log for GC/orphan reclamation (Spec 7).

    - Appends all deletions (key/value/timestamp) for background GC.
    - Backed by HDF5 dataset with canonical dtype.
    - Used by BackgroundGC for safe, idempotent orphan cleanup.
    """

    def __init__(self, h5file: Any):
        assert h5file is not None, "h5file must not be None"
        self.file = h5file
        if DELETION_LOG_DATASET not in self.file:
            self.ds = self.file.create_dataset(
                DELETION_LOG_DATASET,
                shape=(0,),
                maxshape=(None,),
                dtype=delete_log_dtype,
                chunks=True,
            )
        else:
            self.ds = self.file[DELETION_LOG_DATASET]
        self.lock = threading.Lock()

    def append(
        self, key_high: int, key_low: int, value_high: int, value_low: int
    ) -> None:
        assert isinstance(key_high, int), "key_high must be int"
        assert isinstance(key_low, int), "key_low must be int"
        assert isinstance(value_high, int), "value_high must be int"
        assert isinstance(value_low, int), "value_low must be int"
        with self.lock:
            idx = self.ds.shape[0]
            self.ds.resize((idx + 1,))
            self.ds[idx] = (key_high, key_low, value_high, value_low, time.time())
            self.file.flush()

    def scan(self) -> List[Any]:
        # No assert needed
        with self.lock:
            return list(self.ds[:])

    def clear(self) -> None:
        # No assert needed
        with self.lock:
            self.ds.resize((0,))
            self.file.flush()


class BackgroundGC(threading.Thread):
    """
    BackgroundGC: Background thread for garbage collection and orphan reclamation.

    - Periodically scans the deletion log and triggers compaction/cleanup.
    - Runs as a daemon thread; can be stopped via stop().
    """

    def __init__(self, tree: Any, interval: int = 60) -> None:
        assert tree is not None, "tree must not be None"
        assert isinstance(interval, int), "interval must be int"
        super().__init__(daemon=True)
        self.tree = tree
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self) -> None:
        # No assert needed
        while not self.stop_event.is_set():
            try:
                self.tree.run_gc_once()
            except Exception as e:
                print(f"[GC] Error: {e}")
            self.stop_event.wait(self.interval)

    def stop(self) -> None:
        # No assert needed
        self.stop_event.set()
