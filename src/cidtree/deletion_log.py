"""deletion_log.py - Dedicated deletion log and background GC for CIDTree (Spec 7)"""

import h5py
import numpy as np
import threading
import time
from .config import CONFIG_GROUP

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
    def __init__(self, h5file):
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

    def append(self, key_high, key_low, value_high, value_low):
        with self.lock:
            idx = self.ds.shape[0]
            self.ds.resize((idx + 1,))
            self.ds[idx] = (key_high, key_low, value_high, value_low, time.time())
            self.file.flush()

    def scan(self):
        with self.lock:
            return list(self.ds[:])

    def clear(self):
        with self.lock:
            self.ds.resize((0,))
            self.file.flush()

class BackgroundGC(threading.Thread):
    """
    BackgroundGC: Background thread for garbage collection and orphan reclamation.

    - Periodically scans the deletion log and triggers compaction/cleanup.
    - Runs as a daemon thread; can be stopped via stop().
    """
    def __init__(self, tree, interval=60):
        super().__init__(daemon=True)
        self.tree = tree
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.tree.run_gc_once()
            except Exception as e:
                print(f"[GC] Error: {e}")
            self.stop_event.wait(self.interval)

    def stop(self):
        self.stop_event.set()
    def stop(self):
        self.stop_event.set()
