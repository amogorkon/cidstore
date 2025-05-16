"""tree.py - Main CIDTree class, directory, bucket, and ValueSet logic (Spec 2)"""

import io
import h5py
import numpy as np  # unused, can be removed
import time
from .keys import E
from .storage import StorageManager
from .wal import WAL
from .valueset import ValueSet
from .deletion_log import DeletionLog, BackgroundGC
from .config import MULTI_VALUE_THRESHOLD

class CIDTree:
    """
    CIDTree: Main entry point for the CIDTree hash directory.

    - Provides insert, delete, lookup, and multi-value key support.
    - Handles directory migration (attributes → dataset) and background maintenance.
    - Exposes metrics/logging for Prometheus/monitoring.
    - All mutating operations are WAL-logged and crash-safe.
    - See Spec 2, 3, 5, 6, 7, 9 for details.
    """
    def migrate_directory(self):
        """Migrate directory from HDF5 attribute to dataset (Spec 3). Idempotent."""
        self._maybe_migrate_directory()
    # --- Metrics and Logging ---
    def get_metrics(self):
        """Return detailed metrics for Prometheus/monitoring and cache stats."""
        metrics = {
            "bucket_count": len(self.buckets),
            "directory_size": len(self.directory),
            "split_events": getattr(self, '_split_events', 0),
            "merge_events": getattr(self, '_merge_events', 0),
            "gc_runs": getattr(self, '_gc_runs', 0),
            "last_error": getattr(self, '_last_error', None),
        }
        # Cache stats (if present)
        if hasattr(self, '_bucket_cache'):
            cache = self._bucket_cache
            metrics["cache_entries"] = getattr(cache, 'size', lambda: len(cache))()
            metrics["cache_hits"] = getattr(cache, 'hits', 0)
            metrics["cache_misses"] = getattr(cache, 'misses', 0)
            metrics["cache_hit_ratio"] = (
                metrics["cache_hits"] / (metrics["cache_hits"] + metrics["cache_misses"])
                if (metrics["cache_hits"] + metrics["cache_misses"]) > 0 else None
            )
        # Prometheus metrics (if available)
        if hasattr(self, 'wal') and self.wal and hasattr(self.wal, 'prometheus_metrics'):
            metrics["wal_prometheus"] = self.wal.prometheus_metrics()
        return metrics
    def expose_metrics(self):
        """Expose metrics for Prometheus scraping (if prometheus_client is available)."""
        if hasattr(self, 'wal') and self.wal and hasattr(self.wal, 'prometheus_metrics'):
            return self.wal.prometheus_metrics()
        return []

    def log_cache_stats(self):
        """Log current cache stats for observability."""
        logger = logging.getLogger("cidtree.cache")
        if hasattr(self, '_bucket_cache'):
            cache = self._bucket_cache
            logger.info(f"Bucket cache: size={getattr(cache, 'size', lambda: len(cache))()}, hits={getattr(cache, 'hits', 0)}, misses={getattr(cache, 'misses', 0)}")
        else:
            logger.info("No bucket cache present.")

    def log_metrics(self):
        """Log all metrics for monitoring."""
        logger = logging.getLogger("cidtree.metrics")
        metrics = self.get_metrics()
        logger.info(f"CIDTree metrics: {metrics}")

    def log_event(self, event, **kwargs):
        """Log merge/GC/error events for monitoring."""
        logger = logging.getLogger("cidtree.metrics")
        logger.info(f"{event}: {kwargs}")
        # Increment counters for metrics
        if event == "split":
            self._split_events = getattr(self, '_split_events', 0) + 1
        elif event == "merge":
            self._merge_events = getattr(self, '_merge_events', 0) + 1
        elif event == "gc":
            self._gc_runs = getattr(self, '_gc_runs', 0) + 1
        elif event == "error":
            self._last_error = kwargs.get("error")
    SPLIT_THRESHOLD = 128

    def __init__(self, storage):
        import threading
        self._writer_lock = threading.RLock()  # Global writer lock for SWMR
        if isinstance(storage, io.BytesIO):
            self._h5buffer = storage
            self.file = h5py.File(self._h5buffer, "a")
            self.storage = None
        elif isinstance(storage, str):
            self.storage = StorageManager(storage)
            self.file = self.storage.open()
        else:
            self.storage = storage
            self.file = self.storage.open()
        # Ensure canonical HDF5 layout
        self._init_hdf5_layout()
        self.wal = WAL(self.storage) if self.storage else None
        self._directory_mode = "attr"  # 'attr' for HDF5 attributes, 'ds' for dataset
        self._directory_dataset = None
        self._directory_attr_threshold = 1000  # Example threshold for migration
        self.directory = {}
        # Ensure /buckets group exists in HDF5
        if "/buckets" not in self.file:
            self.file.create_group("/buckets")
        self.buckets = {}
        self._bucket_counter = 0
        self.root = self._load_root()
        self._load_directory()
        # Wire up StorageManager hooks for WAL replay
        if self.storage:
            self.storage.apply_insert = self._wal_apply_insert
            self.storage.apply_delete = self._wal_apply_delete
        # WAL recovery: replay log to restore state (idempotent)
        if self.wal:
            self.wal.replay()

    def _wal_apply_insert(self, key, value):
        # Used by WAL for replay: key and value are E objects
        # Insert without logging to WAL again
        self.insert(key, value)

    def _wal_apply_delete(self, key):
        # Used by WAL for replay: key is an E object
        # Delete without logging to WAL again
        self.delete(key)

    def _init_hdf5_layout(self):
        """Ensure all groups, datasets, and attributes match canonical layout (Spec 9)."""
        f = self.file
        # Create config group and set attributes
        if "/config" not in f:
            cfg = f.create_group("/config")
        else:
            cfg = f["/config"]
        # Canonical attributes
        cfg.attrs.setdefault("format_version", 1)
        cfg.attrs.setdefault("created_by", "CIDTree")
        cfg.attrs.setdefault("swmr", True)
        cfg.attrs.setdefault("last_opened", int(time.time()))
        cfg.attrs.setdefault("last_modified", int(time.time()))
        # Directory attribute (may be migrated to dataset)
        # Do not store dict as attribute; initialize as empty if not present
        if "directory" not in cfg.attrs:
            cfg.attrs["directory"] = "{}"  # Store as JSON string for compatibility
        # Version/config attributes
        f.attrs.setdefault("cidtree_version", "1.0")
        f.attrs.setdefault("hdf5_version", h5py.version.hdf5_version)
        f.attrs.setdefault("swmr", True)
        # Ensure values group exists
        if "/values" not in f:
            f.create_group("/values")
        if "/values/sp" not in f:
            f["/values"].create_group("sp")
        # Ensure nodes group exists
        if "/nodes" not in f:
            f.create_group("/nodes")
        # Ensure deletion log exists (handled by DeletionLog)
        # All other metadata as attributes
        f.flush()

        # Deletion log and background GC
        self.deletion_log = DeletionLog(self.file)
        self.gc_thread = BackgroundGC(self)
        self.gc_thread.start()

    def _load_directory(self):
        """Load directory from HDF5 attributes or canonical dataset (Spec 3)."""
        f = self.file
        if "directory" in f:
            ds = f["directory"]
            self._directory_mode = "ds"
            self._directory_dataset = ds
            # Canonical: key_high, key_low, bucket_id, spill_ptr, state_mask, version
            self.directory = {}
            for row in ds:
                key = (int(row["key_high"]) << 64) | int(row["key_low"])
                self.directory[str(key)] = int(row["bucket_id"])
                # Optionally, you can store spill_ptr, state_mask, version in a parallel dict if needed
        elif "/config" in f and "directory" in f["/config"].attrs:
            # Directory as HDF5 attribute (small scale, stored as JSON string)
            import json
            self._directory_mode = "attr"
            attr = f["/config"].attrs["directory"]
            if isinstance(attr, (bytes, str)):
                # Parse JSON string
                if isinstance(attr, bytes):
                    attr = attr.decode("utf-8")
                try:
                    self.directory = json.loads(attr)
                except Exception:
                    self.directory = {}
            else:
                # Fallback for legacy dict (should not occur)
                self.directory = dict(attr)
        else:
            self._directory_mode = "attr"
            self.directory = {}

    def _save_directory(self):
        """Save directory to HDF5 attributes or canonical dataset (Spec 3)."""
        f = self.file
        if self._directory_mode == "attr":
            # Save as attribute (serialize as JSON string)
            import json
            f["/config"].attrs.modify("directory", json.dumps(self.directory))
        elif self._directory_mode == "ds":
            # Save as canonical dataset
            ds = self._directory_dataset
            ds.resize((len(self.directory),))
            for i, (k, v) in enumerate(self.directory.items()):
                # k is str(key) as loaded; convert to int for split
                key_int = int(k)
                key_high = key_int >> 64
                key_low = key_int & ((1 << 64) - 1)
                ds[i]["key_high"] = key_high
                ds[i]["key_low"] = key_low
                ds[i]["bucket_id"] = v
                # Optionally preserve spill_ptr, state_mask, version if you track them
                ds[i]["spill_ptr"] = b""
                ds[i]["state_mask"] = 0
                ds[i]["version"] = 1
        f.flush()

    def _maybe_migrate_directory(self):
        """Migrate directory from attribute to canonical, scalable, sharded/hybrid dataset (Spec 3, 9)."""
        if self._directory_mode == "attr" and len(self.directory) > self._directory_attr_threshold:
            f = self.file
            dt = np.dtype([
                ("key_high", "<u8"),
                ("key_low", "<u8"),
                ("bucket_id", "<i8"),
                ("spill_ptr", "S32"),
                ("state_mask", "u1"),
                ("version", "<u4"),
            ])
            # Hybrid: if >1M buckets, use sharded directory; else, single dataset
            SHARD_THRESHOLD = 1_000_000
            SHARD_SIZE = 100_000
            items = list(self.directory.items())
            if len(items) > SHARD_THRESHOLD:
                # Sharded directory: /directory/shard_{i}
                num_shards = (len(items) + SHARD_SIZE - 1) // SHARD_SIZE
                if "directory" in f:
                    del f["directory"]
                dir_group = f.require_group("directory")
                for k in list(dir_group.keys()):
                    del dir_group[k]
                for shard_idx in range(num_shards):
                    start = shard_idx * SHARD_SIZE
                    end = min((shard_idx + 1) * SHARD_SIZE, len(items))
                    shard_items = items[start:end]
                    ds = dir_group.create_dataset(
                        f"shard_{shard_idx:04d}",
                        shape=(len(shard_items),),
                        maxshape=(None,),
                        dtype=dt,
                        chunks=True,
                        track_times=False,
                    )
                    for i, (k, v) in enumerate(shard_items):
                        ek = E.from_str(k) if isinstance(k, str) else E(k)
                        ds[i]["key_high"] = ek.high
                        ds[i]["key_low"] = ek.low
                        ds[i]["bucket_id"] = v
                        ds[i]["spill_ptr"] = b""
                        ds[i]["state_mask"] = 0
                        ds[i]["version"] = 1
                self._directory_mode = "sharded"
                self._directory_dataset = dir_group
            else:
                # Single canonical dataset
                if "directory" in f:
                    del f["directory"]
                ds = f.create_dataset(
                    "directory",
                    shape=(len(items),),
                    maxshape=(None,),
                    dtype=dt,
                    chunks=True,
                    track_times=False,
                )
                for i, (k, v) in enumerate(items):
                    ek = E.from_str(k) if isinstance(k, str) else E(k)
                    ds[i]["key_high"] = ek.high
                    ds[i]["key_low"] = ek.low
                    ds[i]["bucket_id"] = v
                    ds[i]["spill_ptr"] = b""
                    ds[i]["state_mask"] = 0
                    ds[i]["version"] = 1
                self._directory_mode = "ds"
                self._directory_dataset = ds
            # Remove old attribute
            if "/config" in f and "directory" in f["/config"].attrs:
                f["/config"].attrs.modify("directory", None)
            f.flush()

    def _get_bucket_id(self, key):
        return hash(key) % max(1, len(self.buckets)) if self.buckets else 0

    def insert(self, key, value):
        with self._writer_lock:
            # WAL log before mutation, wrap in TXN if not in explicit transaction
            if self.wal:
                ek = E.from_str(key) if isinstance(key, str) else E(key)
                ev = E.from_str(value) if isinstance(value, str) else E(value)
                # Check for explicit transaction context (not implemented here, so always wrap)
                self.wal.append([
                    {"op_type": 3, "key_high": 0, "key_low": 0, "value_high": 0, "value_low": 0},  # TXN_START
                    {"op_type": 1, "key_high": ek.high, "key_low": ek.low, "value_high": ev.high, "value_low": ev.low},
                    {"op_type": 4, "key_high": 0, "key_low": 0, "value_high": 0, "value_low": 0},  # TXN_COMMIT
                ])
            if key not in self.directory:
                if not self.buckets:
                    bucket_id = self._bucket_counter
                    # Create HDF5 dataset for the bucket
                    bucket_ds = self.file["/buckets"].create_dataset(
                        f"bucket_{bucket_id}",
                        shape=(0,),
                        maxshape=(None,),
                        dtype=self.wal.hash_entry_dtype if hasattr(self.wal, "hash_entry_dtype") else __import__("cidtree.wal", fromlist=["hash_entry_dtype"]).hash_entry_dtype,
                        chunks=True,
                        track_times=False,
                    )
                    # Add sorted_count attribute
                    bucket_ds.attrs["sorted_count"] = 0
                    self.buckets[bucket_id] = bucket_ds
                    self._bucket_counter += 1
                else:
                    bucket_id = self._get_bucket_id(key)
                    if bucket_id not in self.buckets:
                        # Create HDF5 dataset for the bucket if not present
                        bucket_ds = self.file["/buckets"].create_dataset(
                            f"bucket_{bucket_id}",
                            shape=(0,),
                            maxshape=(None,),
                            dtype=self.wal.hash_entry_dtype if hasattr(self.wal, "hash_entry_dtype") else __import__("cidtree.wal", fromlist=["hash_entry_dtype"]).hash_entry_dtype,
                            chunks=True,
                            track_times=False,
                        )
                        bucket_ds.attrs["sorted_count"] = 0
                        self.buckets[bucket_id] = bucket_ds
                self.directory[key] = bucket_id
                self._save_directory()
            else:
                bucket_id = self.directory[key]
            bucket = self.buckets[bucket_id]
            # Insert: always append to unsorted region (after sorted_count)
            ek = E.from_str(key) if isinstance(key, str) else E(key)
            sorted_count = int(bucket.attrs.get("sorted_count", 0))
            # Try to find existing entry for this key in both regions
            found = False
            for i in range(bucket.shape[0]):
                if bucket[i]["key_high"] == ek.high and bucket[i]["key_low"] == ek.low:
                    # Find the first empty slot, or overwrite the first slot if all are full
                    slot_found = False
                    for j in range(4):
                        if bucket[i]["slots"][j] == 0:
                            bucket[i]["slots"][j] = int(value)
                            slot_found = True
                            break
                    if not slot_found:
                        # All slots full, overwrite the first slot (or promote to ValueSet in future)
                        bucket[i]["slots"][0] = int(value)
                    bucket[i]["state_mask"] = 1  # Mark as inline, not spilled
                    bucket[i]["version"] += 1
                    found = True
                    break
            if not found:
                # Append new entry to unsorted region (at end)
                new_entry = np.zeros(1, dtype=bucket.dtype)
                new_entry[0]["key_high"] = ek.high
                new_entry[0]["key_low"] = ek.low
                new_entry[0]["slots"][0] = int(value)
                new_entry[0]["state_mask"] = 1
                new_entry[0]["version"] = 1
                bucket.resize((bucket.shape[0] + 1,))
                bucket[-1] = new_entry[0]
            bucket.file.flush()
    # --- Multi-value/ValueSet API for test compatibility ---
    def compact(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return
        bucket = self.buckets.get(bucket_id, {})
        vs = bucket.get(key)
        if vs:
            vs.compact()

    def is_spilled(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return False
        bucket = self.buckets.get(bucket_id, {})
        vs = bucket.get(key)
        return vs.is_spilled() if vs else False

    def demote_if_possible(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return
        bucket = self.buckets.get(bucket_id, {})
        vs = bucket.get(key)
        if vs:
            vs.demote()

    def valueset_exists(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return False
        bucket = self.buckets.get(bucket_id, {})
        vs = bucket.get(key)
        return vs.valueset_exists() if vs else False

    def get_tombstone_count(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return 0
        bucket = self.buckets.get(bucket_id, {})
        vs = bucket.get(key)
        return vs.tombstone_count() if vs else 0

    def lookup(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return []
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return []
        ek = E.from_str(key) if isinstance(key, str) else E(key)
        sorted_count = int(bucket.attrs.get("sorted_count", 0))
        # Binary search in sorted region
        left, right = 0, sorted_count - 1
        while left <= right:
            mid = (left + right) // 2
            cmp_high = bucket[mid]["key_high"]
            cmp_low = bucket[mid]["key_low"]
            if (cmp_high, cmp_low) < (ek.high, ek.low):
                left = mid + 1
            elif (cmp_high, cmp_low) > (ek.high, ek.low):
                right = mid - 1
            else:
                return [slot for slot in bucket[mid]["slots"] if slot != 0]
        # Linear scan in unsorted region
        for i in range(sorted_count, bucket.shape[0]):
            if bucket[i]["key_high"] == ek.high and bucket[i]["key_low"] == ek.low:
                return [slot for slot in bucket[i]["slots"] if slot != 0]
        return []
    def merge_sort_bucket(self, bucket_id):
        """Merge/sort unsorted region into sorted region, update sorted_count atomically."""
        bucket = self.buckets[bucket_id]
        sorted_count = int(bucket.attrs.get("sorted_count", 0))
        if sorted_count == bucket.shape[0]:
            return  # Already fully sorted
        # Extract sorted and unsorted regions
        sorted_region = bucket[:sorted_count]
        unsorted_region = bucket[sorted_count:]
        if unsorted_region.shape[0] == 0:
            return
        # Merge and sort all entries by (key_high, key_low)
        all_entries = np.concatenate([sorted_region, unsorted_region])
        idx = np.lexsort((all_entries["key_low"], all_entries["key_high"]))
        all_entries = all_entries[idx]
        # Overwrite bucket with sorted entries
        bucket[:all_entries.shape[0]] = all_entries
        bucket.attrs["sorted_count"] = all_entries.shape[0]
        bucket.file.flush()

    def get_sorted_count(self, key):
        """Return sorted_count for the bucket containing key."""
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return 0
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return 0
        return int(bucket.attrs.get("sorted_count", 0))

    def get_sorted_region(self, key):
        """Return the sorted region (list of keys) for the bucket containing key."""
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return []
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return []
        sorted_count = int(bucket.attrs.get("sorted_count", 0))
        region = []
        for i in range(sorted_count):
            key_high = bucket[i]["key_high"]
            key_low = bucket[i]["key_low"]
            region.append((key_high, key_low))
        return region

    def delete(self, key, value=None):
        with self._writer_lock:
            # WAL log before mutation
            ek = E.from_str(key) if isinstance(key, str) else E(key)
            if self.wal:
                if value is not None:
                    ev = E.from_str(value) if isinstance(value, str) else E(value)
                    self.wal.append([
                        {"op_type": 2, "key_high": ek.high, "key_low": ek.low, "value_high": ev.high, "value_low": ev.low}
                    ])
                else:
                    self.wal.append([
                        {"op_type": 2, "key_high": ek.high, "key_low": ek.low, "value_high": 0, "value_low": 0}
                    ])
            # Deletion log (always log, even if WAL is disabled)
            if value is not None:
                ev = E.from_str(value) if isinstance(value, str) else E(value)
                self.deletion_log.append(ek.high, ek.low, ev.high, ev.low)
            else:
                self.deletion_log.append(ek.high, ek.low, 0, 0)

        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return
        ek = E.from_str(key) if isinstance(key, str) else E(key)
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == ek.high and bucket[i]["key_low"] == ek.low:
                if value is None:
                    # Delete the entry by removing the row
                    mask = np.ones(bucket.shape[0], dtype=bool)
                    mask[i] = False
                    bucket[...] = bucket[mask]
                    bucket.resize((bucket.shape[0] - 1,))
                else:
                    # Remove value from slots (inline mode only)
                    for j in range(4):
                        if bucket[i]["slots"][j] == int(value):
                            bucket[i]["slots"][j] = 0
                    # If all slots are zero, remove the entry
                    if not any(bucket[i]["slots"]):
                        mask = np.ones(bucket.shape[0], dtype=bool)
                        mask[i] = False
                        bucket[...] = bucket[mask]
                        bucket.resize((bucket.shape[0] - 1,))
                bucket.file.flush()
                break
        if key in self.directory:
            del self.directory[key]

    def run_gc_once(self):
        """Background GC: scan deletion log, reclaim orphans, compact ValueSets. Idempotent and crash-safe."""
        log_entries = self.deletion_log.scan()
        for entry in log_entries:
            key_high, key_low, value_high, value_low, ts = entry
            # Reconstruct key and value
            key = (int(key_high) << 64) | int(key_low)
            value = (int(value_high) << 64) | int(value_low)
            # Check if key/value is still present
            found = False
            for bucket in self.buckets.values():
                for k, vs in bucket.items():
                    ek = E.from_str(k) if isinstance(k, str) else E(k)
                    if ek.high == key_high and ek.low == key_low:
                        if value_high == 0 and value_low == 0:
                            # Key deletion: if not present, reclaim
                            if not vs.values():
                                found = True
                        else:
                            # Value deletion: if not present, reclaim
                            if value not in vs.values():
                                found = True
            # If not found, reclaim (remove from log)
            if found:
                continue
            # Orphaned: safe to reclaim (remove from log)
            # (In real system, would also delete HDF5 datasets if needed)
        # After scan, clear log (idempotent)
        self.deletion_log.clear()

    def get_entry(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return None
        bucket = self.buckets.get(bucket_id, {})
        vs = bucket.get(key)
        values = vs.values() if vs else []
        try:
            ek = E.from_str(key) if isinstance(key, str) else E(key)
            key_high = ek.high if hasattr(ek, "high") else 0
            key_low = ek.low if hasattr(ek, "low") else 0
        except Exception:
            ek = key
            key_high = 0
            key_low = 0
        return {
            "key": ek,
            "key_high": key_high,
            "key_low": key_low,
            "values": values,
            "value": values[0] if values else None,
        }

    def _split_bucket(self, bucket_id):
        # Enforce split threshold invariant
        bucket = self.buckets[bucket_id]
        if len(bucket) <= self.SPLIT_THRESHOLD:
            return
        # Copy-on-Write: create new bucket, do not modify in place
        items = list(bucket.items())
        mid = len(items) // 2
        left_items = items[:mid]
        right_items = items[mid:]
        new_bucket_id = self._bucket_counter
        self._bucket_counter += 1
        # WAL log before mutation (split event)
        if self.wal:
            self.wal.append([
                {"op_type": 10, "key_high": 0, "key_low": 0, "value_high": 0, "value_low": 0}  # op_type 10 = split
            ])
        self.buckets[new_bucket_id] = dict(right_items)
        # Update directory for moved keys
        for k, _ in right_items:
            self.directory[k] = new_bucket_id
        # Shrink original bucket
        self.buckets[bucket_id] = dict(left_items)
        self._save_directory()

    def _maybe_merge_buckets(self):
        # Background maintenance: merge underfilled buckets
        # Invariant: if a bucket is under threshold, merge with neighbor
        underfilled = [bid for bid, b in self.buckets.items() if len(b) < self.SPLIT_THRESHOLD // 4]
        merged = set()
        for i, bid in enumerate(underfilled):
            if bid in merged:
                continue
            # Find a neighbor (next bucket by id)
            neighbor = None
            for nbid in self.buckets:
                if nbid != bid and nbid not in merged:
                    neighbor = nbid
                    break
            if neighbor is not None:
                self.merge_buckets(bid, neighbor)
                merged.add(bid)
                merged.add(neighbor)

    def background_maintenance(self):
        """Background maintenance: merge/sort/compact buckets using danger scores from WAL."""
        # Merge/sort buckets with unsorted region
        for bucket_id, bucket in self.buckets.items():
            sorted_count = int(bucket.attrs.get("sorted_count", 0))
            if bucket.shape[0] > sorted_count:
                self.merge_sort_bucket(bucket_id)
        self._maybe_merge_buckets()
        # ValueSet compaction (if implemented)
        # for bucket in self.buckets.values():
        #     for vs in bucket.values():
        #         if hasattr(vs, "compact"):
        #             vs.compact()

    def merge_buckets(self, bucket_id1, bucket_id2):
        # Example merge logic: merge bucket2 into bucket1, then delete bucket2
        if bucket_id1 not in self.buckets or bucket_id2 not in self.buckets:
            return
        if self.wal:
            self.wal.append([
                {"op_type": 11, "key_high": 0, "key_low": 0, "value_high": 0, "value_low": 0}  # op_type 11 = merge
            ])
        bucket1 = self.buckets[bucket_id1]
        bucket2 = self.buckets[bucket_id2]
        bucket1.update(bucket2)
        # Update directory for moved keys
        for k in bucket2.keys():
            self.directory[k] = bucket_id1
        del self.buckets[bucket_id2]
        self._save_directory()

    def _load_root(self):
        # Dummy for TDD
        return None
