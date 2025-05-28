"""store.py - Main CIDStore class with proper extendible hashing implementation"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from typing import Any

import h5py
import numpy as np

from .keys import E
from .maintenance import MaintenanceConfig, MaintenanceManager
from .storage import Storage
from .wal import WAL

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

CONF = "/config"


class CIDStore:
    async def auto_tune(self):
        """Run auto-tuning feedback loop using current metrics."""
        from .metrics import auto_tune as _auto_tune

        await _auto_tune(self)

    """
    CIDStore: Main entry point for the CIDStore hash directory (fully async interface, sync logic).
    All public methods are async and log to WAL asynchronously.
    Implements extendible hashing with global_depth and local_depth per Spec 3.
    """

    SPLIT_THRESHOLD: int = 128

    def __init__(self, hdf: Storage, wal: WAL) -> None:
        assert isinstance(hdf, Storage), "hdf must be a Storage"
        assert isinstance(wal, WAL), "wal must be a WAL"
        self.hdf = hdf
        self.wal = wal  # Extendible hashing components (Spec 3)
        self.global_depth: int = 1  # Directory depth
        self.bucket_pointers: list[dict] = []  # Array of BucketPointer entries
        self.num_buckets: int = 0

        self._writer_lock = threading.RLock()
        self.hdf._init_hdf5_layout()
        self._directory_mode = "attr"
        self._directory_dataset = None
        self._directory_attr_threshold = 1000
        self._directory_ds_threshold = (
            100000  # Threshold for dataset → sharded migration
        )
        self._directory_shards = {}  # Dict of shard_id → dataset for sharded mode
        with self.hdf as f:
            if "/buckets" not in f:
                f.create_group("/buckets")
        self._bucket_counter = 0
        self._wal_consumer_task: asyncio.Task | None = None

        # Initialize extendible hash directory
        self._init_directory()

        # Replay WAL for recovery
        self._replay_wal()  # Initialize deletion log and background GC
        self._init_deletion_log_and_gc()  # Initialize metrics and auto-tuning
        self._init_metrics_and_autotune()

    def _migrate_attr_to_dataset(self):
        """Migrate directory from attribute mode to dataset mode."""
        with self.hdf as f:
            if "directory" not in f:
                dtype = np.dtype([
                    ("bucket_id", "<u8"),
                    ("hdf5_ref", "<u8"),
                ])
                ds = f.create_dataset(
                    "directory",
                    shape=(len(self.bucket_pointers),),
                    maxshape=(None,),
                    dtype=dtype,
                    chunks=True,
                    track_times=False,
                )
                for i, pointer in enumerate(self.bucket_pointers):
                    ds[i] = (pointer["bucket_id"], pointer.get("hdf5_ref", 0))
                self._directory_mode = "ds"
                self._directory_dataset = ds
                logger.info(
                    f"[CIDStore] Migrated directory to dataset mode (size={len(self.bucket_pointers)})"
                )
                # Optionally remove old attribute if present
                config = f["/config"]
                if "directory" in config.attrs:
                    del config.attrs["directory"]

    def _migrate_dataset_to_sharded(self):
        """Migrate directory from dataset mode to sharded mode."""
        with self.hdf as f:
            # Create directory shards group
            if "directory_shards" not in f:
                f.create_group("directory_shards")
            shards_group = f["directory_shards"]

            # Calculate shard size (e.g., 50K entries per shard)
            shard_size = 50000
            num_shards = (len(self.bucket_pointers) + shard_size - 1) // shard_size

            dtype = np.dtype([
                ("bucket_id", "<u8"),
                ("hdf5_ref", "<u8"),
            ])

            # Create shards and distribute directory entries
            self._directory_shards = {}
            for shard_id in range(num_shards):
                shard_name = f"shard_{shard_id:04d}"
                start_idx = shard_id * shard_size
                end_idx = min(start_idx + shard_size, len(self.bucket_pointers))
                shard_entries = self.bucket_pointers[start_idx:end_idx]

                shard_ds = shards_group.create_dataset(
                    shard_name,
                    shape=(len(shard_entries),),
                    maxshape=(None,),
                    dtype=dtype,
                    chunks=True,
                    track_times=False,
                )

                for i, pointer in enumerate(shard_entries):
                    shard_ds[i] = (pointer["bucket_id"], pointer.get("hdf5_ref", 0))

                self._directory_shards[shard_id] = shard_ds

            # Remove old directory dataset
            if "directory" in f:
                del f["directory"]

            self._directory_mode = "sharded"
            self._directory_dataset = None

            # Store shard metadata
            config = f["/config"]
            config.attrs["directory_shard_size"] = shard_size
            config.attrs["directory_num_shards"] = num_shards

            logger.info(
                f"[CIDStore] Migrated directory to sharded mode (size={len(self.bucket_pointers)}, shards={num_shards})"
            )

    _global_writer_lock = threading.Lock()

    def acquire_global_writer_lock(self, blocking: bool = True) -> bool:
        """Acquire the global writer lock to prevent multiple writers (SWMR)."""
        return self._global_writer_lock.acquire(blocking=blocking)

    def release_global_writer_lock(self):
        """Release the global writer lock."""
        self._global_writer_lock.release()

    def __enter__(self):
        self.acquire_global_writer_lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_global_writer_lock()
        if (
            hasattr(self, "_wal_consumer_task")
            and self._wal_consumer_task
            and not self._wal_consumer_task.done()
        ):
            try:
                self._wal_consumer_task.cancel()
            except Exception:
                pass  # Stop background threads        assert hasattr(self, 'maintenance_manager'), "MaintenanceManager not initialized"
        self.maintenance_manager.stop()

    def _init_deletion_log_and_gc(self):
        # Initialize unified maintenance manager
        maintenance_config = MaintenanceConfig(
            gc_interval=60,
            maintenance_interval=30,
            sort_threshold=16,
            merge_threshold=8,
            wal_analysis_interval=120,
            adaptive_maintenance_enabled=True,
        )
        self.maintenance_manager = MaintenanceManager(self, maintenance_config)
        self.maintenance_manager.start()

    def log_deletion(self, key: E, value: E):
        assert hasattr(self, "maintenance_manager"), (
            "MaintenanceManager not initialized"
        )
        self.maintenance_manager.log_deletion(key.high, key.low, value.high, value.low)

    def run_gc_once(self):
        # Delegate to maintenance manager
        assert hasattr(self, "maintenance_manager"), (
            "MaintenanceManager not initialized"
        )
        self.maintenance_manager.run_gc_once()

    async def _maybe_merge_bucket(self, bucket_id: int, merge_threshold: int = 8):
        """Automatically merge bucket with its pair if both are underfull and local_depth > 1."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                return
            bucket = buckets_group[bucket_name]
            local_depth = int(bucket.attrs.get("local_depth", 1))
            entry_count = int(bucket.attrs.get("entry_count", 0))
            if local_depth <= 1 or entry_count > merge_threshold:
                return
            # Find pair bucket (differ by last local_depth bit)
            pair_id = bucket_id ^ (1 << (local_depth - 1))
            pair_name = f"bucket_{pair_id:04d}"
            if pair_name not in buckets_group:
                return
            pair_bucket = buckets_group[pair_name]
            pair_local_depth = int(pair_bucket.attrs.get("local_depth", 1))
            pair_entry_count = int(pair_bucket.attrs.get("entry_count", 0))
            if pair_local_depth != local_depth or pair_entry_count > merge_threshold:
                return
            # Merge buckets
            merged_entries = list(bucket[:]) + list(pair_bucket[:])
            # Remove both buckets
            del buckets_group[bucket_name]
            del buckets_group[pair_name]
            # Create new merged bucket
            new_bucket_id = min(bucket_id, pair_id)
            new_bucket_name = f"bucket_{new_bucket_id:04d}"
            hash_entry_dtype = np.dtype([
                ("key_high", "<u8"),
                ("key_low", "<u8"),
                ("slots", "<u8", (2,)),
                ("checksum", "<u8", (2,)),
            ])
            merged_bucket = buckets_group.create_dataset(
                new_bucket_name,
                shape=(len(merged_entries),),
                maxshape=(None,),
                dtype=hash_entry_dtype,
                chunks=True,
                track_times=False,
            )
            if merged_entries:
                merged_bucket[:] = merged_entries
            merged_bucket.attrs["local_depth"] = local_depth - 1
            merged_bucket.attrs["entry_count"] = len(merged_entries)
            merged_bucket.attrs["sorted_count"] = len(merged_entries)
            # Update directory pointers
            mask = (1 << (local_depth - 1)) - 1
            for i, pointer in enumerate(self.bucket_pointers):
                if (i & ~mask) >> (local_depth - 1) == (
                    new_bucket_id >> (local_depth - 1)
                ):
                    pointer["bucket_id"] = new_bucket_id
            self.num_buckets -= 1
            self._save_directory_metadata()

    async def maintenance(self, sort_threshold: int = 16, merge_threshold: int = 8):
        """Background maintenance: sort/merge unsorted region and merge underfull buckets."""
        with self.hdf as f:
            buckets_group = f["/buckets"]
            bucket_ids = [int(name.split("_")[-1]) for name in buckets_group]
            for bucket_id in bucket_ids:
                bucket = buckets_group[f"bucket_{bucket_id:04d}"]
                sorted_count = int(bucket.attrs.get("sorted_count", 0))
                total = bucket.shape[0]
                unsorted_count = total - sorted_count
                if unsorted_count >= sort_threshold:
                    # Merge/sort unsorted region
                    all_entries = list(bucket[:])
                    all_entries.sort(key=lambda e: (e["key_high"], e["key_low"]))
                    bucket[:] = all_entries
                    bucket.attrs["sorted_count"] = total
                    bucket.flush()
                entry_count = int(bucket.attrs.get("entry_count", 0))
                local_depth = int(bucket.attrs.get("local_depth", 1))
                if local_depth > 1 and entry_count <= merge_threshold:
                    await self._maybe_merge_bucket(
                        bucket_id, merge_threshold=merge_threshold
                    )

    # --- Spec 5: Multi-Value Key Promotion/Demotion and ValueSet Compaction ---

    def promote_to_spill(self, key: E, new_value: E = None):
        """Promote inline entry to ValueSet (spill) mode for a key."""
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                return
            bucket = buckets_group[bucket_name]
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    slots = list(entry["slots"])
                    values = [E(s) for s in slots if s != 0]
                    if new_value is not None:
                        values.append(new_value)
                    sp_group = self._get_valueset_group(f)
                    ds_name = self._get_spill_ds_name(bucket_id, key)
                    if ds_name in sp_group:
                        del sp_group[ds_name]
                    ds = sp_group.create_dataset(
                        ds_name, shape=(len(values),), maxshape=(None,), dtype="<u8"
                    )
                    ds[:] = [int(v) for v in values]
                    bucket[i]["slots"] = (0, int(ds.id.ref))
                    bucket.flush()
                    return

    def demote_if_possible(self, key: E):
        """Demote a key from spill to inline if possible (≤2 values)."""
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                return
            bucket = buckets_group[bucket_name]
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    slots = list(entry["slots"])
                    if slots[0] == 0 and slots[1] != 0:
                        sp_group = self._get_valueset_group(f)
                        ds_name = self._get_spill_ds_name(bucket_id, key)
                        if ds_name in sp_group:
                            ds = sp_group[ds_name]
                            values = [E(v) for v in ds[:] if v != 0]
                            if len(values) <= 2:
                                new_slots = [int(v) for v in values] + [0] * (
                                    2 - len(values)
                                )
                                bucket[i]["slots"] = tuple(new_slots)
                                del sp_group[ds_name]
                                bucket.flush()
                    return

    def compact(self, key: E):
        """Remove tombstones (0s) from ValueSet for a key."""
        bucket_id = self._find_bucket_id(key)
        with self.hdf as f:
            sp_group = self._get_valueset_group(f)
            ds_name = self._get_spill_ds_name(bucket_id, key)
            if ds_name in sp_group:
                ds = sp_group[ds_name]
                values = [v for v in ds[:] if v != 0]
                ds.resize((len(values),))
                ds[:] = values

    # --- Spec 5: Multi-Value Key Promotion/Demotion and ValueSet Compaction ---

    def is_spilled(self, key: E) -> bool:
        """Return True if the key is in spill (ValueSet) mode."""
        entry = self.get_entry_sync(key)
        if entry is None:
            return False
        slots = entry["slots"]
        if not isinstance(slots, (list, tuple)) or len(slots) < 2:
            return False
        return slots[0] == 0 and slots[1] != 0

    def get_entry_sync(self, key: E):
        # Synchronous version of get_entry for internal use
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj
                if bucket_name not in buckets_group:
                    return None
                bucket_obj = buckets_group[bucket_name]
                if isinstance(bucket_obj, h5py.Dataset):
                    bucket = bucket_obj
                    for i in range(bucket.shape[0]):
                        entry = bucket[i]
                        if (
                            entry["key_high"] == key.high
                            and entry["key_low"] == key.low
                        ):
                            return {
                                "key_high": int(entry["key_high"]),
                                "key_low": int(entry["key_low"]),
                                "slots": list(entry["slots"]),
                                "checksum": list(entry["checksum"]),
                            }
        return None

    def _get_valueset_group(self, f):
        if "/values" not in f:
            f.create_group("/values")
        values_group = f["/values"]
        if isinstance(values_group, h5py.Group):
            if "sp" not in values_group:
                values_group.create_group("sp")
            return values_group["sp"]
        raise RuntimeError("/values is not a group in HDF5 file")

    def _get_spill_ds_name(self, bucket_id, key):
        return f"sp_{bucket_id}_{key.high}_{key.low}"

    def _promote_to_spill(self, bucket, entry_idx, key, new_value=None):
        """Promote inline entry to ValueSet (spill) mode."""
        slots = list(bucket[entry_idx]["slots"])
        values = [E(s) for s in slots if s != 0]
        if new_value is not None:
            values.append(new_value)
        bucket_id = (
            int(bucket.attrs["bucket_id"]) if "bucket_id" in bucket.attrs else None
        )
        if bucket_id is None:
            # Fallback: try to find bucket_id from name
            bucket_id = int(bucket.name.split("_")[-1])
        with self.hdf as f:
            sp_group = self._get_valueset_group(f)
            ds_name = self._get_spill_ds_name(bucket_id, key)
            if ds_name in sp_group and isinstance(sp_group, h5py.Group):
                if isinstance(sp_group[ds_name], h5py.Dataset):
                    del sp_group[ds_name]
            ds = sp_group.create_dataset(
                ds_name, shape=(len(values),), maxshape=(None,), dtype="<u8"
            )
            ds[:] = [int(v) for v in values]
            # Update entry to spill mode: slots[0]=0, slots[1]=spill pointer (use HDF5 object id)
            spill_ptr = ds.id.ref
            bucket[entry_idx]["slots"] = (0, int(spill_ptr))
            bucket.flush()

    def _demote_from_spill(self, bucket, entry_idx, key):
        """Demote ValueSet (spill) entry to inline if <=2 values remain."""
        bucket_id = (
            int(bucket.attrs["bucket_id"]) if "bucket_id" in bucket.attrs else None
        )
        if bucket_id is None:
            bucket_id = int(bucket.name.split("_")[-1])
        with self.hdf as f:
            sp_group = self._get_valueset_group(f)
            ds_name = self._get_spill_ds_name(bucket_id, key)
            if ds_name not in sp_group or not isinstance(sp_group, h5py.Group):
                return
            ds = sp_group[ds_name]
            if not isinstance(ds, h5py.Dataset):
                return
            values = [E(v) for v in ds[:] if v != 0]
            if len(values) <= 2:
                # Demote: move values back inline, delete ValueSet
                slots = [int(v) for v in values] + [0] * (2 - len(values))
                bucket[entry_idx]["slots"] = tuple(slots)
                del sp_group[ds_name]
                bucket.flush()

    async def async_init(self) -> None:
        if self._wal_consumer_task is None:
            self._wal_consumer_task = asyncio.create_task(self.wal.consume_polling())

    async def wal_checkpoint(self):
        self.hdf.file.flush()
        self.wal.flush()

    def apply(self, op: dict[str, Any]) -> None:
        """Apply a WAL operation to the store."""
        op_type = op.get("op_type")
        # OpType values: 1=INSERT, 2=DELETE, 3=TXN_START, 4=TXN_COMMIT
        if op_type == 1:  # INSERT
            key = E((op["k_high"] << 64) | op["k_low"])
            value = E((op["v_high"] << 64) | op["v_low"])
            # Synchronously insert (no WAL logging)
            loop = asyncio.get_event_loop()
            if loop.is_running():
                coro = self.insert(key, value)
                asyncio.ensure_future(coro)
            else:
                loop.run_until_complete(self.insert(key, value))
        elif op_type == 2:  # DELETE
            key = E((op["k_high"] << 64) | op["k_low"])
            loop = asyncio.get_event_loop()
            if hasattr(self, "delete"):
                if loop.is_running():
                    coro = self.delete(key)
                    asyncio.ensure_future(coro)
                else:
                    loop.run_until_complete(self.delete(key))
        # Add more op_types as needed

    def _replay_wal(self):
        """Replay WAL records to restore the store state."""
        logger.info("[CIDStore] Replaying WAL for recovery")
        ops = self.wal.replay()
        for op in ops:
            self.apply(op)

    async def recover(self) -> None:
        """Recover from WAL and replay any pending operations."""
        logger.info("[CIDStore.recover] Starting WAL recovery")
        await self.wal.replay()
        logger.info("[CIDStore.recover] WAL recovery completed")

    def _init_directory(self) -> None:
        """
        Initialize the extendible hash directory structure.
        Load existing directory from HDF5 or create a new one.
        """
        with self.hdf as f:
            config = f.require_group("/config")

            # Load or initialize global_depth
            self.global_depth = config.attrs.get("global_depth", 1)
            self.num_buckets = config.attrs.get(
                "num_buckets", 0
            )  # Initialize directory with 2^global_depth entries
            directory_size = 2**self.global_depth

            if "directory_shards" in f:
                # Load existing sharded directory
                shards_group = f["directory_shards"]
                self._directory_mode = "sharded"
                self._directory_dataset = None
                self._directory_shards = {}

                # Load shard metadata
                config = f["/config"]
                shard_size = config.attrs.get("directory_shard_size", 50000)
                num_shards = config.attrs.get("directory_num_shards", 0)

                # Load all shards
                self.bucket_pointers = []
                for shard_id in range(num_shards):
                    shard_name = f"shard_{shard_id:04d}"
                    if shard_name in shards_group:
                        shard_ds = shards_group[shard_name]
                        self._directory_shards[shard_id] = shard_ds

                        # Load entries from this shard
                        for entry in shard_ds[:]:
                            self.bucket_pointers.append({
                                "bucket_id": int(entry["bucket_id"]),
                                "hdf5_ref": int(entry["hdf5_ref"])
                                if "hdf5_ref" in entry.dtype.names
                                else 0,
                            })

                # Ensure directory is the right size
                while len(self.bucket_pointers) < directory_size:
                    self.bucket_pointers.append({"bucket_id": 0, "hdf5_ref": 0})

            elif "directory" in f:
                # Load existing directory dataset
                directory_obj = f["directory"]
                if isinstance(directory_obj, h5py.Dataset):
                    directory_ds = directory_obj
                    self._directory_mode = "ds"
                    self._directory_dataset = directory_ds
                    self.bucket_pointers = []
                    for i in range(min(directory_size, directory_ds.shape[0])):
                        entry = directory_ds[i]
                        self.bucket_pointers.append({
                            "bucket_id": int(entry["bucket_id"]),
                            "hdf5_ref": int(entry["hdf5_ref"])
                            if "hdf5_ref" in entry.dtype.names
                            else 0,
                        })
                    # Ensure directory is the right size
                    while len(self.bucket_pointers) < directory_size:
                        self.bucket_pointers.append({"bucket_id": 0, "hdf5_ref": 0})
            else:
                # Create new directory
                self.bucket_pointers = [
                    {"bucket_id": 0, "hdf5_ref": 0} for _ in range(directory_size)
                ]

                # Create initial bucket if none exist
                if self.num_buckets == 0:
                    self._create_initial_bucket()

    def _create_initial_bucket(self) -> None:
        """Create the initial bucket and update directory pointers."""
        bucket_id = self.num_buckets
        bucket_name = f"bucket_{bucket_id:04d}"

        # Create bucket dataset in HDF5
        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj
                if bucket_name not in buckets_group:
                    # Use hash_entry_dtype from specs
                    hash_entry_dtype = np.dtype([
                        ("key_high", "<u8"),
                        ("key_low", "<u8"),
                        ("slots", "<u8", (2,)),  # Simplified to 2 slots as per Spec 2
                        ("checksum", "<u8", (2,)),  # 2 x uint64 for checksum
                    ])

                    bucket_ds = buckets_group.create_dataset(
                        bucket_name,
                        shape=(0,),
                        maxshape=(None,),
                        dtype=hash_entry_dtype,
                        chunks=True,
                        track_times=False,
                    )

                    # Set bucket attributes
                    bucket_ds.attrs["local_depth"] = 1
                    bucket_ds.attrs["sorted_count"] = 0
                    bucket_ds.attrs["entry_count"] = 0

        # Update all directory pointers to point to this bucket
        for pointer in self.bucket_pointers:
            pointer["bucket_id"] = bucket_id
            self.num_buckets += 1
        self._save_directory_metadata()

    def _save_directory_metadata(self) -> None:
        """Save global_depth, num_buckets to HDF5 config and migrate directory if needed."""
        with self.hdf as f:
            config = f["/config"]
            if isinstance(config, h5py.Group):
                config.attrs["global_depth"] = self.global_depth
                config.attrs["num_buckets"] = self.num_buckets
                f.flush()
        self._maybe_migrate_directory()
        self._save_directory_to_storage()

    def _maybe_migrate_directory(self):
        """Placeholder for directory migration logic."""
        pass

    def _save_directory_to_storage(self) -> None:
        """Save directory bucket pointers to appropriate storage format (attr/dataset/sharded)."""
        if self._directory_mode == "attr":
            # For small directories, store as attribute (legacy mode)
            pass  # Usually handled elsewhere for compatibility
        elif self._directory_mode == "ds":
            # Update existing dataset
            if self._directory_dataset is not None:
                self._update_directory_dataset()
        elif self._directory_mode == "sharded":
            # Update sharded directories
            self._update_sharded_directory()

    def _update_directory_dataset(self) -> None:
        """Update directory dataset with current bucket pointers."""
        if self._directory_dataset is None:
            return

        # Resize dataset if needed
        if self._directory_dataset.shape[0] < len(self.bucket_pointers):
            self._directory_dataset.resize((len(self.bucket_pointers),))

        # Update all entries
        for i, pointer in enumerate(self.bucket_pointers):
            self._directory_dataset[i] = (
                pointer["bucket_id"],
                pointer.get("hdf5_ref", 0),
            )

    def _update_sharded_directory(self) -> None:
        """Update sharded directory with current bucket pointers."""
        if not self._directory_shards:
            return

        with self.hdf as f:
            config = f["/config"]
            shard_size = config.attrs.get("directory_shard_size", 50000)

            # Calculate required number of shards
            num_shards_needed = (
                len(self.bucket_pointers) + shard_size - 1
            ) // shard_size

            # Ensure we have enough shards
            shards_group = f["directory_shards"]
            while len(self._directory_shards) < num_shards_needed:
                shard_id = len(self._directory_shards)
                shard_name = f"shard_{shard_id:04d}"

                dtype = np.dtype([
                    ("bucket_id", "<u8"),
                    ("hdf5_ref", "<u8"),
                ])

                shard_ds = shards_group.create_dataset(
                    shard_name,
                    shape=(0,),
                    maxshape=(None,),
                    dtype=dtype,
                    chunks=True,
                    track_times=False,
                )
                self._directory_shards[shard_id] = shard_ds

            # Update shard entries
            for shard_id in range(num_shards_needed):
                start_idx = shard_id * shard_size
                end_idx = min(start_idx + shard_size, len(self.bucket_pointers))
                shard_entries = self.bucket_pointers[start_idx:end_idx]

                shard_ds = self._directory_shards[shard_id]

                # Resize shard if needed
                if shard_ds.shape[0] != len(shard_entries):
                    shard_ds.resize((len(shard_entries),))

                # Update shard entries
                for i, pointer in enumerate(shard_entries):
                    shard_ds[i] = (pointer["bucket_id"], pointer.get("hdf5_ref", 0))

            # Update metadata
            config.attrs["directory_num_shards"] = num_shards_needed

            logger.info(
                f"[CIDStore] Updated sharded directory metadata (shards={num_shards_needed})"
            )

    def _find_bucket_id(self, key: E) -> int:
        """
        Find bucket ID for a key using extendible hashing (Spec 3).
        Extract top global_depth bits from key.high as directory index.
        """
        directory_index = key.high >> (64 - self.global_depth)
        directory_index = directory_index % len(self.bucket_pointers)
        return self.bucket_pointers[directory_index]["bucket_id"]

    async def get(self, key: E) -> list[E]:
        """Get all values for a key using binary search in sorted region and linear scan in unsorted region."""
        logger.info(f"[CIDStore.get] Called with {key=}")
        assert isinstance(key, E)

        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if not isinstance(buckets_group_obj, h5py.Group):
                return []
            buckets_group = buckets_group_obj
            if bucket_name not in buckets_group:
                logger.info(f"[CIDStore.get] No bucket found for {key=}")
                return []

            bucket_obj = buckets_group[bucket_name]
            if not isinstance(bucket_obj, h5py.Dataset):
                return []
            bucket = bucket_obj

            sorted_count = int(bucket.attrs.get("sorted_count", 0))
            total = bucket.shape[0]

            # --- Binary search in sorted region ---
            left, right = 0, sorted_count - 1
            while left <= right:
                mid = (left + right) // 2
                entry = bucket[mid]
                entry_key = (entry["key_high"], entry["key_low"])
                search_key = (key.high, key.low)
                if entry_key == search_key:
                    # Found in sorted region
                    if len(entry["slots"]) > 0 and entry["slots"][0] != 0:
                        return [E(slot) for slot in entry["slots"] if slot != 0]
                    else:
                        # Check for spilled values
                        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                        values_group_obj = f.get("/values/sp")
                        if values_group_obj and isinstance(
                            values_group_obj, h5py.Group
                        ):
                            if spill_ds_name in values_group_obj:
                                spill_ds_obj = values_group_obj[spill_ds_name]
                                if isinstance(spill_ds_obj, h5py.Dataset):
                                    return [E(v) for v in spill_ds_obj[:] if v != 0]
                        return []
                elif entry_key < search_key:
                    left = mid + 1
                else:
                    right = mid - 1

            # --- Linear scan in unsorted region ---
            for i in range(sorted_count, total):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    if len(entry["slots"]) > 0 and entry["slots"][0] != 0:
                        return [E(slot) for slot in entry["slots"] if slot != 0]
                    else:
                        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                        values_group_obj = f.get("/values/sp")
                        if values_group_obj and isinstance(
                            values_group_obj, h5py.Group
                        ):
                            if spill_ds_name in values_group_obj:
                                spill_ds_obj = values_group_obj[spill_ds_name]
                                if isinstance(spill_ds_obj, h5py.Dataset):
                                    return [E(v) for v in spill_ds_obj[:] if v != 0]
                        return []

        return []

    async def get_entry(self, key: E) -> dict[str, Any] | None:
        """Get bucket entry for a key (for debugging and testing), using binary search in sorted region and linear scan in unsorted region."""
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if not isinstance(buckets_group_obj, h5py.Group):
                return None
            buckets_group = buckets_group_obj
            if bucket_name not in buckets_group:
                return None

            bucket_obj = buckets_group[bucket_name]
            if not isinstance(bucket_obj, h5py.Dataset):
                return None
            bucket = bucket_obj

            sorted_count = int(bucket.attrs.get("sorted_count", 0))
            total = bucket.shape[0]

            # --- Binary search in sorted region ---
            left, right = 0, sorted_count - 1
            while left <= right:
                mid = (left + right) // 2
                entry = bucket[mid]
                entry_key = (entry["key_high"], entry["key_low"])
                search_key = (key.high, key.low)
                if entry_key == search_key:
                    return {
                        "key_high": int(entry["key_high"]),
                        "key_low": int(entry["key_low"]),
                        "slots": list(entry["slots"]),
                        "checksum": list(entry["checksum"]),
                    }
                elif entry_key < search_key:
                    left = mid + 1
                else:
                    right = mid - 1

            # --- Linear scan in unsorted region ---
            for i in range(sorted_count, total):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    return {
                        "key_high": int(entry["key_high"]),
                        "key_low": int(entry["key_low"]),
                        "slots": list(entry["slots"]),
                        "checksum": list(entry["checksum"]),
                    }

        return None

    async def insert(self, key: E, value: E) -> None:
        """Insert a key-value pair, appending to the unsorted region."""
        logger.info(f"[CIDStore.insert] Called with {key=}, {value=}")
        assert isinstance(key, E)
        assert isinstance(value, E)

        bucket_id = self._find_bucket_id(key)
        bucket_name = (
            f"bucket_{bucket_id:04d}"  # Record operation in WAL pattern analyzer
        )
        # Record operation for WAL analysis
        assert hasattr(self, "maintenance_manager"), (
            "MaintenanceManager not initialized"
        )
        assert hasattr(self.maintenance_manager, "wal_analyzer_thread"), (
            "WAL analyzer thread not initialized"
        )  # Extract bucket ID from bucket name for WAL analysis
        try:
            bucket_id = int(bucket_name.split("_")[-1]) if "_" in bucket_name else 0
            self.maintenance_manager.wal_analyzer_thread.record_operation(
                bucket_id, 1
            )  # Insert operation
        except (ValueError, IndexError):
            pass  # Skip recording if bucket name format is unexpected

        # Log to WAL
        await self.wal.log_insert(key.high, key.low, value.high, value.low)

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj

                if bucket_name not in buckets_group:
                    # Create new bucket
                    hash_entry_dtype = np.dtype([
                        ("key_high", "<u8"),
                        ("key_low", "<u8"),
                        ("slots", "<u8", (2,)),
                        ("checksum", "<u8", (2,)),
                    ])
                    bucket_ds = buckets_group.create_dataset(
                        bucket_name,
                        shape=(0,),
                        maxshape=(None,),
                        dtype=hash_entry_dtype,
                        chunks=True,
                        track_times=False,
                    )
                    bucket_ds.attrs["local_depth"] = 1
                    bucket_ds.attrs["sorted_count"] = 0
                    bucket_ds.attrs["entry_count"] = 0
                else:
                    bucket_obj = buckets_group[bucket_name]
                    if isinstance(bucket_obj, h5py.Dataset):
                        bucket_ds = bucket_obj
                    else:
                        return  # Invalid bucket type

                # Always append new entry to the unsorted region
                # sorted_count = int(bucket_ds.attrs.get("sorted_count", 0))
                total = bucket_ds.shape[0]

                # Search for existing entry in both regions
                found_entry_idx = None
                for i in range(total):
                    entry = bucket_ds[i]
                    if entry["key_high"] == key.high and entry["key_low"] == key.low:
                        found_entry_idx = i
                        break

                if found_entry_idx is not None:
                    entry = bucket_ds[found_entry_idx]
                    slots = list(entry["slots"])
                    # If already in spill mode, append to ValueSet
                    if slots[0] == 0 and slots[1] != 0:
                        bucket_id = (
                            int(bucket_ds.attrs["bucket_id"])
                            if "bucket_id" in bucket_ds.attrs
                            else bucket_id
                        )
                        with self.hdf as f2:
                            sp_group = self._get_valueset_group(f2)
                            ds_name = self._get_spill_ds_name(bucket_id, key)
                            if ds_name in sp_group:
                                ds = sp_group[ds_name]
                                values = list(ds[:])
                                if int(value) not in values:
                                    values.append(int(value))
                                    ds.resize((len(values),))
                                    ds[:] = values
                        return
                    # Inline mode
                    if int(value) not in slots:
                        if slots.count(0) == 0:
                            self._promote_to_spill(
                                bucket_ds, found_entry_idx, key, new_value=value
                            )
                        else:
                            for j in range(len(slots)):
                                if slots[j] == 0:
                                    slots[j] = int(value)
                                    break
                            new_entry = (
                                entry["key_high"],
                                entry["key_low"],
                                tuple(slots),
                                entry["checksum"],
                            )
                            bucket_ds[found_entry_idx] = new_entry
                else:
                    # Append new entry to the end (unsorted region)
                    new_entry = (key.high, key.low, (int(value), 0), (0, 0))
                    old_size = bucket_ds.shape[0]
                    bucket_ds.resize((old_size + 1,))
                    bucket_ds[old_size] = new_entry
                    bucket_ds.attrs["entry_count"] = (
                        old_size + 1
                    )  # Check if bucket needs splitting
                if bucket_ds.shape[0] >= self.SPLIT_THRESHOLD:
                    await self._maybe_split_bucket(bucket_id)

    async def _maybe_split_bucket(self, bucket_id: int) -> None:
        """Split bucket if it's over threshold (adaptive based on danger score) and has sufficient local depth."""
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj

                if bucket_name in buckets_group:
                    bucket_obj = buckets_group[bucket_name]
                    if isinstance(bucket_obj, h5py.Dataset):
                        bucket_ds = bucket_obj
                        local_depth = bucket_ds.attrs.get("local_depth", 1)
                        entry_count = bucket_ds.attrs.get(
                            "entry_count", bucket_ds.shape[0]
                        )

                        # Use default split threshold (simplified)
                        adaptive_threshold = self.SPLIT_THRESHOLD

                        logger.info(
                            f"[_maybe_split_bucket] {bucket_name}: entries={entry_count}, "
                            f"using_threshold={adaptive_threshold}"
                        )

                        if entry_count >= adaptive_threshold:
                            if local_depth < self.global_depth:
                                # Can split without doubling directory
                                await self._split_bucket(bucket_id)
                            elif local_depth == self.global_depth:
                                # Need to double directory first
                                self._double_directory()
                                await self._split_bucket(bucket_id)

    def _double_directory(self) -> None:
        """Double the directory size (increase global_depth by 1)."""
        self.global_depth += 1

        # Double the bucket_pointers array
        old_pointers = self.bucket_pointers.copy()
        self.bucket_pointers = old_pointers + old_pointers

        # For sharded mode, we might need to expand shards or create new ones
        if self._directory_mode == "sharded":
            with self.hdf as f:
                config = f["/config"]
                shard_size = config.attrs.get("directory_shard_size", 50000)

                # Calculate if we need more shards
                num_shards_needed = (
                    len(self.bucket_pointers) + shard_size - 1
                ) // shard_size
                current_num_shards = len(self._directory_shards)

                if num_shards_needed > current_num_shards:
                    # Create additional shards
                    shards_group = f["directory_shards"]
                    dtype = np.dtype([
                        ("bucket_id", "<u8"),
                        ("hdf5_ref", "<u8"),
                    ])

                    for shard_id in range(current_num_shards, num_shards_needed):
                        shard_name = f"shard_{shard_id:04d}"
                        shard_ds = shards_group.create_dataset(
                            shard_name,
                            shape=(0,),
                            maxshape=(None,),
                            dtype=dtype,
                            chunks=True,
                            track_times=False,
                        )
                        self._directory_shards[shard_id] = shard_ds

        self._save_directory_metadata()

    async def _split_bucket(self, bucket_id: int) -> None:
        """Split a bucket into two buckets."""
        old_bucket_name = f"bucket_{bucket_id:04d}"
        new_bucket_id = self.num_buckets
        new_bucket_name = f"bucket_{new_bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj

                old_bucket_obj = buckets_group[old_bucket_name]
                if isinstance(old_bucket_obj, h5py.Dataset):
                    old_bucket = old_bucket_obj

                    # Create new bucket with same structure
                    new_bucket = buckets_group.create_dataset(
                        new_bucket_name,
                        shape=(0,),
                        maxshape=(None,),
                        dtype=old_bucket.dtype,
                        chunks=True,
                        track_times=False,
                    )

                    local_depth = old_bucket.attrs.get("local_depth", 1)
                    new_local_depth = local_depth + 1

                    new_bucket.attrs["local_depth"] = new_local_depth
                    old_bucket.attrs["local_depth"] = new_local_depth
                    new_bucket.attrs["sorted_count"] = 0
                    new_bucket.attrs["entry_count"] = 0

                    # Redistribute entries based on key bits
                    await self._redistribute_bucket_entries(
                        bucket_id, new_bucket_id, new_local_depth
                    )

                    self.num_buckets += 1
                    self._save_directory_metadata()

    async def _redistribute_bucket_entries(
        self, old_bucket_id: int, new_bucket_id: int, local_depth: int
    ) -> None:
        """Redistribute entries between old and new bucket based on key bit patterns."""
        old_bucket_name = f"bucket_{old_bucket_id:04d}"
        new_bucket_name = f"bucket_{new_bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj

                old_bucket_obj = buckets_group[old_bucket_name]
                new_bucket_obj = buckets_group[new_bucket_name]

                if isinstance(old_bucket_obj, h5py.Dataset) and isinstance(
                    new_bucket_obj, h5py.Dataset
                ):
                    old_bucket = old_bucket_obj
                    new_bucket = new_bucket_obj

                    # Read all entries from old bucket
                    old_entries = old_bucket[:]

                    old_bucket_entries = []
                    new_bucket_entries = []

                    # Redistribute based on key bit pattern
                    for entry in old_entries:
                        key_high = entry["key_high"]
                        bit_index = 64 - local_depth
                        bit_value = (key_high >> bit_index) & 1

                        if bit_value == 0:
                            old_bucket_entries.append(entry)
                        else:
                            new_bucket_entries.append(entry)

                    # Update buckets with redistributed entries
                    if old_bucket_entries:
                        old_bucket.resize((len(old_bucket_entries),))
                        old_bucket[:] = old_bucket_entries
                    else:
                        old_bucket.resize((0,))

                    if new_bucket_entries:
                        new_bucket.resize((len(new_bucket_entries),))
                        new_bucket[:] = new_bucket_entries  # Update entry counts
                    old_bucket.attrs["entry_count"] = len(old_bucket_entries)
                    new_bucket.attrs["entry_count"] = len(
                        new_bucket_entries
                    )  # Update directory pointers
                    self._update_directory_pointers_after_split(
                        old_bucket_id, new_bucket_id, local_depth
                    )

    async def run_adaptive_maintenance(self) -> None:
        """Perform simplified adaptive maintenance based on operation patterns."""
        logger.info(
            "[adaptive_maintenance] Starting adaptive maintenance check"
        )  # Get insights from simplified WAL analyzer
        assert hasattr(self, "maintenance_manager"), (
            "MaintenanceManager not initialized"
        )
        assert hasattr(self.maintenance_manager, "wal_analyzer_thread"), (
            "WAL analyzer thread not initialized"
        )

        insights = self.maintenance_manager.wal_analyzer_thread.get_insights()
        logger.info(f"[adaptive_maintenance] Processing {len(insights)} insights")

        for insight in insights:
            try:
                bucket_id = insight.bucket_id
                operation_count = insight.operation_count

                logger.info(
                    f"[adaptive_maintenance] Processing bucket {bucket_id} "
                    f"(operations={operation_count}): {insight.suggestion}"
                )

                # Check if bucket needs splitting based on high activity
                with self.hdf as f:
                    bucket_name = f"bucket_{bucket_id:04d}"
                    buckets_group = f["/buckets"]

                    if bucket_name in buckets_group:
                        bucket = buckets_group[bucket_name]
                        entry_count = bucket.attrs.get("entry_count", bucket.shape[0])

                        # Lower threshold for high-activity buckets
                        threshold = max(self.SPLIT_THRESHOLD // 2, 64)

                        if entry_count >= threshold:
                            logger.info(
                                f"[adaptive_maintenance] Triggering split for {bucket_name} "
                                f"(entries={entry_count}, threshold={threshold})"
                            )
                            await self._maybe_split_bucket(bucket_id)

            except Exception as e:
                logger.warning(
                    f"[adaptive_maintenance] Failed to process insight for bucket {insight.bucket_id}: {e}"
                )

        logger.info(
            f"[adaptive_maintenance] Completed ({len(insights)} insights processed)"
        )

    def _update_directory_pointers_after_split(
        self, old_bucket_id: int, new_bucket_id: int, local_depth: int
    ) -> None:
        """Update directory pointers after bucket split."""
        for i, pointer in enumerate(self.bucket_pointers):
            if pointer["bucket_id"] == old_bucket_id:
                # Check if this directory entry should point to new bucket
                directory_bit_pos = self.global_depth - local_depth
                if directory_bit_pos >= 0:
                    bit_value = (i >> directory_bit_pos) & 1

                    if bit_value == 1:
                        pointer["bucket_id"] = new_bucket_id

    async def delete(self, key: E) -> None:
        """Delete all values for a key and log the deletion."""
        logger.info(f"[CIDStore.delete] Called with {key=}")
        assert isinstance(key, E)

        # Get all current values for the key first
        values = await self.lookup(key)

        # Delete each value individually to ensure proper logging
        for value in values:
            await self.delete_value(key, value)

        # Log to WAL
        await self.wal.log_delete_key(key.high, key.low)

    async def delete_value(self, key: E, value: E) -> None:
        """Delete a specific value from a key and log the deletion."""
        logger.info(f"[CIDStore.delete_value] Called with {key=}, {value=}")
        assert isinstance(key, E)
        assert isinstance(value, E)

        bucket_id = self._find_bucket_id(key)
        bucket_name = (
            f"bucket_{bucket_id:04d}"  # Record operation in WAL pattern analyzer
        )
        # Record deletion operation for WAL analysis
        assert hasattr(self, "maintenance_manager"), (
            "MaintenanceManager not initialized"
        )
        assert hasattr(self.maintenance_manager, "wal_analyzer_thread"), (
            "WAL analyzer thread not initialized"
        )
        try:
            bucket_id = int(bucket_name.split("_")[-1]) if "_" in bucket_name else 0
            self.maintenance_manager.wal_analyzer_thread.record_operation(
                bucket_id, 2
            )  # Delete operation
        except (ValueError, IndexError):
            pass

        # Log to WAL
        await self.wal.log_delete(key.high, key.low, value.high, value.low)

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj

                if bucket_name in buckets_group:
                    bucket_obj = buckets_group[bucket_name]
                    if isinstance(bucket_obj, h5py.Dataset):
                        bucket = bucket_obj

                        # Find and update the entry
                        for i in range(bucket.shape[0]):
                            entry = bucket[i]
                            if (
                                entry["key_high"] == key.high
                                and entry["key_low"] == key.low
                            ):
                                slots = list(entry["slots"])

                                # Remove value from slots (set to 0)
                                for j in range(len(slots)):
                                    if slots[j] == int(value):
                                        slots[j] = 0
                                        # Log the deletion
                                        self.log_deletion(key, value)
                                        break

                                # Update entry
                                new_entry = (
                                    entry["key_high"],
                                    entry["key_low"],
                                    tuple(slots),
                                    entry["checksum"],
                                )
                                bucket[i] = new_entry
                                break

    async def rebalance_buckets(self) -> None:
        """Rebalance buckets by merging underfilled ones."""
        logger.info("[CIDStore.rebalance_buckets] Starting bucket rebalancing")

        with self.hdf as f:
            buckets_group = f["/buckets"]
            bucket_ids = [int(name.split("_")[-1]) for name in buckets_group]

            for bucket_id in bucket_ids:
                await self._maybe_merge_bucket(bucket_id)

        logger.info("[CIDStore.rebalance_buckets] Bucket rebalancing completed")

    # Context management

    # Debug and utility methods
    def debug_dump(self) -> str:
        """Return debug information about the store structure."""
        output = [
            f"CIDStore debug dump (global_depth={self.global_depth}, num_buckets={self.num_buckets})"
        ]
        output.append(f"Directory size: {len(self.bucket_pointers)}")

        # Directory pointers
        for i, pointer in enumerate(self.bucket_pointers):
            output.append(f"  Dir[{i:04d}] -> Bucket {pointer['bucket_id']}")

        # Bucket details
        for bucket_id in range(self.num_buckets):
            output.append(self.debug_dump_bucket(bucket_id))

        return "\n".join(output)

    def debug_dump_bucket(self, bucket_id: int) -> str:
        """Return debug information about a specific bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj

                if bucket_name in buckets_group:
                    bucket_obj = buckets_group[bucket_name]
                    if isinstance(bucket_obj, h5py.Dataset):
                        bucket = bucket_obj
                        local_depth = bucket.attrs.get("local_depth", 1)
                        entry_count = bucket.attrs.get("entry_count", 0)

                        output = [
                            f"Bucket {bucket_id}:",
                            f"  Local depth: {local_depth}",
                            f"  Entry count: {entry_count}",
                            "  Entries:",
                        ]

                        for i in range(bucket.shape[0]):
                            entry = bucket[i]
                            key = E((entry["key_high"] << 64) | entry["key_low"])
                            slots = [s for s in entry["slots"] if s != 0]
                            output.append(f"    {key} -> {slots}")

                        return "\n".join(output)

        return f"Bucket {bucket_id}: Not found"

    def _init_metrics_and_autotune(self):
        """Initialize metrics collection and auto-tuning."""
        from .metrics import init_metrics_and_autotune

        init_metrics_and_autotune(self)

    # --- Spec 3: Sorted/Unsorted Region Support ---
    def get_sorted_count(self, bucket_id: int) -> int:
        """Return the number of sorted entries in the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                bucket = buckets_group[bucket_name]
                if hasattr(bucket, "attrs"):
                    return int(bucket.attrs.get("sorted_count", 0))
        return 0

    def get_unsorted_count(self, bucket_id: int) -> int:
        """Return the number of unsorted entries in the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                bucket = buckets_group[bucket_name]
                if hasattr(bucket, "shape") and hasattr(bucket, "attrs"):
                    total = bucket.shape[0]
                    sorted_count = int(bucket.attrs.get("sorted_count", 0))
                    return total - sorted_count
        return 0

    def get_sorted_region(self, bucket_id: int):
        """Return the sorted region (as a list of entries) of the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                bucket = buckets_group[bucket_name]
                if hasattr(bucket, "attrs"):
                    sorted_count = int(bucket.attrs.get("sorted_count", 0))
                    return list(bucket[:sorted_count])
        return []

    def get_unsorted_region(self, bucket_id: int):
        """Return the unsorted region (as a list of entries) of the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                bucket = buckets_group[bucket_name]
                if hasattr(bucket, "attrs"):
                    sorted_count = int(bucket.attrs.get("sorted_count", 0))
                    return list(bucket[sorted_count:])
        return []

    def sort_bucket(self, bucket_id: int):
        """Sort the unsorted region and merge it into the sorted region."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                bucket = buckets_group[bucket_name]
                sorted_count = int(bucket.attrs.get("sorted_count", 0))
                total = bucket.shape[0]
                if sorted_count < total:
                    # Merge unsorted region into sorted region
                    all_entries = list(bucket[:])
                    # Sort all by key_high, key_low
                    all_entries.sort(key=lambda e: (e["key_high"], e["key_low"]))
                    bucket[:] = all_entries
                    bucket.attrs["sorted_count"] = total
                    bucket.flush()

    def get_tombstone_count(self, key: E) -> int:
        """Count tombstones (zeros) in the slots for a given key."""
        entry = self.get_entry_sync(key)
        if entry is None:
            return 0

        slots = entry["slots"]
        if not isinstance(slots, (list, tuple)):
            return 0

        return sum(1 for slot in slots if slot == 0)

    async def valueset_exists(self, key: E) -> bool:
        """Check if a valueset exists for the given key."""
        bucket_id = self._find_bucket_id(key)
        with self.hdf as f:
            sp_group = self._get_valueset_group(f)
            ds_name = self._get_spill_ds_name(bucket_id, key)
            return ds_name in sp_group
