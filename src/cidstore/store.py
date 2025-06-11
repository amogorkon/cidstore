"""store.py - Main CIDStore class with proper extendible hashing implementation"""

from __future__ import annotations

import threading
from asyncio import Task, create_task
from typing import Any

import numpy as np
from h5py import Dataset, Group

from cidstore.constants import (
    BUCKET_POINTER_DTYPE,
    BUCKS,
    CONF,
    DIRECTORY_DATASET_THRESHOLD,
    DIRECTORY_SHARD_THRESHOLD,
    HASH_ENTRY_DTYPE,
    SPLIT_THRESHOLD,
    OpType,
)

from .constants import OP
from .keys import E
from .logger import get_logger
from .maintenance import MaintenanceConfig, MaintenanceManager
from .metrics import init_metrics_and_autotune
from .storage import Storage
from .utils import assumption
from .wal import WAL

logger = get_logger(__name__)


class CIDStore:
    """
    CIDStore: Main entry point for the CIDStore hash directory.
    All public methods are async and log to WAL asynchronously.
    Implements extendible hashing with global_depth and local_depth per Spec 3.
    """

    def __init__(self, hdf: Storage, wal: WAL, testing=False) -> None:
        assert assumption(hdf, Storage)
        assert assumption(wal, WAL)
        self.hdf = hdf
        self.wal = wal
        self.debugging = testing
        self.global_depth: int = 1
        self.bucket_pointers: list[dict] = []
        "Array of BucketPointer"
        self.num_buckets: int = 0
        "Number of buckets in the directory"
        self._writer_lock = threading.RLock()
        self._directory_dataset = None
        "Directory dataset for attribute/dataset mode"
        self._directory_attr_threshold = 1000
        "Threshold for attribute → dataset migration"
        self._directory_ds_threshold = 100000
        "Threshold for dataset → sharded migration"
        self._directory_shards = {}
        "Dict of shard_id → dataset for sharded mode"
        assert BUCKS in self.hdf, "Buckets group doesn't exist in HDF5 file?"
        self._bucket_counter = 0
        "Counter for bucket IDs, used for new buckets"
        self._wal_consumer_task: Task | None = None
        "Background task for WAL consumption"
        self.wal.apply = self.apply

        # Initialize extendible hash directory
        self._init_directory()

        # Replay WAL for recovery
        if not testing:
            self._init_deletion_log_and_gc()  # Initialize metrics and auto-tuning
            init_metrics_and_autotune(self)

    async def async_init(self) -> None:
        await self._replay_wal()
        if self._wal_consumer_task is None:
            self._wal_consumer_task = create_task(self.wal.consume_polling())

    def _migrate_attr_to_dataset(self):
        """Migrate directory from attribute mode to dataset mode."""
        if "directory" not in self.hdf:
            ds = self.hdf.file.create_dataset(
                "directory",
                shape=(len(self.bucket_pointers),),
                maxshape=(None,),
                dtype=BUCKET_POINTER_DTYPE,
                chunks=True,
                track_times=False,
            )
            for i, pointer in enumerate(self.bucket_pointers):
                ds[i] = (pointer["bucket_id"], pointer.get("hdf5_ref", 0))
            self._directory_dataset = ds
            logger.info(
                f"[CIDStore] Migrated directory to dataset mode (size={len(self.bucket_pointers)})"
            )
            config = self.hdf[CONF]
            if "directory" in config.attrs:
                del config.attrs["directory"]

    def _migrate_dataset_to_sharded(self):
        """Migrate directory from dataset mode to sharded mode."""
        # Create directory shards group
        if "directory_shards" not in self.hdf:
            self.hdf.create_group("directory_shards")
        shards_group = self.hdf["directory_shards"]

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

            self._directory_dataset = None

            # Store shard metadata
            config = self.hdf[CONF]
            config.attrs["directory_shard_size"] = shard_size
            config.attrs["directory_num_shards"] = num_shards

            logger.info(
                f"[CIDStore] Migrated directory to sharded mode (size={len(self.bucket_pointers)}, shards={num_shards})"
            )

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
        assert hasattr(self, "maintenance_manager"), "Maintenance not initialized"
        self.maintenance_manager.log_deletion(key.high, key.low, value.high, value.low)

    def run_gc_once(self):
        # Delegate to maintenance manager
        assert hasattr(self, "maintenance_manager"), "Maintenance not initialized"
        self.maintenance_manager.run_gc_once()

    async def _maybe_merge_bucket(self, bucket_id: int, merge_threshold: int = 8):
        """Automatically merge bucket with its pair if both are underfull and local_depth > 1."""
        bucket_name = f"bucket_{bucket_id:04d}"
        buckets_group = self.hdf[BUCKS]
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
            if (i & ~mask) >> (local_depth - 1) == (new_bucket_id >> (local_depth - 1)):
                pointer["bucket_id"] = new_bucket_id
        self.num_buckets -= 1
        self._save_directory_metadata()

    def compact(self, key: E):
        """Remove tombstones (0s) from ValueSet for a key."""
        _, bucket_id = self._bucket_name_and_id(key.high, key.low)
        sp_group = self._get_valueset_group(self.hdf)
        ds_name = self._get_spill_ds_name(bucket_id, key)
        if ds_name in sp_group:
            ds = sp_group[ds_name]
            values = [v for v in ds[:] if v != 0]
            ds.resize((len(values),))
            ds[:] = values

    # --- Spec 5: Multi-Value Key Promotion/Demotion and ValueSet Compaction ---

    def is_spilled(self, key: E) -> bool:
        """Return True if the key is in spill (ValueSet) mode."""
        entry = self.get_entry(key)
        if entry is None:
            return False
        slots = entry["slots"]
        if not isinstance(slots, (list, tuple)) or len(slots) < 2:
            return False
        return slots[0] == 0 and slots[1] != 0

    def _get_valueset_group(self, f):
        if "/values" not in f:
            f.create_group("/values")
        values_group = f["/values"]
        assert assumption(values_group, Group)
        if "sp" not in values_group:
            values_group.create_group("sp")
        return values_group["sp"]

    def _get_spill_ds_name(self, bucket_id, key):
        return f"sp_{bucket_id}_{key.high}_{key.low}"

    def _demote_from_spill(self, bucket, entry_idx, key):
        """Demote ValueSet (spill) entry to inline if <=2 values remain."""
        bucket_id = (
            int(bucket.attrs["bucket_id"]) if "bucket_id" in bucket.attrs else None
        )
        if bucket_id is None:
            bucket_id = int(bucket.name.split("_")[-1])
        sp_group = self._get_valueset_group(self.hdf)
        ds_name = self._get_spill_ds_name(bucket_id, key)
        if ds_name not in sp_group or not isinstance(sp_group, Group):
            return
        ds = sp_group[ds_name]
        if not isinstance(ds, Dataset):
            return
        values = [E(v) for v in ds[:] if v != 0]
        if len(values) <= 2:
            # Demote: move values back inline, delete ValueSet
            slots = [int(v) for v in values] + [0] * (2 - len(values))
            bucket[entry_idx]["slots"] = tuple(slots)
            del sp_group[ds_name]
            bucket.flush()

    async def wal_checkpoint(self):
        self.hdf.file.flush()
        self.wal.flush()

    async def apply(self, op: OP) -> None:
        """Apply a WAL operation to the store."""
        bucket_name, bucket_id = self._bucket_name_and_id(op.k_high, op.k_low)
        if not self.debugging:
            self.maintenance_manager.wal_analyzer_thread.record_operation(
                bucket_id, op.optype
            )
        match op.optype:
            case OpType.INSERT:
                await self.hdf.apply_insert(op, bucket_name)
            case OpType.DELETE:
                await self.hdf.apply_delete(op, bucket_name)

    async def _replay_wal(self):
        """Replay WAL records to restore the store state."""
        logger.info("[CIDStore] Replaying WAL for recovery")
        for op in self.wal.replay():
            await self.apply(op)

    async def recover(self) -> None:
        """Recover from WAL and replay any pending operations."""
        logger.info("[CIDStore.recover] Starting WAL recovery")
        self.wal.replay()
        logger.info("[CIDStore.recover] WAL recovery completed")

    def _init_directory(self) -> None:
        """
        Initialize the extendible hash directory structure.
        Load existing directory from HDF5 or create a new one.
        """
        config = self.hdf.file.require_group(CONF)

        # Load or initialize global_depth
        self.global_depth = config.attrs.get("global_depth", 1)
        self.num_buckets = config.attrs.get("num_buckets", 0)
        directory_size = 2**self.global_depth
        "Size of the directory (2^global_depth) for extendible hashing"

        if "directory_shards" in self.hdf:
            self._init_directory_sharded(config, directory_size)
        elif "directory" in self.hdf:
            self._init_directory_dataset(directory_size)
        else:
            self._init_directory_attr(directory_size)

    def _init_directory_sharded(self, config: Any, directory_size: int) -> None:
        """Initialize directory, sharded storage mode."""
        shards_group = self.hdf["directory_shards"]
        self._directory_mode = "sharded"
        self._directory_dataset = None
        self._directory_shards = {}

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

    def _init_directory_dataset(self, directory_size: int) -> None:
        """Initialize directory from dataset, storage mode."""
        # Load existing directory dataset
        directory_ds = self.hdf["directory"]
        assert assumption(directory_ds, Dataset)
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

    def _init_directory_attr(self, directory_size: int) -> None:
        """Initialize a new directory, attr mode."""
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
        buckets_group = self.hdf[BUCKS]
        assert assumption(buckets_group, Group)
        buckets_group = buckets_group
        if bucket_name not in buckets_group:
            # Use hash_entry_dtype from specs

            bucket_ds = buckets_group.create_dataset(
                bucket_name,
                shape=(0,),
                maxshape=(None,),
                dtype=HASH_ENTRY_DTYPE,
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
        config = self.hdf[CONF]
        assert assumption(config, Group)
        config.attrs["global_depth"] = self.global_depth
        config.attrs["num_buckets"] = self.num_buckets
        self.hdf.flush()
        self._save_directory_to_storage()

    # Directory storage thresholds

    def _save_directory_to_storage(self) -> None:
        """Save directory bucket pointers to appropriate storage format based on current state."""
        directory_size = len(self.bucket_pointers)

        if "directory_shards" in self.hdf:
            # Already in sharded mode, just update
            self._update_sharded_directory()
            logger.info(
                f"[CIDStore] Directory updated in sharded mode (size={directory_size})"
            )
        elif "directory" in self.hdf:
            # Already in dataset mode
            if directory_size > DIRECTORY_SHARD_THRESHOLD:
                self._migrate_dataset_to_sharded()
                logger.info(
                    f"[CIDStore] Directory migrated from dataset to sharded mode (size={directory_size})"
                )
            else:
                self._update_directory_dataset()
                logger.info(
                    f"[CIDStore] Directory updated in dataset mode (size={directory_size})"
                )
        elif directory_size > DIRECTORY_SHARD_THRESHOLD:
            # Large directory - migrate to sharded mode
            self._migrate_attr_to_dataset()
            self._migrate_dataset_to_sharded()
            logger.info(
                f"[CIDStore] Directory migrated from attr to sharded mode (size={directory_size})"
            )
        elif directory_size > DIRECTORY_DATASET_THRESHOLD:
            # Medium directory - migrate to dataset mode
            self._migrate_attr_to_dataset()
            logger.info(
                f"[CIDStore] Directory migrated from attr to dataset mode (size={directory_size})"
            )
        else:
            # Small directory - use attribute mode
            self._save_directory_as_attributes()
            logger.info(
                f"[CIDStore] Directory saved as attributes (size={directory_size})"
            )

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

    def _save_directory_as_attributes(self) -> None:
        """Save small directory as HDF5 attributes in config group."""
        config = self.hdf[CONF]
        assert assumption(config, Group)

        # Convert bucket pointers to a simple format for attribute storage
        directory_data = []
        directory_data.extend(
            [pointer["bucket_id"], pointer.get("hdf5_ref", 0)]
            for pointer in self.bucket_pointers
        )
        # Store as HDF5 attribute - convert to numpy array for efficient storage
        import numpy as np

        directory_array = np.array(directory_data, dtype=BUCKET_POINTER_DTYPE)
        config.attrs["directory"] = directory_array

        logger.info(
            f"[CIDStore] Saved directory as attributes (size={len(self.bucket_pointers)})"
        )

    def _update_sharded_directory(self) -> None:
        """Update sharded directory with current bucket pointers."""
        if not self._directory_shards:
            return

        config = self.hdf[CONF]
        shard_size = config.attrs.get("directory_shard_size", 50000)

        # Calculate required number of shards
        num_shards_needed = (len(self.bucket_pointers) + shard_size - 1) // shard_size

        # Ensure we have enough shards
        shards_group = self.hdf["directory_shards"]
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
                shard_ds[i] = (
                    pointer["bucket_id"],
                    pointer.get("hdf5_ref", 0),
                )  # Update metadata
        config.attrs["directory_num_shards"] = num_shards_needed

        logger.info(
            f"[CIDStore] Updated sharded directory metadata (shards={num_shards_needed})"
        )

    def _bucket_name_and_id(self, high: int, low: int) -> tuple[str, int]:
        """
        Find bucket ID for a key using extendible hashing (Spec 3).
        Extract top global_depth bits from key.high as directory index.
        """
        directory_index = high >> (64 - self.global_depth)
        directory_index = directory_index % len(self.bucket_pointers)
        bucket_id = self.bucket_pointers[directory_index]["bucket_id"]
        return f"bucket_{bucket_id:04d}", bucket_id

    async def get(self, key: E) -> list[E]:
        """Get all values for a key using binary search in sorted region and linear scan in unsorted region."""
        logger.info(f"[CIDStore.get] Called with {key=}")
        assert assumption(key, E)
        bucket_name, bucket_id = self._bucket_name_and_id(key.high, key.low)
        buckets_group = self.hdf[BUCKS]
        assert buckets_group is not None, "Buckets group not in HDF5 file!"
        assert assumption(buckets_group, Group)

        if not (bucket_obj := buckets_group[bucket_name]):
            logger.info(f"[CIDStore.get] No bucket found for {key=}")
            return []

        assert assumption(bucket_obj, Dataset)

        sorted_count = int(bucket_obj.attrs.get("sorted_count", 0))
        total = bucket_obj.shape[0]

        # --- Binary search in sorted region ---
        left, right = 0, sorted_count - 1
        while left <= right:
            mid = (left + right) // 2
            entry = bucket_obj[mid]
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                return self.hdf.get_values(entry)
            elif (entry["key_high"], entry["key_low"]) < (key.high, key.low):
                left = mid + 1
            else:
                right = mid - 1

        # --- Linear scan in unsorted region ---
        for i in range(sorted_count, total):
            entry = bucket_obj[i]

            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                assert entry.dtype == HASH_ENTRY_DTYPE, (
                    f"Entry dtype mismatch: got {entry.dtype}, expected {HASH_ENTRY_DTYPE}"
                )

                return self.hdf.get_values(entry)
        return []

    async def get_entry(self, key: E) -> dict[str, Any] | None:
        """Get bucket entry for a key (for debugging and testing), using binary search in sorted region and linear scan in unsorted region."""
        bucket_name, _ = self._bucket_name_and_id(key.high, key.low)

        buckets_group = self.hdf[BUCKS]
        assert assumption(buckets_group, Group)

        if bucket_name not in buckets_group:
            return None

        bucket_obj = buckets_group[bucket_name]
        assert assumption(bucket_obj, Dataset)

        sorted_count = int(bucket_obj.attrs.get("sorted_count", 0))
        total = bucket_obj.shape[0]

        # --- Binary search in sorted region ---
        left, right = 0, sorted_count - 1
        while left <= right:
            mid = (left + right) // 2
            entry = bucket_obj[mid]
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
            entry = bucket_obj[i]
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
                return {
                    "key_high": int(entry["key_high"]),
                    "key_low": int(entry["key_low"]),
                    "slots": list(entry["slots"]),
                    "checksum": list(entry["checksum"]),
                }

        return None

    async def insert(self, key: E, value: E) -> None:
        """Insert a key-value pair into the CIDStore."""
        logger.info(f"[CIDStore.insert] Called with {key=}, {value=}")
        assert assumption(key, E)
        assert assumption(value, E)

        if V := await self.get(key):
            if value in V:
                logger.info(
                    f"[CIDStore.insert] Value {value} already exists for key {key}"
                )
                return
        await self.wal.log_insert(key.high, key.low, value.high, value.low)

    async def _maybe_split_bucket(self, bucket_id: int) -> None:
        """Split bucket if it's over threshold (adaptive based on danger score) and has sufficient local depth."""
        buckets_group = self.hdf[BUCKS]
        if isinstance(buckets_group, Group):
            bucket_name = f"bucket_{bucket_id:04d}"

            if bucket_name in buckets_group:
                bucket_obj = buckets_group[bucket_name]
                if isinstance(bucket_obj, Dataset):
                    bucket_ds = bucket_obj
                    local_depth = bucket_ds.attrs.get("local_depth", 1)
                    entry_count = bucket_ds.attrs.get("entry_count", bucket_ds.shape[0])

                    logger.info(
                        f"[_maybe_split_bucket] {bucket_name}: entries={entry_count}, "
                        f"using_threshold={SPLIT_THRESHOLD}"
                    )

                    if entry_count >= SPLIT_THRESHOLD:
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
            config = self.hdf[CONF]
            shard_size = config.attrs.get("directory_shard_size", 50000)

            # Calculate if we need more shards
            num_shards_needed = (
                len(self.bucket_pointers) + shard_size - 1
            ) // shard_size
            current_num_shards = len(self._directory_shards)

            if num_shards_needed > current_num_shards:
                # Create additional shards
                shards_group = self.hdf["directory_shards"]
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

        buckets_group = self.hdf[BUCKS]
        assert assumption(buckets_group, Group)

        old_bucket_obj = buckets_group[old_bucket_name]
        assert assumption(old_bucket_obj, Dataset)
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

        buckets_group: Group = self.hdf[BUCKS]
        assert assumption(buckets_group, Group)

        old_bucket_obj = buckets_group[old_bucket_name]
        new_bucket_obj = buckets_group[new_bucket_name]

        assert assumption(old_bucket_obj, Dataset)
        assert assumption(new_bucket_obj, Dataset)

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

                bucket_name = f"bucket_{bucket_id:04d}"
                buckets_group = self.hdf[BUCKS]

                if bucket_name in buckets_group:
                    bucket = buckets_group[bucket_name]
                    entry_count = bucket.attrs.get("entry_count", bucket.shape[0])

                    # Lower threshold for high-activity buckets
                    threshold = max(SPLIT_THRESHOLD // 2, 64)

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
        assert assumption(key, E)

        # Get all current values for the key first
        values = await self.get(key)

        # Delete each value individually to ensure proper logging
        for value in values:
            await self.delete_value(key, value)

        # Log to WAL
        await self.wal.log_delete(key.high, key.low)

    async def delete_value(self, key: E, value: E) -> None:
        """Delete a specific value from a key and log the deletion."""
        logger.info(f"[CIDStore.delete_value] Called with {key=}, {value=}")
        assert assumption(key, E)
        assert assumption(value, E)

        bucket_name, bucket_id = self._bucket_name_and_id(key.high, key.low)
        assert hasattr(self, "maintenance_manager"), (
            "MaintenanceManager not initialized"
        )
        assert hasattr(self.maintenance_manager, "wal_analyzer_thread"), (
            "WAL analyzer thread not initialized"
        )
        try:
            bucket_id = int(bucket_name.split("_")[-1]) if "_" in bucket_name else 0
            self.maintenance_manager.wal_analyzer_thread.record_operation(
                bucket_id, OpType.DELETE
            )
        except (ValueError, IndexError):
            pass

        await self.wal.log_delete_value(key.high, key.low, value.high, value.low)

        buckets_group = self.hdf[BUCKS]
        assert assumption(buckets_group, Group)
        assert bucket_name in buckets_group, "Bucket not found in HDF5 file!"

        bucket_obj = buckets_group[bucket_name]
        assert assumption(bucket_obj, Dataset)
        bucket = bucket_obj

        # Find and update the entry
        for i in range(bucket.shape[0]):
            entry = bucket[i]
            if entry["key_high"] == key.high and entry["key_low"] == key.low:
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

        buckets_group = self.hdf[BUCKS]
        bucket_ids = [int(name.split("_")[-1]) for name in buckets_group]

        for bucket_id in bucket_ids:
            await self._maybe_merge_bucket(bucket_id)

        logger.info("[CIDStore.rebalance_buckets] Bucket rebalancing completed")

    # Context management

    # Debug and utility methods
    def debug_dump(self) -> str:
        """Return debug information about the store structure."""
        output = [
            f"CIDStore debug dump (global_depth={self.global_depth}, num_buckets={self.num_buckets})",
            f"Directory size: {len(self.bucket_pointers)}",
        ]
        # Directory pointers
        output.extend(
            f"  Dir[{i:04d}] -> Bucket {pointer['bucket_id']}"
            for i, pointer in enumerate(self.bucket_pointers)
        )
        # Bucket details
        output.extend(
            self.debug_dump_bucket(bucket_id) for bucket_id in range(self.num_buckets)
        )
        return "\n".join(output)

    def debug_dump_bucket(self, bucket_id: int) -> str:
        """Return debug information about a specific bucket."""
        buckets_group = self.hdf[BUCKS]
        if isinstance(buckets_group, Group):
            buckets_group = buckets_group

            bucket_name = f"bucket_{bucket_id:04d}"
            if bucket_name in buckets_group:
                bucket_obj = buckets_group[bucket_name]
                if isinstance(bucket_obj, Dataset):
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

    # --- Spec 3: Sorted/Unsorted Region Support ---
    def get_sorted_count(self, bucket_id: int) -> int:
        """Return the number of sorted entries in the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        buckets_group = self.hdf[BUCKS]
        if bucket_name in buckets_group:
            bucket = buckets_group[bucket_name]
            if hasattr(bucket, "attrs"):
                return int(bucket.attrs.get("sorted_count", 0))
        return 0

    def get_unsorted_count(self, bucket_id: int) -> int:
        """Return the number of unsorted entries in the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        buckets_group = self.hdf[BUCKS]
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

        buckets_group = self.hdf[BUCKS]
        if bucket_name in buckets_group:
            bucket = buckets_group[bucket_name]
            if hasattr(bucket, "attrs"):
                sorted_count = int(bucket.attrs.get("sorted_count", 0))
                return list(bucket[:sorted_count])
        return []

    def get_unsorted_region(self, bucket_id: int):
        """Return the unsorted region (as a list of entries) of the bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"
        buckets_group = self.hdf[BUCKS]
        if bucket_name in buckets_group:
            bucket = buckets_group[bucket_name]
            if hasattr(bucket, "attrs"):
                sorted_count = int(bucket.attrs.get("sorted_count", 0))
                return list(bucket[sorted_count:])
        return []

    def sort_bucket(self, bucket_id: int):
        """Sort the unsorted region and merge it into the sorted region."""
        bucket_name = f"bucket_{bucket_id:04d}"

        buckets_group = self.hdf[BUCKS]
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

        return sum(slot == 0 for slot in slots)

    async def valueset_exists(self, key: E) -> bool:
        """Check if a valueset exists for the given key."""
        _, bucket_id = self._bucket_name_and_id(key.high, key.low)

        sp_group = self._get_valueset_group(self.hdf)
        ds_name = self._get_spill_ds_name(bucket_id, key)
        return ds_name in sp_group

    def close(self) -> None:
        """Close underlying HDF5 and WAL resources."""
        if hasattr(self, "hdf") and hasattr(self.hdf, "close"):
            self.hdf.close()
        if hasattr(self, "wal") and hasattr(self.wal, "close"):
            self.wal.close()

    async def aclose(self) -> None:
        """Async close for compatibility with async contexts."""
        self.close()
