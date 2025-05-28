"""store.py - Main CIDStore class, directory, bucket, and ValueSet logic (Spec 2)"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
from typing import Any, Iterable

import numpy as np

from .keys import E
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
        self.wal = wal
        
        # Extendible hashing components (Spec 3)
        self.global_depth: int = 1  # Directory depth
        self.bucket_pointers: list[dict] = []  # Array of BucketPointer entries
        self.num_buckets: int = 0
        
        self._writer_lock = threading.RLock()
        self.hdf._init_hdf5_layout()
        self._directory_mode = "attr"
        self._directory_dataset = None
        self._directory_attr_threshold = 1000
        with self.hdf as f:
            if "/buckets" not in f:
                f.create_group("/buckets")
        self._bucket_counter = 0
        self._wal_consumer_task: asyncio.Task
        self.wal.consume = self.apply
        
        # Initialize extendible hash directory
        self._init_directory()

    async def async_init(self) -> None:
        self._wal_consumer_task = asyncio.create_task(self.wal.consume_polling())

    async def wal_checkpoint(self):
        self.hdf.file.flush()
        self.wal.flush()

    async def auto_tune(self, metrics):
        """Delegate auto-tuning to the AutoTuner component."""
        await self.auto_tuner.auto_tune(metrics)

    async def batch_insert(self, items: list[tuple[E, E]]) -> None:
        assert all(isinstance(k, E) and isinstance(v, E) for k, v in items)
        await self.wal.batch_insert([
            (key.high, key.low, value.high, value.low) for key, value in items
        ])

    async def batch_delete(self, keys: list[E]) -> None:
        assert all(isinstance(k, E) for k in keys)
        await self.wal.batch_delete([(key.high, key.low) for key in keys])

    async def insert(self, key: E, value: E) -> None:
        logger.info(f"[CIDStore.insert] Called with {key=} {value=}")
        assert isinstance(key, E)
        assert isinstance(value, E)
        assert key != E(0) != value
        V = await self.get(key)
        if V and value in V:
            logger.info(f"[CIDStore.insert] Value already present for {key=}")
            return        await self.wal.log_insert(key.high, key.low, value.high, value.low)
        logger.info(f"[CIDStore.insert] Inserted value for {key=}")

    async def delete(self, key: E) -> None:
        """
        Delete a key and all its values.
        Args:
            key: The key to delete (E).
        Removes all values (inline and spill) for the key.
        """
        assert isinstance(key, E)
        await self.wal.log_delete(key.high, key.low)

    async def get(self, key: E) -> Iterable[E]:
        logger.info(f"[CIDStore.get] Called with {key=}")
        assert isinstance(key, E)
        
        # Use extendible hashing to find bucket
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"
        
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                logger.info(f"[CIDStore.get] No bucket found for {key=}")
                return iter([])
            
            bucket = buckets_group[bucket_name]
            
            # Search bucket for key
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    # Check if it's spilled to external ValueSet
                    if len(entry["slots"]) > 0 and entry["slots"][0] != 0:
                        # Inline values
                        logger.info(f"[CIDStore.get] Returning inline values for {key=}")
                        return (E(slot) for slot in entry["slots"] if slot != 0)
                    else:
                        # Check for spilled values
                        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"                        values_group = f.get("/values/sp")
                        if values_group and spill_ds_name in values_group:
                            logger.info(f"[CIDStore.get] Returning spill values for {key=}")
                            return (E(v) for v in values_group[spill_ds_name][:] if v != 0)
                        else:
                            logger.info(f"[CIDStore.get] No values found for {key=}")
                            return iter([])
            
            logger.info(f"[CIDStore.get] Key not found in bucket: {key=}")
            return iter([])

    async def get_entry(self, key: E) -> dict[str, Any] | None:
        """
        Retrieve the entry for a key, including all canonical fields.
        Args:
            key: Key to retrieve (E).
        Returns:
            dict or None: Entry dict with key, key_high, key_low, slots, values.
        """
        assert isinstance(key, E)
        
        # Use extendible hashing to find bucket
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"
        
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                return None
            
            bucket = buckets_group[bucket_name]
            
            # Search bucket for key
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    entry_dict = {
                        "key": key,
                        "key_high": int(entry["key_high"]),
                        "key_low": int(entry["key_low"]),
                        "slots": entry["slots"].copy(),
                    }
                    
                    # Check if values are inline or spilled
                    if len(entry["slots"]) > 0 and entry["slots"][0] != 0:
                        # Inline values
                        values = [slot for slot in entry["slots"] if slot != 0]
                    else:
                        # Check for spilled values
                        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                        values_group = f.get("/values/sp")
                        values = (
                            [v for v in values_group[spill_ds_name][:] if v != 0]
                            if values_group and spill_ds_name in values_group
                            else []
                        )
                    
                    entry_dict["values"] = values
                    entry_dict["value"] = values[0] if values else None
                    return entry_dict
            
            return None

    async def delete_value(self, key: E, value: E) -> None:
        await self.wal.log_delete_value(key.high, key.low, value.high, value.low)

    def _load_directory(self) -> None:
        """
        Load directory from HDF5 attributes or canonical dataset (Spec 3).
        """
        with self.hdf as f:
            if "directory" in f:
                ds = f["directory"]
                self._directory_mode = "ds"
                self._directory_dataset = ds
                self.dir = {
                    E((int(row["key_high"]) << 64) | int(row["key_low"])): int(
                        row["bucket_id"]
                    )
                    for row in ds
                }
            elif CONF in f and "directory" in f[CONF].attrs:
                self._directory_mode = "attr"
                attr = f[CONF].attrs["directory"]
                if isinstance(attr, bytes):
                    attr = attr.decode("utf-8")
                try:
                    loaded = json.loads(attr)
                except Exception:
                    loaded = {}
                self.dir = {
                    E.from_str(k) if hasattr(E, "from_str") else E(int(k)): v
                    for k, v in loaded.items()
                }
            else:
                self._directory_mode = "attr"
                self.dir = {}

    async def _save_directory(self) -> None:
        """
        Save directory to HDF5 attributes or canonical dataset (Spec 3).
        """
        if self._directory_mode == "attr":
            self.hdf.file[CONF].attrs.modify("dir", json.dumps(self.dir))
        elif self._directory_mode == "ds":
            ds = self._directory_dataset
            ds.resize((len(self.dir),))
            for i, (k, v) in enumerate(self.dir.items()):
                key_int = int(k)
                key_high = key_int >> 64
                key_low = key_int & ((1 << 64) - 1)
                ds[i]["key_high"] = key_high
                ds[i]["key_low"] = key_low
                ds[i]["bucket_id"] = v
                ds[i]["spill_ptr"] = b""
                ds[i]["state_mask"] = 0
                ds[i]["version"] = 1
        self.hdf.file.flush()

    async def _maybe_migrate_directory(self) -> None:
        """
        Migrate directory from attribute to canonical, scalable, sharded/hybrid dataset.
        Only migrates if the directory exceeds the attribute threshold.
        """
        if (
            self._directory_mode != "attr"
            or len(self.dir) <= self._directory_attr_threshold
        ):
            return
        f = self.hdf.file
        dt = np.dtype([
            ("key_high", "<u8"),
            ("key_low", "<u8"),
            ("bucket_id", "<i8"),
            ("spill_ptr", "S32"),
            ("state_mask", "u1"),
            ("version", "<u4"),
        ])
        # Hybrid: single dataset if >1M buckets else sharded directory
        SHARD_THRESHOLD = 1_000_000
        items = list(self.dir.items())
        if len(items) > SHARD_THRESHOLD:
            self._shard_items_into_directory(items, f, dt)
        else:
            ds = f.create_dataset(
                "dir",
                shape=(len(items),),
                maxshape=(None,),
                dtype=dt,
                chunks=True,
                track_times=False,
            )
            for i, (k, v) in enumerate(items):
                ds[i]["key_high"] = k.high
                ds[i]["key_low"] = k.low
                ds[i]["bucket_id"] = v
                ds[i]["spill_ptr"] = b""
                ds[i]["state_mask"] = 0
                ds[i]["version"] = 1
            self._directory_mode = "ds"
            self._directory_dataset = ds
        # Remove old attribute
        if CONF in f and "dir" in f[CONF].attrs:
            f[CONF].attrs.modify("dir", None)
        f.flush()

    async def _shard_items_into_directory(self, items, f, dt):
        SHARD_SIZE = 100_000
        # Sharded directory: /directory/shard_{i}
        num_shards = (len(items) + SHARD_SIZE - 1) // SHARD_SIZE
        dir_group = f.require_group("dir")
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

    def _init_directory(self) -> None:
        """
        Initialize the extendible hash directory structure.
        Load existing directory from HDF5 or create a new one.
        """
        with self.hdf as f:
            config = f.require_group("/config")
            
            # Load or initialize global_depth
            self.global_depth = config.attrs.get("global_depth", 1)
            self.num_buckets = config.attrs.get("num_buckets", 0)
            
            # Initialize directory with 2^global_depth entries
            directory_size = 2 ** self.global_depth
            
            if "directory" in f:
                # Load existing directory dataset
                directory_ds = f["directory"]
                self._directory_mode = "ds"
                self._directory_dataset = directory_ds
                self.bucket_pointers = []
                for i in range(min(directory_size, directory_ds.shape[0])):
                    entry = directory_ds[i]
                    self.bucket_pointers.append({
                        "bucket_id": int(entry["bucket_id"]),
                        "hdf5_ref": int(entry["hdf5_ref"]) if "hdf5_ref" in entry.dtype.names else 0
                    })
                # Ensure directory is the right size
                while len(self.bucket_pointers) < directory_size:
                    self.bucket_pointers.append({"bucket_id": 0, "hdf5_ref": 0})
            else:
                # Create new directory
                self.bucket_pointers = [{"bucket_id": 0, "hdf5_ref": 0} for _ in range(directory_size)]
                
                # Create initial bucket if none exist
                if self.num_buckets == 0:
                    self._create_initial_bucket()

    def _create_initial_bucket(self) -> None:
        """Create the initial bucket and update directory pointers."""
        bucket_id = self.num_buckets
        bucket_name = f"bucket_{bucket_id:04d}"
        
        # Create bucket dataset in HDF5
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                # Use hash_entry_dtype from specs
                hash_entry_dtype = np.dtype([
                    ("key_high", "<u8"),
                    ("key_low", "<u8"),
                    ("slots", "<u8", (2,)),  # Simplified to 2 slots as per Spec 2
                    ("checksum", "<u8", (2,))  # 2 x uint64 for checksum
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
                bucket_ds.attrs["last_compacted"] = 0
        
        # Update directory pointers to point to this bucket
        for i in range(len(self.bucket_pointers)):
            self.bucket_pointers[i] = {"bucket_id": bucket_id, "hdf5_ref": 0}
        
        self.num_buckets = 1
        self._save_directory_metadata()

    def _save_directory_metadata(self) -> None:
        """Save global_depth, num_buckets to HDF5 config."""
        with self.hdf as f:
            config = f["/config"]
            config.attrs["global_depth"] = self.global_depth
            config.attrs["num_buckets"] = self.num_buckets
            f.flush()

    def _find_bucket_id(self, key: E) -> int:
        """
        Find bucket ID for a key using extendible hashing (Spec 3).
        Extract top global_depth bits from key.high as directory index.
        """
        directory_index = key.high >> (64 - self.global_depth)
        directory_index = directory_index % len(self.bucket_pointers)
        return self.bucket_pointers[directory_index]["bucket_id"]

    def _get_bucket_local_depth(self, bucket_id: int) -> int:
        """Get the local_depth of a bucket from its HDF5 attributes."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                return int(buckets_group[bucket_name].attrs.get("local_depth", 1))
        return 1

    def _should_split_bucket(self, bucket_id: int) -> bool:
        """Check if a bucket should be split based on entry count."""
        bucket_name = f"bucket_{bucket_id:04d}"
        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name in buckets_group:
                bucket_ds = buckets_group[bucket_name]
                return bucket_ds.shape[0] >= self.SPLIT_THRESHOLD
        return False

    async def valueset_exists(self, key: E) -> bool:
        """
        Check if a value set exists for a key (including spilled sets).
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return False
        bucket = self.buckets.get(f"bucket_{bucket_id}")
        if bucket is None:
            return False
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                if bucket[i]["state_mask"] == 0:
                    spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                    values_group = self.hdf.file["/values/sp"]
                    return spill_ds_name in values_group
                return any(slot != 0 for slot in bucket[i]["slots"])
        return False

    def get_tombstone_count(self, key: E) -> int:
        """
        Count tombstones (zeros) in the spill dataset for a key.
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return 0
        bucket = self.buckets.get(f"bucket_{bucket_id}")
        if bucket is None:
            return 0
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                if bucket[i]["state_mask"] == 0:
                    spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                    values_group = self.hdf.file["/values/sp"]
                    if spill_ds_name in values_group:
                        ds = values_group[spill_ds_name]
                        return int((ds[:] == 0).sum())
                return 0
        return 0

    async def is_spilled(self, key: E) -> bool:
        """
        Check if a key has spilled values (i.e., if a spill dataset exists).
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return False
        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
        values_group = self.hdf.file["/values/sp"]
        return spill_ds_name in values_group

    async def demote_if_possible(self, key: E) -> None:
        """
        Move spilled values back to inline slots if possible (<=4 values).
        If the spill dataset has <= 4 values, moves them back to inline slots and deletes the spill.
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                if bucket[i]["state_mask"] == 0:
                    spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                    values_group = self.hdf.file["/values/sp"]
                    if spill_ds_name in values_group:
                        ds = values_group[spill_ds_name]
                        arr = ds[:]
                        arr = arr[arr != 0]
                        if len(arr) <= 4:
                            bucket[i]["slots"][:] = 0
                            for j, v in enumerate(arr):
                                bucket[i]["slots"][j] = v
                            del values_group[spill_ds_name]
                break

    async def gc(self) -> None:
        """
        Run background GC (orphan/tombstone cleanup).
        Compacts all buckets and removes empty spill datasets.
        """
        # Compact all buckets
        for key in list(self.dir.keys()):
            self.compact(key)
        # Remove empty spill datasets
        values_group = self.hdf.file["/values/sp"]
        for dsname in list(values_group.keys()):
            ds = values_group[dsname]
            if ds.shape[0] == 0 or all(ds[:] == 0):
                del values_group[dsname]
        self.hdf.file.flush()

    async def maintain(self) -> None:
        """
        Run background merge/sort/compaction for all buckets.
        Merges unsorted regions, merges underfilled buckets, and runs GC.
        """
        await self.background_bucket_maintenance()
        await self._maybe_merge_buckets()
        await self.run_gc_once()

    def apply(self, op: dict[str, Any]) -> None:
        """
        Apply a single WAL operation.
        Args:
            op: WAL operation dict.
        Supported op types:
        - 1: Insert
        - 2: Delete
        - 3: Transaction start (no-op)
        - 4: Transaction commit (no-op)
        """
        match op["op_type"]:
            case 1:
                key = E(op["key_high"], op["key_low"])
                value = E(op["value_high"], op["value_low"])
                self._wal_replay_insert(key, value)
            case 2:
                key = E(op["key_high"], op["key_low"])
                self._wal_replay_delete(key)
            case 3:
                self._wal_replay_txn_start()
            case 4:
                self._wal_replay_txn_commit()

    def _load_wal(self) -> None:
        """
        Load and apply the WAL.
        Reads the WAL file, replays the operations, and applies them to the tree.
        """
        if self.wal:
            self.wal.load()

    def _save_wal(self) -> None:
        """
        Save the current state to the WAL.
        Writes the current state of the tree to the WAL for recovery.
        """
        if self.wal:
            self.wal.save()

    def __enter__(self) -> CIDStore:
        """
        Enter the runtime context related to this object.
        Returns:
            CIDStore: The CIDStore instance.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Exit the runtime context related to this object.
        Closes the CIDStore instance.
        """
        asyncio.run(self.close())

    def __repr__(self) -> str:
        """
        Return a string representation of the CIDStore instance.
        Returns:
            str: String representation.
        """
        return f"<CIDStore buckets={len(self.buckets)} directory_size={len(self.dir)}>"

    def __len__(self) -> int:
        """
        Return the number of keys in the directory.
        Returns:
            int: Number of keys.
        """
        return len(self.dir)

    def debug_dump(self) -> str:
        """
        Debug: Dump the entire tree structure.
        Returns:
            str: String representation of the entire tree.
        """
        output = [f"CIDStore debug dump (buckets={len(self.buckets)})"]
        output.extend(self.debug_dump_bucket(bucket_id) for bucket_id in self.buckets)
        return "\n".join(output)

    def debug_dump_bucket(self, bucket_id: int) -> str:
        """
        Debug: Dump the contents of a bucket.
        Args:
            bucket_id: The ID of the bucket to dump.
        Returns:
            str: String representation of the bucket contents.
        """
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return "Bucket not found"
        sorted_count = int(bucket.attrs.get("sorted_count", 0))
        unsorted_count = bucket.shape[0] - sorted_count
        return (
            f"Bucket {bucket_id}:\n"
            f"  Sorted count: {sorted_count}\n"
            f"  Unsorted count: {unsorted_count}\n"
            f"  Entries:\n"
            + "\n".join(
                f"    {i}: key_high={bucket[i]['key_high']} key_low={bucket[i]['key_low']} "
                f"slots={bucket[i]['slots']} state_mask={bucket[i]['state_mask']} "
                f"version={bucket[i]['version']}"
                for i in range(bucket.shape[0])
            )
        )

    def _add_value_to_bucket_entry(
        self, bucket, entry_idx: int, key: E, value: E
    ) -> None:
        slots = bucket[entry_idx]["slots"]
        for j in range(4):
            if slots[j] == 0:
                slots[j] = int(value)
                bucket[entry_idx]["version"] += 1
                bucket.file.flush()
                return
        # All slots full, promote to spill
        bucket_id = self.dir[key]
        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
        values_group = self.file["/values/sp"]
        if spill_ds_name not in values_group:
            slot_values = [v for v in slots if v != 0]
            all_values = slot_values + [int(value)]
            ds = values_group.create_dataset(
                spill_ds_name,
                shape=(len(all_values),),
                maxshape=(None,),
                dtype="<u8",
                chunks=True,
            )
            ds[:] = all_values
            for j in range(4):
                slots[j] = 0
        else:
            ds = values_group[spill_ds_name]
            arr = ds[:]
            if int(value) not in arr:
                ds.resize((ds.shape[0] + 1,))
                ds[-1] = int(value)
        bucket[entry_idx]["state_mask"] = 0
