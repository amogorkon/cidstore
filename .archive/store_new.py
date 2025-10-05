"""store.py - Main CIDStore class, directory, bucket, and ValueSet logic (Spec 2)"""

from __future__ import annotations

import asyncio
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
            directory_size = 2**self.global_depth

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
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                # Use hash_entry_dtype from specs
                hash_entry_dtype = np.dtype([
                    ("key_high", "<u8"),
                    ("key_low", "<u8"),
                    ("slots", "<u8", (2,)),  # 2 slots
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

    def _split_bucket(self, bucket_id: int) -> None:
        """
        Split a bucket using extendible hashing algorithm (Spec 6).
        """
        bucket_name = f"bucket_{bucket_id:04d}"
        local_depth = self._get_bucket_local_depth(bucket_id)

        with self.hdf as f:
            buckets_group = f["/buckets"]
            old_bucket = buckets_group[bucket_name]

            # Check if we need to double the directory
            if local_depth >= self.global_depth:
                self._double_directory()

            # Create new bucket
            new_bucket_id = self.num_buckets
            new_bucket_name = f"bucket_{new_bucket_id:04d}"

            # Copy structure from old bucket
            new_bucket = buckets_group.create_dataset(
                new_bucket_name,
                shape=(0,),
                maxshape=(None,),
                dtype=old_bucket.dtype,
                chunks=True,
                track_times=False,
            )

            # Set new bucket attributes
            new_local_depth = local_depth + 1
            new_bucket.attrs["local_depth"] = new_local_depth
            new_bucket.attrs["sorted_count"] = 0
            new_bucket.attrs["entry_count"] = 0
            new_bucket.attrs["last_compacted"] = 0

            # Update old bucket's local depth
            old_bucket.attrs["local_depth"] = new_local_depth

            # Redistribute entries between old and new bucket
            self._redistribute_bucket_entries(bucket_id, new_bucket_id, new_local_depth)

            # Update directory pointers
            self._update_directory_pointers_after_split(
                bucket_id, new_bucket_id, new_local_depth
            )

            self.num_buckets += 1
            self._save_directory_metadata()

    def _double_directory(self) -> None:
        """Double the directory size and increment global_depth."""
        old_pointers = self.bucket_pointers[:]
        self.global_depth += 1

        # Create new directory with double the size
        new_size = 2**self.global_depth
        self.bucket_pointers = []

        # Each old entry maps to two new entries
        for old_entry in old_pointers:
            self.bucket_pointers.append(old_entry.copy())
            self.bucket_pointers.append(old_entry.copy())

    def _redistribute_bucket_entries(
        self, old_bucket_id: int, new_bucket_id: int, local_depth: int
    ) -> None:
        """Redistribute entries between old and new bucket based on key bits."""
        old_bucket_name = f"bucket_{old_bucket_id:04d}"
        new_bucket_name = f"bucket_{new_bucket_id:04d}"

        with self.hdf as f:
            buckets_group = f["/buckets"]
            old_bucket = buckets_group[old_bucket_name]
            new_bucket = buckets_group[new_bucket_name]

            # Read all entries from old bucket
            old_entries = old_bucket[:]

            # Split entries based on the local_depth-th bit
            bit_mask = 1 << (64 - local_depth)
            old_bucket_entries = []
            new_bucket_entries = []

            for entry in old_entries:
                key_high = entry["key_high"]
                if key_high & bit_mask:
                    new_bucket_entries.append(entry)
                else:
                    old_bucket_entries.append(entry)

            # Resize and update buckets
            if old_bucket_entries:
                old_bucket.resize((len(old_bucket_entries),))
                old_bucket[:] = old_bucket_entries
                old_bucket.attrs["entry_count"] = len(old_bucket_entries)
            else:
                old_bucket.resize((0,))
                old_bucket.attrs["entry_count"] = 0

            if new_bucket_entries:
                new_bucket.resize((len(new_bucket_entries),))
                new_bucket[:] = new_bucket_entries
                new_bucket.attrs["entry_count"] = len(new_bucket_entries)

    def _update_directory_pointers_after_split(
        self, old_bucket_id: int, new_bucket_id: int, local_depth: int
    ) -> None:
        """Update directory pointers after bucket split."""
        # Update directory entries that should point to the new bucket
        bit_mask = 1 << (64 - local_depth)

        for i in range(len(self.bucket_pointers)):
            if i & bit_mask:
                self.bucket_pointers[i]["bucket_id"] = new_bucket_id
            # else: keep pointing to old_bucket_id

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
            return
        await self.wal.log_insert(key.high, key.low, value.high, value.low)
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
                        logger.info(
                            f"[CIDStore.get] Returning inline values for {key=}"
                        )
                        return (E(slot) for slot in entry["slots"] if slot != 0)
                    else:
                        # Check for spilled values
                        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                        values_group = f.get("/values/sp")
                        if values_group and spill_ds_name in values_group:
                            logger.info(
                                f"[CIDStore.get] Returning spill values for {key=}"
                            )
                            return (
                                E(v) for v in values_group[spill_ds_name][:] if v != 0
                            )
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

    def _wal_replay_insert(self, key: E, value: E) -> None:
        """Apply an insert operation from WAL replay."""
        # Use extendible hashing to find bucket
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                # Create bucket if it doesn't exist
                self._create_bucket(bucket_id)

            bucket = buckets_group[bucket_name]

            # Check if key already exists
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if entry["key_high"] == key.high and entry["key_low"] == key.low:
                    # Key exists, add value to it
                    self._add_value_to_bucket_entry(bucket, i, key, value)
                    return

            # Key doesn't exist, create new entry
            new_entry = np.zeros(1, dtype=bucket.dtype)
            new_entry[0]["key_high"] = key.high
            new_entry[0]["key_low"] = key.low
            new_entry[0]["slots"][0] = int(value)
            new_entry[0]["checksum"] = [0, 0]  # Placeholder checksum

            # Resize bucket and add entry
            bucket.resize((bucket.shape[0] + 1,))
            bucket[-1] = new_entry[0]
            bucket.attrs["entry_count"] = bucket.shape[0]

            # Check if bucket needs splitting
            if self._should_split_bucket(bucket_id):
                self._split_bucket(bucket_id)

    def _wal_replay_delete(self, key: E) -> None:
        """Apply a delete operation from WAL replay."""
        # Use extendible hashing to find bucket
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                return  # Nothing to delete

            bucket = buckets_group[bucket_name]

            # Find and remove entry
            entries_to_keep = []
            for i in range(bucket.shape[0]):
                entry = bucket[i]
                if not (entry["key_high"] == key.high and entry["key_low"] == key.low):
                    entries_to_keep.append(entry)

            # Update bucket with remaining entries
            if len(entries_to_keep) != bucket.shape[0]:
                bucket.resize((len(entries_to_keep),))
                if entries_to_keep:
                    bucket[:] = entries_to_keep
                bucket.attrs["entry_count"] = len(entries_to_keep)

                # Clean up spilled values if any
                spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                values_group = f.get("/values/sp")
                if values_group and spill_ds_name in values_group:
                    del values_group[spill_ds_name]

    def _wal_replay_txn_start(self) -> None:
        """Handle transaction start (no-op for now)."""
        pass

    def _wal_replay_txn_commit(self) -> None:
        """Handle transaction commit (no-op for now)."""
        pass

    def _create_bucket(self, bucket_id: int) -> None:
        """Create a bucket with the given ID."""
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group = f["/buckets"]
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
                bucket_ds.attrs["last_compacted"] = 0

    def _add_value_to_bucket_entry(
        self, bucket, entry_idx: int, key: E, value: E
    ) -> None:
        """Add a value to an existing bucket entry."""
        slots = bucket[entry_idx]["slots"]
        for j in range(len(slots)):
            if slots[j] == 0:
                slots[j] = int(value)
                bucket.file.flush()
                return

        # All slots full, promote to spill
        bucket_id = self._find_bucket_id(key)
        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"

        with self.hdf as f:
            values_group = f.require_group("/values/sp")
            if spill_ds_name not in values_group:
                # Create spill dataset with current slot values plus new value
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
                # Clear slots since values are now spilled
                for j in range(len(slots)):
                    slots[j] = 0
            else:
                # Add to existing spill dataset
                ds = values_group[spill_ds_name]
                arr = ds[:]
                if int(value) not in arr:
                    ds.resize((ds.shape[0] + 1,))
                    ds[-1] = int(value)

    async def close(self) -> None:
        """Close the CIDStore and clean up resources."""
        if hasattr(self, "_wal_consumer_task"):
            self._wal_consumer_task.cancel()
            try:
                await self._wal_consumer_task
            except asyncio.CancelledError:
                pass
        self.hdf.close()
        self.wal.close()

    def __enter__(self) -> CIDStore:
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the runtime context related to this object."""
        asyncio.run(self.close())

    def __repr__(self) -> str:
        """Return a string representation of the CIDStore instance."""
        return f"<CIDStore num_buckets={self.num_buckets} global_depth={self.global_depth}>"

    def __len__(self) -> int:
        """Return the number of directory entries."""
        return len(self.bucket_pointers)

    def debug_dump(self) -> str:
        """Debug: Dump the entire directory and bucket structure."""
        output = [
            f"CIDStore debug dump (global_depth={self.global_depth}, num_buckets={self.num_buckets})"
        ]
        output.append(f"Directory size: {len(self.bucket_pointers)}")

        for i, pointer in enumerate(self.bucket_pointers):
            output.append(f"  Dir[{i:04d}] -> Bucket {pointer['bucket_id']}")

        for bucket_id in range(self.num_buckets):
            output.append(self.debug_dump_bucket(bucket_id))

        return "\n".join(output)

    def debug_dump_bucket(self, bucket_id: int) -> str:
        """Debug: Dump the contents of a bucket."""
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group = f["/buckets"]
            if bucket_name not in buckets_group:
                return f"Bucket {bucket_id}: Not found"

            bucket = buckets_group[bucket_name]
            local_depth = bucket.attrs.get("local_depth", 1)
            entry_count = bucket.attrs.get("entry_count", bucket.shape[0])

            output = [
                f"Bucket {bucket_id}:",
                f"  Local depth: {local_depth}",
                f"  Entry count: {entry_count}",
                "  Entries:",
            ]

            for i in range(bucket.shape[0]):
                entry = bucket[i]
                output.append(
                    f"    {i}: key_high={entry['key_high']:016x} key_low={entry['key_low']:016x} "
                    f"slots={list(entry['slots'])}"
                )

            return "\n".join(output)
