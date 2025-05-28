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
        self._wal_consumer_task: asyncio.Task | None = None

        # Initialize extendible hash directory
        self._init_directory()

    async def async_init(self) -> None:
        if self._wal_consumer_task is None:
            self._wal_consumer_task = asyncio.create_task(self.wal.consume_polling())

    async def wal_checkpoint(self):
        self.hdf.file.flush()
        self.wal.flush()

    def apply(self, op: dict[str, Any]) -> None:
        """Apply a WAL operation to the store."""
        pass  # WAL integration placeholder

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
        """Save global_depth, num_buckets to HDF5 config."""
        with self.hdf as f:
            config = f["/config"]
            if isinstance(config, h5py.Group):
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

    async def get(self, key: E) -> list[E]:
        """Get all values for a key."""
        logger.info(f"[CIDStore.get] Called with {key=}")
        assert isinstance(key, E)

        # Use extendible hashing to find bucket
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

        with self.hdf as f:
            buckets_group_obj = f["/buckets"]
            if isinstance(buckets_group_obj, h5py.Group):
                buckets_group = buckets_group_obj
                if bucket_name not in buckets_group:
                    logger.info(f"[CIDStore.get] No bucket found for {key=}")
                    return []

                bucket_obj = buckets_group[bucket_name]
                if isinstance(bucket_obj, h5py.Dataset):
                    bucket = bucket_obj

                    # Search bucket for key
                    for i in range(bucket.shape[0]):
                        entry = bucket[i]
                        if (
                            entry["key_high"] == key.high
                            and entry["key_low"] == key.low
                        ):
                            # Check if it's spilled to external ValueSet
                            if len(entry["slots"]) > 0 and entry["slots"][0] != 0:
                                # Inline values
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
                                            return [
                                                E(v) for v in spill_ds_obj[:] if v != 0
                                            ]
                                return []

        return []

    async def get_entry(self, key: E) -> dict[str, Any] | None:
        """Get bucket entry for a key (for debugging and testing)."""
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

                    # Search bucket for key
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

    async def insert(self, key: E, value: E) -> None:
        """Insert a key-value pair."""
        logger.info(f"[CIDStore.insert] Called with {key=}, {value=}")
        assert isinstance(key, E)
        assert isinstance(value, E)

        # Use extendible hashing to find bucket
        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

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

                # Search for existing entry
                found_entry_idx = None
                for i in range(bucket_ds.shape[0]):
                    entry = bucket_ds[i]
                    if entry["key_high"] == key.high and entry["key_low"] == key.low:
                        found_entry_idx = i
                        break

                if found_entry_idx is not None:
                    # Update existing entry - add value to first empty slot
                    entry = bucket_ds[found_entry_idx]
                    slots = list(entry["slots"])

                    # Find first empty slot (0) or check if value already exists
                    if int(value) not in slots:
                        for j in range(len(slots)):
                            if slots[j] == 0:
                                slots[j] = int(value)
                                break

                        # Update the entry
                        new_entry = (
                            entry["key_high"],
                            entry["key_low"],
                            tuple(slots),
                            entry["checksum"],
                        )
                        bucket_ds[found_entry_idx] = new_entry
                else:
                    # Create new entry
                    new_entry = (
                        key.high,
                        key.low,
                        (int(value), 0),  # First slot gets the value, second is empty
                        (0, 0),  # Zero checksum for now
                    )

                    # Resize and add entry
                    old_size = bucket_ds.shape[0]
                    bucket_ds.resize((old_size + 1,))
                    bucket_ds[old_size] = new_entry

                    # Update entry count
                    bucket_ds.attrs["entry_count"] = old_size + 1

                # Check if bucket needs splitting
                if bucket_ds.shape[0] >= self.SPLIT_THRESHOLD:
                    await self._maybe_split_bucket(bucket_id)

    async def _maybe_split_bucket(self, bucket_id: int) -> None:
        """Split bucket if it's over threshold and has sufficient local depth."""
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
                        new_bucket[:] = new_bucket_entries

                    # Update entry counts
                    old_bucket.attrs["entry_count"] = len(old_bucket_entries)
                    new_bucket.attrs["entry_count"] = len(new_bucket_entries)

                    # Update directory pointers
                    self._update_directory_pointers_after_split(
                        old_bucket_id, new_bucket_id, local_depth
                    )

    def _update_directory_pointers_after_split(
        self, old_bucket_id: int, new_bucket_id: int, local_depth: int
    ) -> None:
        """Update directory pointers after bucket split."""
        for i, pointer in enumerate(self.bucket_pointers):
            if (
                pointer["bucket_id"] == old_bucket_id
            ):  # Check if this directory entry should point to new bucket
                directory_bit_pos = self.global_depth - local_depth
                if directory_bit_pos >= 0:
                    bit_value = (i >> directory_bit_pos) & 1

                    if bit_value == 1:
                        pointer["bucket_id"] = new_bucket_id

    async def delete_value(self, key: E, value: E) -> None:
        """Delete a specific value from a key."""
        logger.info(f"[CIDStore.delete_value] Called with {key=}, {value=}")
        assert isinstance(key, E)
        assert isinstance(value, E)

        bucket_id = self._find_bucket_id(key)
        bucket_name = f"bucket_{bucket_id:04d}"

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

    # Context management
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._wal_consumer_task and not self._wal_consumer_task.done():
            try:
                self._wal_consumer_task.cancel()
            except Exception:
                pass

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
