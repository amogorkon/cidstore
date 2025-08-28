
from __future__ import annotations
import asyncio
import io
from pathlib import Path
from time import time
import numpy as np
from h5py import Dataset, File, Group, version
from zvic import constrain_this_module
constrain_this_module()
"""storage.py - HDF5 storage manager and raw hooks for CIDTree"""

import asyncio
import io
from pathlib import Path
from time import time

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

logger = get_logger(__name__)


class Storage:
    def __init__(self, path: str | Path | io.BytesIO | None) -> None:
        """
        Initialize the StorageManager with the given file path or in-memory buffer.
        Accepts a string path, pathlib.Path, or an io.BytesIO object for in-memory operation.
        """
        self.path: Path | io.BytesIO
        match path:
            case None:
                self.path = io.BytesIO()
            case str() | Path() | io.BytesIO():
                self.path = Path(path) if isinstance(path, str) else path
            case _:
                raise TypeError("path must be str, Path, or io.BytesIO or None")

        self.file: File = self.open()
        self._init_hdf5_layout()
        self._init_root()

        mode = getattr(self.file, "mode", "unknown")
        swmr = getattr(self.file, "swmr_mode", False)
        # Ensure required groups exist; allow additional metadata/groups to be present
        keys = set(self.file.keys())
        required = {"buckets", "config", "nodes", "values", "root"}
        missing = required - keys
        assert not missing, f"HDF5 file layout is invalid, missing groups: {missing}"
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
            # When given a path, open in 'a' (append) mode to avoid truncation of existing files.
            mode = "a" if not isinstance(self.path, io.BytesIO) else "w"
            self.file = File(self.path, mode, libver="latest")
            self._ensure_core_groups()
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
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        try:
            self.flush()
            self.file.close()
        except (OSError, RuntimeError) as e:
            raise RuntimeError(f"Failed to close HDF5 file: {e}") from e

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

        # Delegate to synchronous delete logic if necessary
        # For now, mirror apply_insert behavior by calling delete logic synchronously
        def _do_delete():
            # Find bucket and mark deletions as zeros (tombstones)
            bucket_group = self.file[BUCKS]
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
                    break

        await asyncio.to_thread(_do_delete)

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
        bucket_group = self.file[BUCKS]
        assert assumption(bucket_group, Group)
        bucket_ds = bucket_group[bucket_name]
        assert assumption(bucket_ds, Dataset)

        total = bucket_ds.shape[0]
        entry_idx = None

        for i in range(total):
            entry = bucket_ds[i]
            if entry["key_high"] == op.k_high and entry["key_low"] == op.k_low:
                entry_idx = i
                break

        if entry_idx is not None:
            entry = bucket_ds[entry_idx]
        else:
            entry = new_entry(op.k_high, op.k_low)

        assert entry.dtype == HASH_ENTRY_DTYPE, "Entry not HASH_ENTRY_DTYPE"

        slot0, slot1 = entry["slots"]

        match any(slot0), any(slot1):
            case (False, False):  # Inline mode, both slots empty, new entry
                entry["slots"][0]["high"] = op.v_high
                entry["slots"][0]["low"] = op.v_low
                bucket_ds.resize((total + 1,))
                bucket_ds[total] = entry
                bucket_ds.attrs["entry_count"] = total + 1

            case (True, False):  # Inline mode, first slot used, put in second slot
                entry["slots"][1]["high"] = op.v_high
                entry["slots"][1]["low"] = op.v_low
                bucket_ds[entry_idx] = entry

            case (True, True):  # Inline mode, both slots used, must promote to spill
                bucket_id = int(bucket_name.split("_")[-1])
                assert entry_idx is not None, "Entry index must be set"
                # Re-fetch the entry from the dataset to ensure it is up-to-date
                entry = bucket_ds[entry_idx]
                await self.apply_insert_promote(
                    bucket_ds,
                    entry_idx,
                    entry,
                    bucket_id,
                    op.k_high,
                    op.k_low,
                    op.v_high,
                    op.v_low,
                )

            case (False, True):
                await self.apply_insert_spill(entry, op.v_high, op.v_low)

        assert assumption(bucket_ds, Dataset)
        return bucket_ds  # type: ignore

    async def apply_insert_promote(
        self,
        bucket_ds: Dataset,
        entry_idx: int,
        entry,
        bucket_id: int,
        key_high: int,
        key_low: int,
        value_high: int,
        value_low: int,
    ) -> None:
        """Promote inline entry to ValueSet (spill) mode when both slots are full."""
        # Directly fetch the slot values from the dataset to ensure correctness
        slots = bucket_ds[entry_idx]["slots"]

        def slot_to_int(slot):
            return (int(slot["high"]) << 64) | int(slot["low"])

        value1 = slot_to_int(slots[0])
        value2 = slot_to_int(slots[1])
        new_value = (value_high << 64) | value_low
        values = [value1, value2, new_value]

        sp_group = self._get_valueset_group()
        ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)

        if ds_name in sp_group:
            del sp_group[ds_name]

        spill_dtype = np.dtype([("high", "<u8"), ("low", "<u8")])

        ds = sp_group.create_dataset(
            ds_name, shape=(len(values),), maxshape=(None,), dtype=spill_dtype
        )
        encoded = [(int(v) >> 64, int(v) & 0xFFFFFFFFFFFFFFFF) for v in values]

        ds[:] = encoded

        # Store the dataset name as the spill pointer (as high/low for compatibility)
        # Use: slot1["high"] = bucket_id, slot1["low"] = key_high
        # Always use entry["key_low"] for dataset name construction
        entry["slots"][0]["high"] = 0
        entry["slots"][0]["low"] = 0
        entry["slots"][1]["high"] = bucket_id
        entry["slots"][1]["low"] = key_high

        # Ensure entry["key_low"] is preserved and not modified
        # (no change needed if not modified elsewhere)
        bucket_ds[entry_idx] = entry

    def _get_valueset_group(self):
        """Get or create the valueset group for spill datasets."""
        # Ensure /values is a group
        if "/values" in self.file and not isinstance(self.file["/values"], Group):
            del self.file["/values"]
        if "/values" not in self.file:
            self.file.create_group("/values")
        values_group = self.file["/values"]
        assert assumption(values_group, Group)
        # Ensure /values/sp is a group
        if "sp" in values_group and not isinstance(values_group["sp"], Group):
            del values_group["sp"]
        if "sp" not in values_group:
            values_group.create_group("sp")
        return values_group["sp"]

    def get_values(self, entry) -> list[E]:
        """
        Given a bucket entry, return all values for that entry (inline or spilled).
        Handles both inline and spill mode.
        """
        slots = entry["slots"]
        # Check for spill mode: slot0 is zero, slot1 is not
        if (
            int(slots[0]["high"]) == 0
            and int(slots[0]["low"]) == 0
            and (int(slots[1]["high"]) != 0 or int(slots[1]["low"]) != 0)
        ):
            # Spill mode: slot1 encodes the dataset name
            slot = slots[1]
            bucket_id = int(slot["high"])
            key_high = int(slot["low"])
            key_low = int(entry["key_low"])
            ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)
            sp_group = self.file["/values/sp"]
            if ds_name in sp_group:
                valueset_ds = sp_group[ds_name]

                try:
                    raw = valueset_ds[:]

                    import collections.abc

                    if not isinstance(raw, collections.abc.Iterable) or isinstance(
                        raw, (str, bytes)
                    ):
                        raw = [raw]

                    decoded = [
                        E.from_int((int(v["high"]) << 64) | int(v["low"]))
                        for v in raw
                        if int(v["high"]) != 0 or int(v["low"]) != 0
                    ]

                    return decoded
                except Exception:
                    raise
            return []
        # Inline mode: slots are structured arrays with 'high' and 'low'
        values = []
        for slot in slots:
            high = int(slot["high"])
            low = int(slot["low"])
            if high == 0 and low == 0:
                continue
            values.append(E.from_int((high << 64) | low))
        return values

    async def apply_insert_spill(self, entry, value_high: int, value_low: int) -> None:
        """
        Insert a new value into an existing spill (valueset) dataset for the given entry.
        """
        # The spill pointer is in slot1: high = bucket_id, low = key_high
        slot = entry["slots"][1]
        bucket_id = int(slot["high"])
        key_high = int(slot["low"])
        key_low = int(entry["key_low"])
        ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)
        sp_group = self._get_valueset_group()
        from h5py import Dataset, Group

        assert isinstance(sp_group, Group), f"sp_group is not a Group: {type(sp_group)}"
        if ds_name not in sp_group:
            raise RuntimeError(f"Spill dataset {ds_name} not found for entry {entry}")
        ds = sp_group[ds_name]
        assert isinstance(ds, Dataset), (
            f"Spill dataset {ds_name} is not a Dataset: {type(ds)}"
        )
        # Read current values
        current = ds[:]
        # Prepare new value as (high, low)
        new_value = (int(value_high), int(value_low))
        # Append new value
        new_len = len(current) + 1
        ds.resize((new_len,))
        ds[-1] = new_value


# STANDALONE FUNCTIONS


def new_entry(k_high, k_low):
    """Make a new entry to the bucket dataset and return the Dataset."""
    entry = np.zeros((), dtype=HASH_ENTRY_DTYPE)
    entry["key_high"] = k_high
    entry["key_low"] = k_low
    return entry


def _get_spill_ds_name(bucket_id: int, key_high: int, key_low: int) -> str:
    """Generate the spill dataset name for a key."""
    return f"sp_{bucket_id}_{key_high}_{key_low}"
