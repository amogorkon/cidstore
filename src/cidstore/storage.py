"""storage.py - HDF5 storage manager and raw hooks for CIDTree"""

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
            case str():
                self.path = Path(path)
            case Path():
                self.path = path
            case io.BytesIO():
                self.path = path
            case _:
                raise TypeError("path must be str, Path, or io.BytesIO or None")

        self.file: File = self.open()
        self._init_hdf5_layout()
        self._init_root()

        mode = getattr(self.file, "mode", "unknown")
        swmr = getattr(self.file, "swmr_mode", False)
        assert set(self.file.keys()) == {
            "buckets",
            "config",
            "nodes",
            "values",
            "root",
        }, "HDF5 file layout is invalid"
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
            self.file = File(self.path, "w", libver="latest")
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
        assumption(bucket_group, Group)
        bucket_ds = bucket_group[bucket_name]
        assumption(bucket_ds, Dataset)

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
            case False, False:  # Inline mode, both slots empty, new entry
                entry["slots"][0]["high"] = op.v_high
                entry["slots"][0]["low"] = op.v_low
                bucket_ds.resize((total + 1,))
                bucket_ds[total] = entry
                bucket_ds.attrs["entry_count"] = total + 1

            case True, False:  # Inline mode, first slot used, put in second slot
                entry["slots"][1]["high"] = op.v_high
                entry["slots"][1]["low"] = op.v_low
                bucket_ds[entry_idx] = entry

            case True, True:  # Inline mode, both slots used, must promote to spill
                bucket_id = int(
                    bucket_name.split("_")[1]
                )  # Parse bucket_id from bucket_name
                assert entry_idx is not None, "Entry index must be set"
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

            case False, True:
                await self.apply_insert_spill(entry, op.v_high, op.v_low)

        return bucket_ds

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
        slot0, slot1 = entry["slots"]

        value1 = (slot0["high"] << 64) | slot0["low"]
        value2 = (slot1["high"] << 64) | slot1["low"]
        new_value = (value_high << 64) | value_low

        values = [value1, value2, new_value]

        sp_group = self._get_valueset_group(self.file)
        ds_name = _get_spill_ds_name(bucket_id, key_high, key_low)

        if ds_name in sp_group:
            del sp_group[ds_name]

        ds = sp_group.create_dataset(
            ds_name, shape=(len(values),), maxshape=(None,), dtype="<u8"
        )
        ds[:] = [int(v) for v in values]

        spill_ptr = ds.id.ref
        entry["slots"][0]["high"] = 0
        entry["slots"][0]["low"] = 0
        entry["slots"][1]["high"] = int(spill_ptr) >> 64
        entry["slots"][1]["low"] = int(spill_ptr) & 0xFFFFFFFFFFFFFFFF

        bucket_ds[entry_idx] = entry

    def _get_valueset_group(self):
        """Get or create the valueset group for spill datasets."""
        if "/values" not in self.file:
            self.file.create_group("/values")
        values_group = self.file["/values"]
        assumption(values_group, Group)
        if "sp" not in values_group:
            values_group.create_group("sp")
        return values_group["sp"]

    def get_values(self, entry) -> list[E]:
        slot0, slot1 = entry["slots"]

        match any(slot0), any(slot1):
            case (True, False):
                # Inline Mode, 1 Entry:
                return [E.from_entry(entry)]
            case (True, True):
                # Inline Mode, 2 Entries:
                return [E.from_entry(s) for s in entry["slots"]]

            case (False, True):
                # Spill Mode, Spillpointer in slot1
                ref_int = (slot1["high"] << 64) | slot1["low"]
                ref_bytes = ref_int.to_bytes(16, "big")
                try:
                    valueset_ds = self.file.dereference(ref_bytes)
                    return [E(v) for v in valueset_ds[:] if v != 0]
                except Exception:
                    pass  # Could not dereference, or dataset missing

        return []


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
