"""storage.py - HDF5 storage manager and raw hooks for CIDTree"""

import io
from pathlib import Path
from time import time
from typing import Any

import h5py

from .config import CONFIG_GROUP, NODES_GROUP, VALUES_GROUP
from .keys import E

HDF5_NOT_OPEN_MSG = "HDF5 file is not open."


ROOT = "/root"


class Storage:
    def __init__(self, path: str | Path | io.BytesIO | None) -> None:
        """
        Initialize the StorageManager with the given file path or in-memory buffer.
        Accepts a string path, pathlib.Path, or an io.BytesIO object for in-memory operation.
        """
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
        self.file: h5py.File = self.open()
        self._init_hdf5_layout()
        self._init_root()

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

        f = self.file
        cfg = f.require_group("/config")
        cfg.attrs.setdefault("format_version", 1)
        cfg.attrs.setdefault("created_by", "CIDStore")
        cfg.attrs.setdefault("swmr", True)
        cfg.attrs.setdefault("last_opened", int(time()))
        cfg.attrs.setdefault("last_modified", int(time()))
        if "dir" not in cfg.attrs:
            cfg.attrs["dir"] = "{}"
        f.attrs.setdefault("cidstore_version", "1.0")
        f.attrs.setdefault("hdf5_version", h5py.version.hdf5_version)
        f.attrs.setdefault("swmr", True)
        if "/values" not in f:
            f.create_group("/values")
        if "/values/sp" not in f:
            f["/values"].create_group("sp")
        if "/nodes" not in f:
            f.create_group("/nodes")
        f.flush()
        # self.deletion_log = DeletionLog(f)
        # self.gc_thread = BackgroundGC(self)
        # self.gc_thread.start()

    def _ensure_core_groups(self) -> None:
        """
        Ensure that the CONFIG, NODES, VALUES, and BUCKETS groups exist.
        Note: WAL_DATASET is managed by the WAL class itself.
        """
        assert self.file is not None, HDF5_NOT_OPEN_MSG
        if self.file is None:
            raise RuntimeError(HDF5_NOT_OPEN_MSG)
        for grp in (CONFIG_GROUP, NODES_GROUP, VALUES_GROUP, "/buckets"):
            g = self.file.require_group(grp)
            # Add required attributes to /config for test compatibility
            if grp == CONFIG_GROUP:
                if "format_version" not in g.attrs:
                    g.attrs["format_version"] = 1
                if "version_string" not in g.attrs:
                    g.attrs["version_string"] = "1.0.0"

    def __getitem__(self, item):
        """
        Allow subscript access to delegate to the underlying HDF5 file.
        """
        if self.file is None:
            self.open("a")
        return self.file[item]

    def __contains__(self, item):
        """
        Allow 'in' checks to delegate to the underlying HDF5 file.
        Always open in append mode to allow creation if needed.
        """
        if self.file is None:
            with self.open("a") as f:
                return item in f
        return item in self.file

    def create_group(self, name):
        """
        Allow create_group to delegate to the underlying HDF5 file.
        """
        if self.file is None:
            self.open()
        return self.file.create_group(name)

    def open(self, mode: str = "a", swmr: bool = False) -> h5py.File:
        """
        Open the HDF5 file (create if needed), enable SWMR if desired,
        and ensure the core groups (config, nodes, values) exist.
        Supports both file paths and in-memory io.BytesIO buffers.
        """
        assert isinstance(mode, str), "mode must be str"
        assert isinstance(swmr, bool), "swmr must be bool"
        try:
            path = self.path
            if isinstance(path, io.BytesIO):
                # In-memory HDF5 file
                self.file = h5py.File(path, mode)
            else:
                # Use Path for filesystem paths
                if isinstance(path, Path):
                    path = str(path)
                if swmr and mode == "r":
                    # Reader in SWMR mode
                    self.file = h5py.File(path, mode, libver="latest", swmr=True)
                else:
                    # Default: support reads/writes
                    self.file = h5py.File(path, mode, libver="latest")
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
        if self.file:
            self.file.flush()

    def close(self) -> None:
        """
        Close the HDF5 file, flushing first. Safe to call multiple times.
        """
        # file can be None, but if not, must be h5py.File
        assert self.file is None or hasattr(self.file, "flush"), (
            "file must be h5py.File or None"
        )
        if self.file is not None:
            try:
                self.flush()
                self.file.close()
            except (OSError, RuntimeError) as e:
                raise RuntimeError(f"Failed to close HDF5 file: {e}") from e
            finally:
                self.file = None

    # Context‐manager support
    def __enter__(self) -> h5py.File:
        """
        Enter the runtime context related to this object.
        Returns the opened HDF5 file.
        """
        # No assert needed, open() will check
        return self.open()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Exit the runtime context and close the HDF5 file.
        """
        # No assert needed
        self.close()

    @property
    def buckets(self):
        """
        Return the buckets group from the HDF5 file as a dict-like object.
        Ensures the file is open if it was closed.
        """
        if self.file is None:
            self.open("a")
        return self.file["/buckets"]

    def _apply_insert(self, key: E, value: E, skip_wal: bool = False) -> None:
        """
        Insert a key-value pair into the tree.
        Args:
            key: Key to insert (E).
            value: Value to insert (E).
        Handles multi-value logic (inline/spill, ECC) and WAL logging.
        Appends to the unsorted region of the bucket; background maintenance merges/sorts.
        """

        with self._writer_lock:
            if not skip_wal:
                # Only log to WAL if not already done in async insert
                self.wal.log_insert(key.high, key.low, value.high, value.low)

        if key not in self.dir:
            if not self.buckets:
                bucket_id: int = self._bucket_counter
                bucket_name = f"bucket_{bucket_id}"
                buckets_group = self.hdf.file["/buckets"]
                if bucket_name in buckets_group:
                    bucket_ds = buckets_group[bucket_name]
                else:
                    bucket_ds: Any = buckets_group.create_dataset(
                        bucket_name,
                        shape=(0,),
                        maxshape=(None,),
                        dtype=HASH_ENTRY_DTYPE,
                        chunks=True,
                        track_times=False,
                    )
                    bucket_ds.attrs["sorted_count"] = 0
                if bucket_name not in self.buckets:
                    self.buckets[bucket_name] = bucket_ds
                self._bucket_counter += 1
                self.dir[key] = bucket_id
            else:
                bucket_id: int = self._get_bucket_id(key)
                bucket_name = f"bucket_{bucket_id}"
                buckets_group = self.hdf.file["/buckets"]
                if bucket_name in buckets_group:
                    bucket_ds = buckets_group[bucket_name]
                else:
                    bucket_ds: Any = buckets_group.create_dataset(
                        bucket_name,
                        shape=(0,),
                        maxshape=(None,),
                        dtype=HASH_ENTRY_DTYPE,
                        chunks=True,
                        track_times=False,
                    )
                    bucket_ds.attrs["sorted_count"] = 0
                if bucket_name not in self.buckets:
                    self.buckets[bucket_name] = bucket_ds
                self.dir[key] = bucket_id
            self._save_directory()

        bucket_id = self.dir[key]
        bucket_name = f"bucket_{bucket_id}"
        bucket: Any = self.buckets[bucket_name]
        entry_idx: int | None = None
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                entry_idx = i
                break
        if entry_idx is not None:
            self._add_value_to_bucket_entry(bucket, entry_idx, key, value)
            return
        # No existing entry found, create a new one
        new_entry: Any = np.zeros(1, dtype=bucket.dtype)
        new_entry[0]["key_high"] = key.high
        new_entry[0]["key_low"] = key.low
        new_entry[0]["slots"][0] = int(value)
        new_entry[0]["state_mask"] = encode_state_mask(1)
        new_entry[0]["version"] = 1
        bucket.resize((bucket.shape[0] + 1,))
        bucket[-1] = new_entry[0]
        bucket.file.flush()

    def _apply_delete(self, key: E) -> None:
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return
        bucket_name = f"bucket_{bucket_id}"
        bucket = self.buckets.get(bucket_name)
        if bucket is None:
            return
        found_idx = next(
            (
                i
                for i in range(bucket.shape[0])
                if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low
            ),
            None,
        )
        if found_idx is None:
            return
        bucket[found_idx]["slots"][:] = 0
        bucket[found_idx]["state_mask"] = 0
        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
        values_group = self.hdf.file["/values/sp"]
        if spill_ds_name in values_group:
            del values_group[spill_ds_name]
        bucket[found_idx]["version"] += 1
        bucket.file.flush()
