"""storage.py - HDF5 storage manager and raw hooks for CIDTree"""

import io
from pathlib import Path
from typing import Any

import h5py

from .config import CONFIG_GROUP, NODES_GROUP, VALUES_GROUP
from .keys import E

HDF5_NOT_OPEN_MSG = "HDF5 file is not open."


class Storage:
    def __init__(self, path: str | Path | io.BytesIO | None) -> None:
        """
        Initialize the StorageManager with the given file path or in-memory buffer.
        Accepts a string path, pathlib.Path, or an io.BytesIO object for in-memory operation.
        """
        self.path: str | Path | io.BytesIO | None = path
        self.file: h5py.File | None = None
        self.open("a")

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

    def rollback_to_version(self, version: int) -> None:
        """
        Override in a subclass or bind to a tree method if needed.
        Rollback to a previous version. This is a stub for WAL compatibility.
        """
        assert isinstance(version, int), "version must be int"

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

    # ----------------------------------------------------------------
    # The following hooks are invoked by WAL._apply_transaction to
    # perform raw insert/delete. They must be wired up to your
    # BPlusTree implementation (e.g. by assigning these methods to
    # call through to tree.insert/tree.delete).
    # ----------------------------------------------------------------

    def apply_insert(self, key: E, value: E) -> None:
        """
        Apply a low-level insert during WAL replay.
        By default, this is a stub; bind it to your BPlusTree.insert().
        """
        assert isinstance(key, E), "key must be E"
        assert isinstance(value, E), "value must be E"
        raise NotImplementedError(
            "StorageManager.apply_insert() not bound; "
            "bind to BPlusTree.insert for WAL replay."
        )

    def apply_delete(self, key: E) -> None:
        """
        Apply a low-level delete during WAL replay.
        By default, this is a stub; bind it to your BPlusTree.delete().
        """
        assert isinstance(key, E), "key must be E"
        raise NotImplementedError(
            "StorageManager.apply_delete() not bound; "
            "bind to BPlusTree.delete for WAL replay."
        )

    @property
    def buckets(self):
        """
        Return the buckets group from the HDF5 file as a dict-like object.
        Ensures the file is open if it was closed.
        """
        if self.file is None:
            self.open("a")
        return self.file["/buckets"]
