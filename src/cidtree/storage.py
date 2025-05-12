# storage.py

import h5py
from typing import Optional
from .config import CONFIG_GROUP, NODES_GROUP, VALUES_GROUP
from .keys import E


class StorageManager:
    def rollback_to_version(self, version: int) -> None:
        """
        Rollback to a previous version. This is a stub for WAL compatibility.
        Override in a subclass or bind to a tree method if needed.
        """
        # No-op by default
        pass
    """
    Manages the HDF5 file, ensures required groups exist, and provides
    basic lifecycle (open/close/flush) plus stubs for raw insert/delete
    hooks used by WAL.replay().
    """

    def __init__(self, path: str):
        self.path: str = path
        self.file: Optional[h5py.File] = None

    def open(self, mode: str = "a", swmr: bool = False) -> h5py.File:
        """
        Open the HDF5 file (create if needed), enable SWMR if desired,
        and ensure the core groups (config, nodes, values) exist.
        """
        try:
            if swmr and mode == "r":
                # Reader in SWMR mode
                self.file = h5py.File(self.path, mode, libver="latest", swmr=True)
            else:
                # Default: support reads/writes
                self.file = h5py.File(self.path, mode, libver="latest")
            self._ensure_core_groups()
            return self.file
        except (OSError, RuntimeError) as e:
            raise RuntimeError(f"Failed to open HDF5 file '{self.path}': {e}") from e

    def _ensure_core_groups(self) -> None:
        """
        Ensure that the CONFIG, NODES, and VALUES groups exist.
        Note: WAL_DATASET is managed by the WAL class itself.
        """
        if self.file is None:
            raise RuntimeError("HDF5 file is not open.")
        for grp in (CONFIG_GROUP, NODES_GROUP, VALUES_GROUP):
            self.file.require_group(grp)

    def flush(self) -> None:
        """
        Flush all in-memory buffers to disk. Useful after batched
        operations or before handing off to readers in SWMR mode.
        """
        if self.file:
            self.file.flush()

    def close(self) -> None:
        """
        Close the HDF5 file, flushing first. Safe to call multiple times.
        """
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
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    # ----------------------------------------------------------------
    # The following hooks are invoked by WAL._apply_transaction to
    # perform raw insert/delete. They must be wired up to your
    # BPlusTree implementation (e.g. by assigning these methods to
    # call through to tree.insert/tree.delete).
    # ----------------------------------------------------------------

    def apply_insert(self, key: "E", value: "E") -> None:
        """
        Apply a low-level insert during WAL replay.
        By default, this is a stub; bind it to your BPlusTree.insert().
        """
        raise NotImplementedError(
            "StorageManager.apply_insert() not bound; "
            "bind to BPlusTree.insert for WAL replay."
        )

    def apply_delete(self, key: "E") -> None:
        """
        Apply a low-level delete during WAL replay.
        By default, this is a stub; bind it to your BPlusTree.delete().
        """
        raise NotImplementedError(
            "StorageManager.apply_delete() not bound; "
            "bind to BPlusTree.delete for WAL replay."
        )
