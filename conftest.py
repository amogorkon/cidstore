import h5py
import pytest

from cidtree.storage import StorageManager
from cidtree.tree import BPlusTree


@pytest.fixture
def in_memory_hdf5():
    # Use the HDF5 core driver for in-memory operation
    f = h5py.File("dummy", "w", driver="core", backing_store=False)
    yield f
    f.close()


@pytest.fixture
def tree(monkeypatch, in_memory_hdf5):
    class InMemoryStorageManager(StorageManager):
        def __init__(self):
            # Provide a dummy path to satisfy the base class
            super().__init__(path="dummy")
        def open(self, mode="a", swmr=False):
            self.file = in_memory_hdf5
            return self.file

    storage = InMemoryStorageManager()
    return BPlusTree(storage)
