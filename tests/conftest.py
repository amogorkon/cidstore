import pytest

from cidtree.storage import StorageManager
from cidtree.tree import CIDTree


@pytest.fixture
def tree(tmp_path):
    # Use a real temporary file for h5py compatibility
    path = tmp_path / "testfile.h5"
    storage = StorageManager(str(path))
    return CIDTree(storage)
