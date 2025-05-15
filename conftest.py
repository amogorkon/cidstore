import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

import io
import pytest
import h5py
from cidtree.tree import CIDTree

@pytest.fixture
def tree():
    # Use an in-memory HDF5 file via BytesIO
    f = io.BytesIO()
    # Pre-create a valid HDF5 file structure
    with h5py.File(f, 'w') as hf:
        hf.create_group('buckets')
    f.seek(0)
    return CIDTree(f)

@pytest.fixture
def directory(tree):
    return tree

@pytest.fixture
def bucket(tree):
    return tree
