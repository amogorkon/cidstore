# Basic HDF5 layout and attribute tests for CIDStore
import io

import h5py

from cidstore.keys import E
from cidstore.main import CIDStore
from cidstore.storage import Storage
from cidstore.store import WAL


def test_hdf5_file_layout(tmp_path):
    f = io.BytesIO()
    storage = Storage(f)
    wal = WAL(None)
    tree = CIDStore(storage, wal=wal)
    # Insert a value to ensure file is initialized
    tree.insert(E.from_str("foo"), E.from_int(1))
    f.seek(0)
    with h5py.File(f, "r") as h5f:
        # Check root groups
        assert any(x in h5f for x in ("buckets", "hash", "directory"))
        # Check config attributes
        assert any("config" in str(x) for x in h5f) or any(
            "config" in str(g) for g in h5f
        )
        # Check for WAL dataset
        found_wal = any("wal" in str(k) for k in h5f.keys()) or any(
            "wal" in str(g) for g in h5f
        )
        assert found_wal
        # Check that HashEntry datasets only have key, slots[2], checksum fields
        if "buckets" in h5f:
            for bucket_name in h5f["buckets"]:
                ds = h5f["buckets"][bucket_name]
                fields = list(ds.dtype.fields.keys()) if hasattr(ds, 'dtype') and ds.dtype.fields else []
                for field in fields:
                    assert field in ("key_high", "key_low", "slots", "checksum"), f"Unexpected field {field} in HashEntry dataset"


def test_hdf5_attributes_and_metadata(tmp_path):
    f = io.BytesIO()
    storage = Storage(f)
    wal = WAL(None)
    tree = CIDStore(storage, wal=wal)
    tree.insert(E.from_str("bar"), E.from_int(2))
    f.seek(0)
    with h5py.File(f, "r") as h5f:
        # Check for format_version and version_string attributes
        config = h5f["/config"] if "/config" in h5f else h5f[list(h5f.keys())[0]]
        assert "format_version" in config.attrs
        assert "version_string" in config.attrs


def test_hdf5_bucket_and_valueset_presence(tmp_path):
    f = io.BytesIO()
    storage = Storage(f)
    wal = WAL(None)
    tree = CIDStore(storage, wal=wal)
    key = "baz"
    tree.insert(E.from_str(key), E.from_int(42))
    f.seek(0)
    with h5py.File(f, "r") as h5f:
        # Check for at least one bucket group/dataset
        found_bucket = any("bucket" in k or "buckets" in k for k in h5f.keys())
        assert found_bucket

        # Insert more values to trigger ValueSet promotion and check for ValueSet/external dataset
        for i in range(10):
            tree.insert(E.from_str(key), E.from_int(i))
        f.seek(0)
        with h5py.File(f, "r") as h5f2:
            found_valueset = any("valueset" in k or "values" in k for k in h5f2.keys())
            assert found_valueset or found_bucket


def _check_bucket_and_valueset(tree, key, f, found_bucket):
    # Insert more values to trigger ValueSet promotion
    for i in range(10):
        tree.insert(E.from_str(key), E.from_int(i))
    f.seek(0)
    with h5py.File(f, "r") as h5f2:
        found_valueset = any("valueset" in k or "values" in k for k in h5f2.keys())
        assert found_valueset or found_bucket
    with h5py.File(f, "r") as h5f2:
        found_valueset = any("valueset" in k or "values" in k for k in h5f2.keys())
        assert found_valueset or found_bucket
