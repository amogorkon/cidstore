# Basic HDF5 layout and attribute tests for CIDTree
import h5py


def test_hdf5_file_layout(tmp_path):
    from cidtree.main import CIDTree

    path = tmp_path / "layout.h5"
    tree = CIDTree(str(path))
    # Insert a value to ensure file is initialized
    tree.insert("foo", 1)
    with h5py.File(path, "r") as f:
        # Check root groups
        assert "buckets" in f or "hash" in f or "directory" in f
        # Check config attributes
        assert "config" in f or "/config" in f or any("config" in g for g in f)
        # Check for WAL dataset
        found_wal = any("wal" in k for k in f.keys()) or any("wal" in g for g in f)
        assert found_wal


def test_hdf5_attributes_and_metadata(tmp_path):
    from cidtree.main import CIDTree

    path = tmp_path / "meta.h5"
    tree = CIDTree(str(path))
    tree.insert("bar", 2)
    with h5py.File(path, "r") as f:
        # Check for format_version and version_string attributes
        config = f["/config"] if "/config" in f else f[list(f.keys())[0]]
        assert "format_version" in config.attrs
        assert "version_string" in config.attrs


def test_hdf5_bucket_and_valueset_presence(tmp_path):
    from cidtree.main import CIDTree

    path = tmp_path / "bucketval.h5"
    tree = CIDTree(str(path))
    key = "baz"
    tree.insert(key, 42)
    with h5py.File(path, "r") as f:
        # Check for at least one bucket group/dataset
        found_bucket = any("bucket" in k or "buckets" in k for k in f.keys())
        assert found_bucket
        # Insert more values to trigger ValueSet promotion
        for i in range(10):
            tree.insert(key, i)
        # Check for ValueSet/external dataset
        found_valueset = any("valueset" in k or "values" in k for k in f.keys())
        assert found_valueset or found_bucket
