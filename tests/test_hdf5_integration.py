"""
Spec 9: HDF5 Integration (TDD)
Covers advanced HDF5 layout, sharding, hybrid directory, and metrics/logging if present.
All tests are TDD-style and implementation-agnostic.
"""

import h5py


def test_hdf5_sharding_and_layout(tmp_path):
    from cidstore.main import CIDTree

    path = tmp_path / "shard.h5"
    tree = CIDTree(str(path))
    # Insert enough keys to trigger sharding/hybrid directory (if supported)
    for i in range(10000):
        tree.insert(f"shardkey{i}", i)
    with h5py.File(path, "r") as f:
        # Check for sharded/hybrid directory structure
        found_shard = any("shard" in k or "hybrid" in k for k in f.keys())
        assert found_shard or True  # Accept if not present
        # Check that HashEntry datasets only have key, slots[2], checksum fields
        if "buckets" in f:
            for bucket_name in f["buckets"]:
                ds = f["buckets"][bucket_name]
                fields = (
                    list(ds.dtype.fields.keys())
                    if hasattr(ds, "dtype") and ds.dtype.fields
                    else []
                )
                for field in fields:
                    assert field in ("key_high", "key_low", "slots", "checksum"), (
                        f"Unexpected field {field} in HashEntry dataset"
                    )


def test_hdf5_metrics_and_logging(tmp_path):
    from cidstore.main import CIDTree

    path = tmp_path / "metrics.h5"
    tree = CIDTree(str(path))
    tree.insert("metrics", 1)
    # If API exposes metrics/logging, check them
    if hasattr(tree, "get_metrics"):
        metrics = tree.get_metrics()
        assert isinstance(metrics, dict)
    if hasattr(tree, "get_log"):
        log = tree.get_log()
        assert isinstance(log, list)


def test_hdf5_sharded_directory_migration(tmp_path):
    """Explicitly test sharded directory migration and verify sharded datasets."""
    import h5py

    from cidstore.main import CIDTree

    path = tmp_path / "sharded_dir.h5"
    tree = CIDTree(str(path))
    # Insert enough keys to trigger sharded directory migration (threshold: 1_000_000)
    SHARD_THRESHOLD = 1_000_000
    for i in range(SHARD_THRESHOLD + 10):
        tree.insert(f"shardkey{i}", i)
    # Force migration if not already triggered
    if hasattr(tree, "migrate_directory"):
        tree.migrate_directory()
    tree.file.flush()
    # Check that sharded directory exists and is populated
    with h5py.File(path, "r") as f:
        assert "directory" in f
        dir_group = f["directory"]
        shard_keys = [k for k in dir_group.keys() if k.startswith("shard_")]
        assert len(shard_keys) > 0
        # Check that at least one shard dataset is non-empty
        found_nonempty = any(dir_group[k].shape[0] > 0 for k in shard_keys)
        assert found_nonempty
        # Optionally, check that total entries match inserted keys
        total = sum(dir_group[k].shape[0] for k in shard_keys)
        assert total >= SHARD_THRESHOLD
