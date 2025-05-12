# Test that the in-memory HDF5 file can be created and basic group/dataset operations work via StorageManager
from cidtree.storage import StorageManager


def test_storage_manager_in_memory(in_memory_hdf5):
    # Use our StorageManager with the in-memory HDF5 file
    storage = StorageManager()
    storage.file = in_memory_hdf5
    # Ensure required groups/datasets are created
    storage._ensure_groups()
    # Check that config, nodes, values groups exist
    from cidtree.config import NODES_GROUP, WAL_DATASET

    # Check that WAL dataset exists
    assert WAL_DATASET in storage.file
    # Write/read to a dataset in one of the groups
    nodes_group = storage.file[NODES_GROUP]
    test_ds = nodes_group.create_dataset("test", shape=(5,), dtype="i")
    test_ds[:] = [1, 2, 3, 4, 5]
    assert list(test_ds[:]) == [1, 2, 3, 4, 5]
    # File should still be open
    assert storage.file.id.valid
