def test_maintenance_manager_timeout():
    """Test that MaintenanceManager thread timeout works (should auto-stop after 1s)."""
    import time

    from cidstore.maintenance import MaintenanceConfig, MaintenanceManager

    config = MaintenanceConfig(thread_timeout=1.0)

    class MockStore:
        def __init__(self):
            self.metrics = None
            self.hdf = None

        def run_gc_once(self):
            pass

    mock_store = MockStore()
    manager = MaintenanceManager(mock_store, config)
    manager.start()
    time.sleep(2.0)  # Wait longer than the timeout
    # The manager should have stopped its threads
    assert not manager.maintenance_thread.is_alive(), (
        "MaintenanceManager thread did not stop after timeout"
    )
    # Cleanup (in case)
    if manager.maintenance_thread.is_alive():
        manager.maintenance_thread.stop()
        manager.maintenance_thread.join(timeout=1.0)


#!/usr/bin/env python3
"""Test unified maintenance system."""

import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))


def test_maintenance_manager():
    """Test the new MaintenanceManager system."""
    print("🔧 Testing unified MaintenanceManager...")

    # Import the classes
    from cidstore.maintenance import MaintenanceConfig, MaintenanceManager

    # Test configuration
    config = MaintenanceConfig(
        gc_interval=5,
        maintenance_interval=3,
        sort_threshold=4,
        merge_threshold=2,
        wal_analysis_interval=10,
        adaptive_maintenance_enabled=True,
    )

    print(
        f"✅ Created config: gc_interval={config.gc_interval}, maintenance_interval={config.maintenance_interval}"
    )

    # Create mock store
    class MockStore:
        def __init__(self):
            self.hdf = MockHDF()
            self.compact_calls = 0

        def compact(self, key):
            self.compact_calls += 1

        async def _maybe_merge_bucket(self, bucket_id, merge_threshold=8):
            pass

    class MockHDF:
        def __enter__(self):
            return {"buckets": MockBucketsGroup()}

        def __exit__(self, *args):
            pass

    class MockBucketsGroup:
        def __iter__(self):
            return iter(["bucket_0001", "bucket_0002"])

        def __getitem__(self, key):
            return MockBucket()

    class MockBucket:
        def __init__(self):
            self.attrs = {"sorted_count": 2, "entry_count": 3, "local_depth": 1}
            self.shape = (5,)

        def __getitem__(self, key):
            return [{"key_high": 1, "key_low": 2}] * 5

        def __setitem__(self, key, value):
            pass

        def flush(self):
            pass

    mock_store = MockStore()

    # Test MaintenanceManager
    manager = MaintenanceManager(mock_store, config)
    print("✅ Created MaintenanceManager")

    # Test deletion log
    manager.log_deletion(123, 456, 789, 101112)
    print("✅ Logged deletion")

    # Get stats before starting
    stats = manager.get_stats()
    print(f"✅ Got stats: {list(stats.keys())}")

    # Start maintenance (brief test)
    manager.start()
    print("✅ Started maintenance threads")

    # Let it run briefly
    time.sleep(0.5)

    # Stop maintenance
    manager.stop()
    print("✅ Stopped maintenance threads")

    # Get final stats
    final_stats = manager.get_stats()
    print(
        f"✅ Final stats: maintenance running={final_stats['maintenance']['running']}"
    )

    print("🎉 MaintenanceManager test completed successfully!")


def test_deletion_log():
    """Test DeletionLog functionality."""
    print("🗑️ Testing DeletionLog...")

    # Mock HDF5 file
    class MockH5File:
        def __init__(self):
            self.datasets = {}

        def __contains__(self, key):
            return key in self.datasets

        def __getitem__(self, key):
            return self.datasets[key]

        def create_dataset(self, name, **kwargs):
            dataset = MockDataset()
            self.datasets[name] = dataset
            return dataset

        def flush(self):
            pass

    class MockDataset:
        def __init__(self):
            self.data = []
            self._shape = (0,)

        @property
        def shape(self):
            return self._shape

        def resize(self, new_shape):
            self._shape = new_shape
            if new_shape[0] > len(self.data):
                self.data.extend([None] * (new_shape[0] - len(self.data)))

        def __setitem__(self, idx, value):
            if idx >= len(self.data):
                self.data.extend([None] * (idx + 1 - len(self.data)))
            self.data[idx] = value

        def __getitem__(self, key):
            if isinstance(key, slice):
                return self.data[key]
            return self.data[key]

    from cidstore.maintenance import DeletionLog

    mock_file = MockH5File()
    deletion_log = DeletionLog(mock_file)
    print("✅ Created DeletionLog")

    # Test append
    deletion_log.append(1, 2, 3, 4)
    deletion_log.append(5, 6, 7, 8)
    print("✅ Appended deletions")

    # Test scan
    entries = deletion_log.scan()
    print(f"✅ Scanned {len(entries)} entries")

    # Test clear
    deletion_log.clear()
    entries_after_clear = deletion_log.scan()
    print(f"✅ After clear: {len(entries_after_clear)} entries")

    print("🎉 DeletionLog test completed!")


if __name__ == "__main__":
    test_deletion_log()
    test_maintenance_manager()
    print("\n🎉 All unified maintenance tests passed!")
