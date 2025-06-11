"""
Test the unified MaintenanceManager with realistic config and store.
"""

import tempfile

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager


def test_maintenance_manager_enhanced():
    with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
        temp_file = tmp.name
    config = MaintenanceConfig(
        gc_interval=5,
        maintenance_interval=3,
        sort_threshold=4,
        merge_threshold=2,
        wal_analysis_interval=10,
        adaptive_maintenance_enabled=True,
    )

    class MockStore:
        def __init__(self):
            self.hdf = None
            self.compact_calls = 0

        def compact(self, key):
            self.compact_calls += 1

        async def _maybe_merge_bucket(self, bucket_id, merge_threshold=8):
            pass

    mock_store = MockStore()
    manager = MaintenanceManager(mock_store, config)
    assert isinstance(manager, MaintenanceManager)
