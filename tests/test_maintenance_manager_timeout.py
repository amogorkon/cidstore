"""
Test that MaintenanceManager thread timeout works (should auto-stop after 1s).
"""

import time

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager


def test_maintenance_manager_timeout():
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
