"""
Comprehensive timeout testing for all maintenance threads.
"""

import time

from cidstore.maintenance import (
    BackgroundGC,
    BackgroundMaintenance,
    MaintenanceConfig,
    WALAnalyzer,
)


def test_individual_thread_timeouts():
    config = MaintenanceConfig(thread_timeout=1.5)

    class MockStore:
        def __init__(self):
            self.metrics = None

    mock_store = MockStore()
    thread_classes = [
        ("WALAnalyzer", WALAnalyzer),
        ("BackgroundGC", BackgroundGC),
        ("BackgroundMaintenance", BackgroundMaintenance),
    ]
    for thread_name, thread_class in thread_classes:
        thread = thread_class(mock_store, config)
        thread.start()
        time.sleep(config.thread_timeout + 0.5)
        assert not thread.is_alive(), f"{thread_name} did not stop after timeout"
        if thread.is_alive():
            thread.stop()
            thread.join(timeout=1.0)
