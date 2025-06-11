"""
Test that the default WALAnalyzer thread timeout works (should auto-stop after 1s).
"""

import time

from cidstore.maintenance import MaintenanceConfig, WALAnalyzer


def test_wal_analyzer_timeout():
    config = MaintenanceConfig()  # default thread_timeout is 1s

    class MockStore:
        def __init__(self):
            self.metrics = None

    mock_store = MockStore()
    analyzer = WALAnalyzer(mock_store, config)
    start_time = time.time()
    analyzer.start()
    time.sleep(2.0)  # Wait longer than the timeout
    elapsed = time.time() - start_time
    # The thread should have stopped itself
    assert not analyzer.is_alive(), (
        f"WALAnalyzer thread still running after {elapsed:.1f}s (timeout failed)"
    )
    # Cleanup (in case)
    if analyzer.is_alive():
        analyzer.stop()
        analyzer.join(timeout=1.0)
