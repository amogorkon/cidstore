#!/usr/bin/env python3
"""
Debug timeout mechanism
"""

import time

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager


def debug_timeout():
    """Debug the timeout mechanism step by step"""
    print("🔍 Debugging timeout mechanism...")

    # Create config with default timeout
    config = MaintenanceConfig()
    print(f"✅ Config thread_timeout: {config.thread_timeout}s")

    # Create mock store
    class MockHDF:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __contains__(self, key):
            return False

        def create_dataset(self, *args, **kwargs):
            class MockDataset:
                def __len__(self):
                    return 0

                def resize(self, *args):
                    pass

                def __getitem__(self, key):
                    return []

                def __setitem__(self, key, value):
                    pass

            return MockDataset()

    class MockStore:
        def __init__(self):
            self.metrics = None
            self.hdf = MockHDF()

            # Add required methods
            def run_gc_once(self):
                pass

            self.run_gc_once = run_gc_once

    mock_store = MockStore()

    # Create MaintenanceManager
    print("🚀 Creating MaintenanceManager...")
    manager = MaintenanceManager(mock_store, config)
    print(f"✅ Created with {len(manager._threads)} threads")

    # Start the manager
    print("🚀 Starting threads...")
    start_time = time.time()
    manager.start()

    print("⏳ Waiting and checking thread status every 0.2s...")

    # Check thread status every 200ms for up to 3 seconds
    for i in range(15):  # 15 * 0.2 = 3 seconds max
        time.sleep(0.2)
        elapsed = time.time() - start_time

        alive_threads = [t for t in manager._threads if t.is_alive()]
        print(
            f"   {elapsed:.1f}s: {len(alive_threads)}/{len(manager._threads)} threads alive"
        )

        if not alive_threads:
            print(f"✅ All threads stopped after {elapsed:.1f}s")
            return

    # If we get here, threads are still running
    print("❌ Threads still running after 3s, forcing stop...")
    manager.stop()
    time.sleep(0.5)

    still_alive = [t for t in manager._threads if t.is_alive()]
    if still_alive:
        print(f"❌ {len(still_alive)} threads still alive after force stop!")
    else:
        print("✅ Threads stopped after force stop")


if __name__ == "__main__":
    debug_timeout()
