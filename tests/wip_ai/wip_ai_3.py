#!/usr/bin/env python3
"""
AI Work in Progress - Comprehensive timeout testing for all maintenance threads
"""

import os
import sys
import time

# Add the project root to the path so we can import cidstore
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from cidstore.maintenance import (
    BackgroundGC,
    BackgroundMaintenance,
    MaintenanceConfig,
    WALAnalyzer,
)


def test_individual_thread_timeouts():
    """Test that each maintenance thread respects timeouts individually"""
    print("🧪 Testing individual thread timeout functionality...")

    config = MaintenanceConfig(thread_timeout=1.5)  # 1.5 second timeout
    print(f"✅ Config created with {config.thread_timeout}s timeout")

    # Create a mock store
    class MockStore:
        def __init__(self):
            self.metrics = None

    mock_store = MockStore()

    # Test each thread type
    thread_classes = [
        ("WALAnalyzer", WALAnalyzer),
        ("BackgroundGC", BackgroundGC),
        ("BackgroundMaintenance", BackgroundMaintenance),
    ]

    for thread_name, thread_class in thread_classes:
        print(f"\n📝 Testing {thread_name}...")

        try:
            # Create and start thread
            thread = thread_class(mock_store, config)
            print(f"✅ {thread_name} created")

            start_time = time.time()
            thread.start()
            print(
                f"🚀 {thread_name} started - {'running' if thread.is_alive() else 'stopped'}"
            )

            # Wait longer than timeout
            wait_time = config.thread_timeout + 0.5
            print(f"⏳ Waiting {wait_time}s for timeout...")
            time.sleep(wait_time)

            # Check status
            elapsed = time.time() - start_time
            if thread.is_alive():
                print(
                    f"❌ {thread_name} TIMEOUT FAILED: still running after {elapsed:.1f}s"
                )
                thread.stop()
                thread.join(timeout=1.0)
                if not thread.is_alive():
                    print(f"✅ {thread_name} stopped after manual stop")
            else:
                print(
                    f"✅ {thread_name} TIMEOUT WORKS: stopped automatically after {elapsed:.1f}s"
                )

        except Exception as e:
            print(f"❌ {thread_name} failed with error: {e}")

    print("\n🧪 Individual thread timeout test completed")


def test_maintenancemanager_timeout():
    """Test MaintenanceManager with timeout"""
    print("\n🧪 Testing MaintenanceManager timeout functionality...")

    from cidstore.maintenance import MaintenanceManager

    # Create a comprehensive mock store
    class MockHDF:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __contains__(self, key):
            return key == "/deletion_log"  # Only this dataset exists

        def create_dataset(self, name, **kwargs):
            # Return a mock dataset
            class MockDataset:
                def __len__(self):
                    return 0

                def resize(self, *args):
                    pass

            return MockDataset()

        def __getitem__(self, key):
            if key == "/deletion_log":

                class MockDataset:
                    def __len__(self):
                        return 0

                    def resize(self, *args):
                        pass

                return MockDataset()
            raise KeyError(key)

    class MockStore:
        def __init__(self):
            self.metrics = None
            self.hdf = MockHDF()

    config = MaintenanceConfig(thread_timeout=2.0)  # 2 second timeout
    print(f"✅ Config created with {config.thread_timeout}s timeout")

    mock_store = MockStore()

    try:
        # Create MaintenanceManager
        manager = MaintenanceManager(mock_store, config)
        print("✅ MaintenanceManager created")

        # Start all threads
        print("🚀 Starting all maintenance threads...")
        start_time = time.time()
        manager.start()

        # Check that threads are running
        print(f"✅ Threads started: {len(manager._threads)} threads")
        for thread in manager._threads:
            print(
                f"   - {thread.name}: {'running' if thread.is_alive() else 'stopped'}"
            )

        # Wait longer than timeout
        wait_time = config.thread_timeout + 1.0
        print(f"⏳ Waiting {wait_time}s for timeout...")
        time.sleep(wait_time)

        # Check status
        elapsed = time.time() - start_time
        running_threads = [t for t in manager._threads if t.is_alive()]

        if running_threads:
            print(
                f"❌ TIMEOUT FAILED: {len(running_threads)}/{len(manager._threads)} threads still running after {elapsed:.1f}s:"
            )
            for thread in running_threads:
                print(f"   - {thread.name}: still alive")

            # Force stop
            print("🛑 Force stopping all threads...")
            manager.stop()
            time.sleep(0.5)

            still_running = [t for t in manager._threads if t.is_alive()]
            if still_running:
                print(
                    f"❌ {len(still_running)} threads still running after force stop!"
                )
            else:
                print("✅ All threads stopped after force stop")
        else:
            print(
                f"✅ TIMEOUT WORKS: All {len(manager._threads)} threads stopped automatically after {elapsed:.1f}s"
            )

    except Exception as e:
        print(f"❌ MaintenanceManager test failed: {e}")
        import traceback

        traceback.print_exc()

    print("🧪 MaintenanceManager timeout test completed")


if __name__ == "__main__":
    test_individual_thread_timeouts()
    test_maintenancemanager_timeout()
    print("\n🎉 All timeout tests completed!")
