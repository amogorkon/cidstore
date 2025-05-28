#!/usr/bin/env python3
"""
AI Work in Progress - Testing timeout functionality for maintenance threads
"""

import time

from cidstore.maintenance import MaintenanceConfig, WALAnalyzer


def test_timeout_functionality():
    """Test that the timeout mechanism actually stops the threads"""
    print("🧪 Testing WALAnalyzer timeout functionality...")

    # Test with very short timeout for testing
    config = MaintenanceConfig(thread_timeout=1.0)  # 1 second timeout
    print(f"✅ Config created with {config.thread_timeout}s timeout")

    # Create a mock store
    class MockStore:
        def __init__(self):
            self.metrics = None

    mock_store = MockStore()
    # Create WALAnalyzer directly (avoid MaintenanceManager complexity)
    analyzer = WALAnalyzer(mock_store, config)
    print("✅ WALAnalyzer created")

    # Debug: check that the analyzer has the right timeout
    print(
        f"🔍 Debug: analyzer.config.thread_timeout = {analyzer.config.thread_timeout}"
    )

    # Start the analyzer
    print("🚀 Starting WALAnalyzer thread...")
    start_time = time.time()
    analyzer.start()

    # Check that thread is running
    print(
        f"✅ Thread started: {analyzer.name} - {'running' if analyzer.is_alive() else 'stopped'}"
    )

    # Wait longer than the timeout to see if it stops automatically
    wait_time = config.thread_timeout + 0.5
    print(f"⏳ Waiting {wait_time}s for timeout to trigger...")
    time.sleep(wait_time)

    # Check if thread is still running
    elapsed = time.time() - start_time
    print(f"⏱️ {elapsed:.1f}s elapsed")

    if analyzer.is_alive():
        print(f"❌ TIMEOUT FAILED: Thread still running after {elapsed:.1f}s")
        print("   The thread should have stopped automatically!")

        # Force stop it
        print("🛑 Force stopping thread...")
        analyzer.stop()
        analyzer.join(timeout=1.0)

        if analyzer.is_alive():
            print("❌ Thread still running even after force stop!")
        else:
            print("✅ Thread stopped after force stop")
    else:
        print("✅ TIMEOUT WORKS: Thread stopped automatically")

    print("🧪 Test completed")


if __name__ == "__main__":
    test_timeout_functionality()
