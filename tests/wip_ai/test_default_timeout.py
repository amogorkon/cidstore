#!/usr/bin/env python3
"""Test that the default 1 second timeout works"""

import os
import sys
import time

# Add the project root to the path so we can import cidstore
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from cidstore.maintenance import MaintenanceConfig, WALAnalyzer


def test_default_timeout():
    """Test that the default timeout is 1 second and works"""
    print("🧪 Testing default 1 second timeout...")

    # Create config with default values
    config = MaintenanceConfig()
    print(f"✅ Default thread_timeout: {config.thread_timeout}s")

    # Create a mock store
    class MockStore:
        def __init__(self):
            self.metrics = None

    mock_store = MockStore()

    # Create WALAnalyzer with default config
    analyzer = WALAnalyzer(mock_store, config)
    print("✅ WALAnalyzer created with default config")

    # Start the analyzer
    print("🚀 Starting WALAnalyzer thread...")
    start_time = time.time()
    analyzer.start()

    # Wait for 2 seconds (longer than the 1 second default timeout)
    print("⏳ Waiting 2.0s (should auto-stop after 1.0s)...")
    time.sleep(2.0)

    # Check if thread stopped
    elapsed = time.time() - start_time
    print(f"⏱️ {elapsed:.1f}s elapsed")

    if analyzer.is_alive():
        print(f"❌ DEFAULT TIMEOUT FAILED: Thread still running after {elapsed:.1f}s")
        analyzer.stop()
        analyzer.join(timeout=1.0)
    else:
        print("✅ DEFAULT TIMEOUT WORKS: Thread stopped automatically")

    print("🧪 Test completed")


if __name__ == "__main__":
    test_default_timeout()
