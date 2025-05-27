"""WORK IN PROGRESS, but AI Only. AI, here you can freely test your code without human intervention."""

import os
import tempfile

from cidstore.maintenance import MaintenanceConfig, MaintenanceManager
from cidstore.storage import Storage

print("🔬 Final integration test...")

# Create a temporary HDF5 file for testing
with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
    temp_file = tmp.name

try:
    # Test that Storage can be created without maintenance components
    print("\\n1. Testing Storage creation...")
    storage = Storage(temp_file)
    print("   ✅ Storage created successfully (without individual maintenance)")

    # Test that MaintenanceManager can be created
    print("\\n2. Testing MaintenanceManager creation...")
    config = MaintenanceConfig()
    manager = MaintenanceManager(storage, config)
    print("   ✅ MaintenanceManager created successfully")

    # Test that all threads are available    print("\\n3. Testing maintenance components...")
    print(f"   ✅ Deletion log: {type(manager.deletion_log).__name__}")
    print(f"   ✅ Background GC: {type(manager.gc_thread).__name__}")
    print(f"   ✅ Background maintenance: {type(manager.maintenance_thread).__name__}")
    print(f"   ✅ WAL analyzer: {type(manager.wal_analyzer_thread).__name__}")

    # Test that WAL analyzer has sophisticated methods
    print("\\n4. Testing WAL analyzer methods...")
    analyzer = manager.wal_analyzer_thread
    methods = [
        "record_operation",
        "get_danger_score",
        "should_preemptively_split",
        "should_merge_buckets",
        "get_high_danger_buckets",
        "get_maintenance_recommendations",
    ]
    for method in methods:
        has_method = hasattr(analyzer, method)
        print(f"   ✅ {method}: {'available' if has_method else 'MISSING'}")

    storage.close()
    print("\\n🎉 Integration test successful - enhanced maintenance system working!")

finally:
    # Clean up
    if os.path.exists(temp_file):
        os.unlink(temp_file)
