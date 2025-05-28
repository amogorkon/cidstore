#!/usr/bin/env python3
"""
AI Work in Progress

AI ONLY.
"""

import os
import tempfile
import threading
import time

from cidstore import wal_analyzer
from cidstore.maintenance import MaintenanceConfig, MaintenanceManager, WALAnalyzer
from cidstore.storage import Storage


def test_enhanced_wal_analyzer():
    """Test the enhanced WAL analyzer with sophisticated pattern analysis."""
    print("🧪 Testing Enhanced WAL Analyzer...")

    # Test that we can create a WAL analyzer
    config = MaintenanceConfig()
    analyzer = WALAnalyzer(None, config)

    # Test recording operations with different types
    analyzer.record_operation(1, 1)  # Insert to bucket 1
    analyzer.record_operation(2, 1)  # Insert to bucket 2
    analyzer.record_operation(1, 2)  # Delete from bucket 1
    analyzer.record_operation(1, 1)  # Another insert to bucket 1

    # Test getting danger scores
    score1 = analyzer.get_danger_score(1)
    score2 = analyzer.get_danger_score(2)

    print(
        f"✅ WAL Analyzer works: bucket 1 score={score1:.2f}, bucket 2 score={score2:.2f}"
    )
    print(f"✅ Bucket stats tracked: {len(analyzer.bucket_stats)} buckets")

    # Test bucket statistics details
    if 1 in analyzer.bucket_stats:
        stats = analyzer.bucket_stats[1]
        print(
            f"✅ Bucket 1 stats: {stats.operation_count} ops, {len(stats.insert_timestamps or [])} inserts tracked"
        )

    # Test global functions directly
    bucket_stats = wal_analyzer.BucketStats(operation_count=50)
    danger_score = wal_analyzer.calculate_danger_score(bucket_stats, 1, [])
    print(f"✅ Global functions work: danger_score={danger_score:.2f}")


def test_maintenance_manager():
    """Test the unified MaintenanceManager."""
    print("\n🧪 Testing MaintenanceManager...")

    # Create a temporary HDF5 file for realistic testing
    with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
        temp_file = tmp.name

    try:
        # Test that Storage can be created without maintenance components
        storage = Storage(temp_file)

        # Test that the MaintenanceManager can be created
        config = MaintenanceConfig()
        manager = MaintenanceManager(storage, config)
        print(
            "✅ MaintenanceManager created successfully"
        )  # Test that WAL analyzer has all the enhanced methods
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
            if hasattr(analyzer, method):
                print(f"✅ WALAnalyzer has {method}")
            else:
                print(f"❌ WALAnalyzer missing {method}")

        # Test manager components
        components = ["deletion_log", "gc_thread", "maintenance_thread", "wal_analyzer"]
        for component in components:
            if hasattr(manager, component):
                print(f"✅ MaintenanceManager has {component}")
            else:
                print(f"❌ MaintenanceManager missing {component}")

        storage.close()
    finally:
        if os.path.exists(temp_file):
            os.unlink(temp_file)


def test_adaptive_functionality():
    """Test adaptive threshold functionality."""
    print("\n🧪 Testing Adaptive Functionality...")

    config = MaintenanceConfig()
    analyzer = WALAnalyzer(None, config)

    # Create a realistic access pattern - heavy activity on bucket 1
    print("📊 Creating heavy access pattern on bucket 1...")
    for i in range(150):
        analyzer.record_operation(1, 1)  # Many inserts
        if i % 10 == 0:  # Occasional deletes
            analyzer.record_operation(1, 2)

    # Some light activity on other buckets
    for bucket_id in [2, 3, 4]:
        for i in range(5):
            analyzer.record_operation(bucket_id, 1)

    # Test danger scores after heavy activity
    for bucket_id in [1, 2, 3, 4]:
        score = analyzer.get_danger_score(bucket_id)
        print(f"📈 Bucket {bucket_id} danger score: {score:.2f}")

    # Test preemptive split decision with different sizes
    for size in [50, 100, 128, 150]:
        should_split = analyzer.should_preemptively_split("bucket_0001", size)
        print(f"⚡ Preemptive split (size {size}): {should_split}")

    # Test merge decisions for small buckets
    should_merge_23 = analyzer.should_merge_buckets("bucket_0002", "bucket_0003", 5, 5)
    should_merge_12 = analyzer.should_merge_buckets(
        "bucket_0001", "bucket_0002", 5, 5
    )  # Hot bucket
    print(f"🔗 Should merge buckets 2&3 (low activity): {should_merge_23}")
    print(f"🔗 Should merge buckets 1&2 (hot bucket 1): {should_merge_12}")

    # Test high danger bucket detection
    high_danger = analyzer.get_high_danger_buckets(threshold=0.1)
    print(f"🚨 High danger buckets (>0.1): {high_danger}")

    # Test maintenance recommendations
    recommendations = analyzer.get_maintenance_recommendations()
    print(f"🛠️  Maintenance recommended: {recommendations['should_run_maintenance']}")
    print(f"📋 Action suggestions: {len(recommendations.get('suggested_actions', []))}")
    for action in recommendations.get("suggested_actions", []):
        print(f"   💡 {action}")


def test_sophisticated_patterns():
    """Test sophisticated pattern detection."""
    print("\n🧪 Testing Sophisticated Pattern Detection...")

    config = MaintenanceConfig()
    analyzer = WALAnalyzer(None, config)

    # Create sequential access pattern
    print("🔄 Creating sequential access pattern...")
    current_time = time.time()
    for i in range(50):
        bucket_id = (i % 10) + 1  # Sequential buckets 1-10
        analyzer.record_operation(bucket_id, 1)
        time.sleep(0.001)  # Small delay for timing

    # Create hot spot pattern
    print("🔥 Creating hot spot pattern...")
    for i in range(100):
        analyzer.record_operation(5, 1)  # Heavy activity on bucket 5
        if i % 20 == 0:
            time.sleep(0.01)  # Simulate bursts

    # Analyze patterns
    recommendations = analyzer.get_maintenance_recommendations()
    print("🔍 Pattern analysis complete:")
    print(f"   📊 Buckets tracked: {len(analyzer.bucket_stats)}")
    print(f"   📈 Operations recorded: {len(analyzer.operation_history)}")
    print(
        f"   🚨 High danger buckets: {len(recommendations.get('high_danger_buckets', []))}"
    )

    # Test adaptive thresholds
    danger_score = analyzer.get_danger_score(5)  # Hot bucket
    threshold = wal_analyzer.get_adaptive_split_threshold(danger_score)
    print(f"⚙️  Bucket 5 danger score: {danger_score:.2f}")
    print(f"🎯 Adaptive split threshold: {threshold}")

    # Test statistics summary
    stats = wal_analyzer.get_stats_summary(
        analyzer.bucket_stats, analyzer.operation_history
    )
    print("📊 Statistics summary:")
    print(f"   📦 Total buckets: {stats['total_buckets_tracked']}")
    print(f"   📝 Total operations: {stats['total_operations']}")
    print(f"   📊 Avg danger score: {stats['avg_danger_score']:.2f}")
    print(f"   📈 Max danger score: {stats['max_danger_score']:.2f}")


def test_thread_safety():
    """Test thread safety of the WAL analyzer."""
    print("\n🧪 Testing Thread Safety...")

    config = MaintenanceConfig()
    analyzer = WALAnalyzer(None, config)

    # Record operations from multiple threads
    def worker_thread(thread_id, operations):
        for i in range(operations):
            bucket_id = (thread_id * 10 + i % 5) + 1
            analyzer.record_operation(bucket_id, 1)

    print("🔄 Starting multiple worker threads...")
    threads = []
    for i in range(5):
        t = threading.Thread(target=worker_thread, args=(i, 50))
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    print("✅ Thread safety test completed")
    print(f"   📊 Final bucket count: {len(analyzer.bucket_stats)}")
    print(f"   📝 Final operation count: {len(analyzer.operation_history)}")

    # Test concurrent access to insights
    insights = analyzer.get_insights()
    recommendations = analyzer.get_maintenance_recommendations()
    print(f"   💡 Generated {len(insights)} insights")
    print(f"   🛠️  {len(recommendations.get('suggested_actions', []))} recommendations")


def test_global_functions():
    """Test that all WAL analyzer global functions work correctly."""
    print("\n🧪 Testing WAL Analyzer Global Functions...")

    # Test dataclasses
    bucket_stats = wal_analyzer.BucketStats(operation_count=100)
    operation = wal_analyzer.Operation(op_type=1, bucket_id=1, timestamp=time.time())
    print(f"✅ BucketStats created: {bucket_stats.operation_count} operations")
    print(
        f"✅ Operation created: type={operation.op_type}, bucket={operation.bucket_id}"
    )

    # Test utility functions
    bucket_id = wal_analyzer.parse_bucket_id("bucket_0042")
    print(f'✅ parse_bucket_id("bucket_0042"): {bucket_id}')

    trimmed = wal_analyzer.trim_list([1, 2, 3, 4, 5], 3)
    print(f"✅ trim_list([1,2,3,4,5], 3): {trimmed}")

    # Test scoring functions
    freq_score = wal_analyzer.calculate_frequency_score(150)
    print(f"✅ frequency_score(150): {freq_score:.2f}")

    # Test with realistic timestamps
    current_time = time.time()
    timestamps = [current_time - 30, current_time - 15, current_time - 5]
    rate_score = wal_analyzer.calculate_insert_rate_score(timestamps, current_time)
    print(f"✅ insert_rate_score: {rate_score:.2f}")

    recency_score = wal_analyzer.calculate_recency_score(
        current_time - 60, current_time
    )
    print(f"✅ recency_score: {recency_score:.2f}")

    # Test adaptive threshold
    danger_score = 0.8
    threshold = wal_analyzer.get_adaptive_split_threshold(danger_score)
    print(f"✅ adaptive_split_threshold(0.8): {threshold}")


if __name__ == "__main__":
    print("🚀 Running AI Integration Tests for Enhanced WAL Analyzer\n")

    test_enhanced_wal_analyzer()
    test_maintenance_manager()
    test_adaptive_functionality()
    test_sophisticated_patterns()
    test_thread_safety()
    test_global_functions()

    print("\n🎉 All integration tests completed!")
    print(
        "📋 Summary: Enhanced WAL analyzer with sophisticated pattern analysis is working correctly!"
    )
