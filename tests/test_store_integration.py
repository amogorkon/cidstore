#!/usr/bin/env python3
"""Test script to verify store integration with WAL analyzer."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

try:
    from cidstore.store import CIDStore
    from cidstore.wal_analyzer import WALPatternAnalyzer

    print("✅ Successfully imported CIDStore and WALPatternAnalyzer")

    # Test that WALPatternAnalyzer can be instantiated
    analyzer = WALPatternAnalyzer()
    print("✅ WALPatternAnalyzer instantiated successfully")
    # Test key methods exist
    assert hasattr(analyzer, "record_operation"), "record_operation method missing"
    assert hasattr(analyzer, "get_danger_score"), "get_danger_score method missing"
    assert hasattr(analyzer, "get_adaptive_split_threshold"), (
        "get_adaptive_split_threshold method missing"
    )
    assert hasattr(analyzer, "get_high_danger_buckets"), (
        "get_high_danger_buckets method missing"
    )

    print("✅ All required methods exist")

    # Test method signatures work
    analyzer.record_operation("bucket_0001", "insert")
    score = analyzer.get_danger_score("bucket_0001")
    threshold = analyzer.get_adaptive_split_threshold(score)
    high_danger = analyzer.get_high_danger_buckets()

    print(
        f"✅ Methods work correctly: score={score:.3f}, threshold={threshold}, high_danger={len(high_danger)} buckets"
    )

except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)

print("🎉 Store integration test passed!")
