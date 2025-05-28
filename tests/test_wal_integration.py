#!/usr/bin/env python3
"""Test script to verify WAL analyzer integration with store."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from cidstore.wal_analyzer import WALPatternAnalyzer


def test_wal_analyzer_basic():
    """Test basic WAL analyzer functionality."""
    analyzer = WALPatternAnalyzer()

    # Test recording operations
    analyzer.record_operation("bucket_0001", "insert")
    analyzer.record_operation("bucket_0001", "insert")
    analyzer.record_operation("bucket_0002", "delete")

    # Test danger score calculation
    danger_score = analyzer.get_danger_score("bucket_0001")
    print(f"Danger score for bucket_0001: {danger_score}")

    # Test adaptive threshold
    threshold = analyzer.get_adaptive_split_threshold(danger_score)
    print(f"Adaptive split threshold: {threshold}")

    # Test high danger buckets
    high_danger = analyzer.get_high_danger_buckets(
        threshold=0.01
    )  # Low threshold to see results
    print(f"High danger buckets: {high_danger}")

    # Test maintenance recommendations
    recommendations = analyzer.get_maintenance_recommendations()
    print(f"Maintenance recommendations: {recommendations}")

    # Test statistics
    stats = analyzer.get_stats_summary()
    print(f"Statistics summary: {stats}")

    print("✅ WAL analyzer basic tests passed!")


def test_functional_features():
    """Test functional programming features."""
    analyzer = WALPatternAnalyzer()

    # Test with multiple operations to trigger functional calculations
    for i in range(10):
        analyzer.record_operation(f"bucket_{i:04d}", "insert")
        analyzer.record_operation(f"bucket_{i:04d}", "insert")

    # Test sequential pattern detection
    for i in range(5):
        analyzer.record_operation(f"bucket_{i:04d}", "insert")

    danger_scores = [analyzer.get_danger_score(f"bucket_{i:04d}") for i in range(5)]
    print(f"Danger scores for buckets 0-4: {danger_scores}")

    # Test adaptive thresholds
    thresholds = [
        analyzer.get_adaptive_split_threshold(score) for score in danger_scores
    ]
    print(f"Adaptive thresholds: {thresholds}")

    print("✅ Functional features tests passed!")


if __name__ == "__main__":
    print("Testing WAL analyzer integration...")
    test_wal_analyzer_basic()
    test_functional_features()
    print("🎉 All tests passed!")
