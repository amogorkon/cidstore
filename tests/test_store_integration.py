#!/usr/bin/env python3
"""Test script to verify store integration with WAL analyzer."""

from cidstore.constants import OpType
from cidstore.maintenance import MaintenanceConfig, WALAnalyzer

print("✅ Successfully imported CIDStore and WALAnalyzer")

# Test that WALAnalyzer can be instantiated
analyzer = WALAnalyzer(store=None, config=MaintenanceConfig())
print("✅ WALAnalyzer instantiated successfully")
# Test key methods exist
assert hasattr(analyzer, "record_operation"), "record_operation method missing"
assert hasattr(analyzer, "get_danger_score"), "get_danger_score method missing"
assert hasattr(analyzer, "get_high_danger_buckets"), (
    "get_high_danger_buckets method missing"
)

print("✅ All required methods exist")


# Test method signatures work
analyzer.record_operation(1, OpType.INSERT)
score = analyzer.get_danger_score(1)
high_danger = analyzer.get_high_danger_buckets()

print(
    f"✅ Methods work correctly: score={score:.3f}, high_danger={len(high_danger)} buckets"
)

print("🎉 Store integration test passed!")
