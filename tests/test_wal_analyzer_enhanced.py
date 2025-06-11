"""
Test the enhanced WAL analyzer with sophisticated pattern analysis.
"""

import cidstore.wal_analyzer as wal_analyzer
from cidstore.constants import OpType
from cidstore.maintenance import MaintenanceConfig, WALAnalyzer


def test_enhanced_wal_analyzer():
    config = MaintenanceConfig()
    analyzer = WALAnalyzer(None, config)
    analyzer.record_operation(1, OpType.INSERT)
    analyzer.record_operation(2, OpType.INSERT)
    analyzer.record_operation(1, OpType.DELETE)
    analyzer.record_operation(1, OpType.INSERT)
    score1 = analyzer.get_danger_score(1)
    score2 = analyzer.get_danger_score(2)
    assert isinstance(score1, float)
    assert isinstance(score2, float)
    bucket_stats = wal_analyzer.BucketStats(operation_count=50)
    danger_score = wal_analyzer.calculate_danger_score(bucket_stats, 1, [])
    assert isinstance(danger_score, float)
