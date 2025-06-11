"""
Test functional programming features of WALAnalyzer.
"""

from cidstore.constants import OpType
from cidstore.maintenance import MaintenanceConfig, WALAnalyzer


def test_wal_analyzer_functional():
    analyzer = WALAnalyzer(store=None, config=MaintenanceConfig())
    for i in range(10):
        analyzer.record_operation(i, OpType.INSERT)
        analyzer.record_operation(i, OpType.INSERT)
    for i in range(5):
        analyzer.record_operation(i, OpType.INSERT)
    danger_scores = [analyzer.get_danger_score(i) for i in range(5)]
    assert all(isinstance(score, float) for score in danger_scores)
