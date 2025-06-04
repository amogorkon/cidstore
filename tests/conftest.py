import pytest
from cidstore.maintenance import WALAnalyzer, MaintenanceManager, MaintenanceConfig
import tempfile

@pytest.fixture
def wal_analyzer(tree):
    # Use default config if not present
    config = getattr(tree, 'config', None) or MaintenanceConfig()
    analyzer = WALAnalyzer(tree, config)
    try:
        yield analyzer
    finally:
        analyzer.stop()
        analyzer.join()

@pytest.fixture
def maintenance_manager(tree):
    config = getattr(tree, 'config', None) or MaintenanceConfig()
    manager = MaintenanceManager(tree, config)
    try:
        yield manager
    finally:
        manager.stop()
        # Threads are joined in manager.stop()

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as td:
        yield td
