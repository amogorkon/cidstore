from fastapi.testclient import TestClient

from cidstore.control_api import app


def test_health():
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.text == "ok"


def test_ready():
    client = TestClient(app)
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.text == "ready"


def test_metrics():
    client = TestClient(app)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "cidstore_latency_p99" in resp.text


def test_config_roundtrip():
    client = TestClient(app)
    # Get and set promotion_threshold
    resp = client.get("/config/promotion_threshold")
    assert resp.status_code == 200
    orig = resp.json()["promotion_threshold"]
    resp = client.put(
        "/config/promotion_threshold",
        json={"promotion_threshold": orig + 1},
        headers={"Authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 200
    resp = client.get("/config/promotion_threshold")
    assert resp.json()["promotion_threshold"] == orig + 1
    # Restore
    client.put(
        "/config/promotion_threshold",
        json={"promotion_threshold": orig},
        headers={"Authorization": "Bearer testtoken"},
    )


def test_debug_bucket():
    client = TestClient(app)
    resp = client.get("/debug/bucket/42", headers={"X-Internal-Test": "1"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["bucket_id"] == 42
    # Check that checksum is present for entry validation
    assert "checksum" in data
