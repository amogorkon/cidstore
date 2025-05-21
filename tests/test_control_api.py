import pytest
from httpx import AsyncClient

from cidstore.control_api import app

pytestmark = pytest.mark.asyncio


@pytest.mark.asyncio
async def test_health():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/health")
        assert resp.status_code == 200
        assert resp.text == "ok"


@pytest.mark.asyncio
async def test_ready():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/ready")
        assert resp.status_code == 200
        assert resp.text == "ready"


@pytest.mark.asyncio
async def test_metrics():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/metrics")
        assert resp.status_code == 200
        assert "cidstore_latency_p99" in resp.text


@pytest.mark.asyncio
async def test_config_roundtrip():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        # Get and set promotion_threshold
        resp = await ac.get("/config/promotion_threshold")
        assert resp.status_code == 200
        orig = resp.json()["promotion_threshold"]
        resp = await ac.put(
            "/config/promotion_threshold", json={"promotion_threshold": orig + 1}
        )
        assert resp.status_code == 200
        resp = await ac.get("/config/promotion_threshold")
        assert resp.json()["promotion_threshold"] == orig + 1
        # Restore
        await ac.put("/config/promotion_threshold", json={"promotion_threshold": orig})


@pytest.mark.asyncio
async def test_debug_bucket():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/debug/bucket/42")
        assert resp.status_code == 200
        data = resp.json()
        assert data["bucket_id"] == 42
        # No ECC/state_mask in new model; check for checksum instead
        assert "checksum" in data
