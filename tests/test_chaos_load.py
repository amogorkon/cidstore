import asyncio
import random

import pytest
from httpx import AsyncClient

from cidstore.control_api import app

pytestmark = pytest.mark.asyncio


@pytest.mark.asyncio
async def test_control_api_load():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        # Simulate rapid config changes
        for _ in range(20):
            val = random.randint(1, 256)
            resp = await ac.put(
                "/config/promotion_threshold", json={"promotion_threshold": val}
            )
            assert resp.status_code == 200
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_control_api_crash_recovery(monkeypatch):
    # Simulate crash by raising exception in config set
    async with AsyncClient(app=app, base_url="http://test") as ac:
        orig = (await ac.get("/config/promotion_threshold")).json()[
            "promotion_threshold"
        ]

        async def crash(*args, **kwargs):
            raise RuntimeError("Simulated crash")

        monkeypatch.setattr("cidstore.control_api.set_promotion_threshold", crash)
        with pytest.raises(RuntimeError):
            await ac.put(
                "/config/promotion_threshold", json={"promotion_threshold": 99}
            )
        # Restore
        monkeypatch.setattr(
            "cidstore.control_api.set_promotion_threshold", lambda data: orig
        )
