import time

import pytest
import zmq
import zmq.asyncio
from httpx import AsyncClient

from cidstore.control_api import app as fastapi_app
from cidstore.keys import E
from cidstore.storage import Storage
from cidstore.store import WAL, CIDStore

pytestmark = pytest.mark.asyncio


@pytest.mark.asyncio
async def test_full_integration(tmp_path):
    # --- Setup store and WAL ---
    storage = Storage(tmp_path / "cidstore.h5")
    wal = WAL(tmp_path / "cidstore.wal")
    store = CIDStore(storage, wal)
    await store.async_init()

    # --- Start FastAPI app using httpx test client ---
    async with AsyncClient(app=fastapi_app, base_url="http://test") as ac:
        # Health check
        resp = await ac.get("/health")
        assert resp.status_code == 200
        assert resp.text == "ok"

        # Config GET/PUT
        resp = await ac.get("/config/promotion_threshold")
        assert resp.status_code == 200
        orig_threshold = resp.json()["promotion_threshold"]
        new_threshold = orig_threshold + 1
        resp = await ac.put(
            "/config/promotion_threshold", json={"promotion_threshold": new_threshold}
        )
        assert resp.status_code == 200
        assert resp.json()["promotion_threshold"] == new_threshold

        # Insert data via store directly (simulate API insert if you have such endpoint)
        key = E.from_str("integration")
        value = E(42)
        await store.insert(key, value)
        result = await store.get(key)
        assert value in result

    # --- ZMQ Integration ---
    ctx = zmq.asyncio.Context()
    # Assume your ZMQ server is running and listening on this endpoint
    zmq_endpoint = "tcp://127.0.0.1:5555"
    socket = ctx.socket(zmq.REQ)
    socket.connect(zmq_endpoint)

    # Send a test request (adapt to your ZMQ protocol)
    socket.send_json({"op": "insert", "key": str(key), "value": int(value)})
    reply = await socket.recv_json()
    assert reply.get("status") == "ok"

    # Query via ZMQ
    socket.send_json({"op": "get", "key": str(key)})
    reply = await socket.recv_json()
    assert int(value) in reply.get("values", [])

    socket.close()
    ctx.term()

    # --- Background maintenance ---
    # Wait for background maintenance to run at least once
    if hasattr(store, "maintenance_manager"):
        time.sleep(1.5)
        stats = store.maintenance_manager.maintenance_thread.get_stats()
        assert stats["last_run"] > 0

    # --- Data integrity after all operations ---
    result = await store.get(key)
    assert value in result

    # --- Cleanup ---
    if hasattr(store, "aclose"):
        await store.aclose()
    elif hasattr(store, "close"):
        store.close()
    wal.close()
