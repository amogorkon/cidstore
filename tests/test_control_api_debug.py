from fastapi.testclient import TestClient


def async_noop():
    async def _noop():
        return None

    return _noop


class FakeResult:
    def __init__(self, high, high_mid, low_mid, low):
        self.high = high
        self.high_mid = high_mid
        self.low_mid = low_mid
        self.low = low


class FakeStore:
    def __init__(self):
        self.deleted = None
        self.deleted_value = None

    async def get(self, key):
        # Return a single result object with numeric components
        return [FakeResult(10, 20, 30, 40)]

    async def delete(self, key):
        self.deleted = key

    async def delete_value(self, key, value):
        self.deleted_value = (key, value)


def test_debug_get_and_delete_4part_and_2part():
    import src.cidstore.control_api as control_api

    # Prevent startup/shutdown events from executing heavy initialization
    # Clear registered startup/shutdown handlers so TestClient won't run them
    control_api.app.router.on_startup.clear()
    control_api.app.router.on_shutdown.clear()

    # Inject a fake store
    fake = FakeStore()
    control_api.store = fake

    with TestClient(control_api.app) as client:
        # 4-part GET should succeed
        resp = client.get("/debug/get?high=1&high_mid=2&low_mid=3&low=4")
        assert resp.status_code == 200
        body = resp.json()
        assert "version" in body
        assert "results" in body
        assert body["results"][0] == [10, 20, 30, 40]

        # Legacy 2-part GET should also work (zero-extended)
        resp2 = client.get("/debug/get?high=123&low=456")
        assert resp2.status_code == 200

        # Invalid GET (missing parts) should return 400
        resp3 = client.get("/debug/get")
        assert resp3.status_code == 200
        data3 = resp3.json()
        assert data3.get("error_code") == 400

        # 4-part delete should return ok and call store.delete
        del_resp = client.post(
            "/debug/delete",
            json={"high": 1, "high_mid": 2, "low_mid": 3, "low": 4},
        )
        assert del_resp.status_code == 200
        assert del_resp.json().get("result") == "ok"

        # Legacy delete should return ok
        del_resp2 = client.post("/debug/delete", json={"high": 7, "low": 8})
        assert del_resp2.status_code == 200
        assert del_resp2.json().get("result") == "ok"
