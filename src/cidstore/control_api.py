from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse

# Placeholder for JWT and IP whitelist logic
# from .auth import verify_jwt, is_internal_ip

app = FastAPI(title="CIDStore Control Plane", docs_url="/docs", redoc_url=None)

# Simulated in-memory config and metrics (replace with real store integration)
CONFIG = {
    "promotion_threshold": 128,
    "batch_size": 64,
}
METRICS = {
    "latency_p99": 0.0001,
    "throughput_ops": 1000,
    "buffer_occupancy": 0.2,
    "flush_duration": 0.01,
    "lock_contention_ratio": 0.01,
    "error_rate": 0.0,
}


@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.get("/ready", response_class=PlainTextResponse)
async def ready():
    return "ready"


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    # In production, return Prometheus-formatted metrics
    return "".join(f"cidstore_{k} {v}\n" for k, v in METRICS.items())


@app.get("/config/promotion_threshold")
async def get_promotion_threshold():
    return {"promotion_threshold": CONFIG["promotion_threshold"]}


@app.put("/config/promotion_threshold")
async def set_promotion_threshold(data: dict):
    # TODO: Add JWT auth check
    if "promotion_threshold" not in data:
        raise HTTPException(400, "Missing promotion_threshold")
    CONFIG["promotion_threshold"] = int(data["promotion_threshold"])
    return {"promotion_threshold": CONFIG["promotion_threshold"]}


@app.get("/config/batch_size")
async def get_batch_size():
    return {"batch_size": CONFIG["batch_size"]}


@app.put("/config/batch_size")
async def set_batch_size(data: dict):
    # TODO: Add JWT auth check
    if "batch_size" not in data:
        raise HTTPException(400, "Missing batch_size")
    CONFIG["batch_size"] = int(data["batch_size"])
    return {"batch_size": CONFIG["batch_size"]}


@app.get("/debug/bucket/{bucket_id}")
async def debug_bucket(bucket_id: int, request: Request):
    # TODO: Restrict to internal IPs
    # TODO: Return real bucket metadata from store
    return {"bucket_id": bucket_id, "ecc_state": "ok", "load": 0.5}
