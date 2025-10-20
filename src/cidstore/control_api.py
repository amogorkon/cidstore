import asyncio
import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

from .auth import internal_only, jwt_required

try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
except ImportError:
    generate_latest = None
    CONTENT_TYPE_LATEST = "text/plain"

logger = logging.getLogger(__name__)

store = None
zmq_server = None
zmq_task = None


app = FastAPI(title="CIDStore Control Plane", docs_url="/docs", redoc_url=None)


@app.on_event("startup")
async def startup_event():
    """Initialize CIDStore and start ZMQ workers on startup."""
    global store, zmq_server, zmq_task

    logger.info("[Control Plane] Starting CIDStore initialization...")

    try:
        # Import here to avoid circular dependencies
        from .async_zmq_server import AsyncZMQServer
        from .storage import Storage
        from .store import CIDStore
        from .wal import WAL

        # Get HDF5 path from environment
        hdf5_path = os.environ.get("CIDSTORE_HDF5_PATH", "/data/cidstore.h5")
        logger.info(f"[Control Plane] Using HDF5 path: {hdf5_path}")

        # Create CIDStore instance (control plane owns it)
        storage = Storage(hdf5_path)
        wal = WAL(None)  # In-memory WAL
        store = CIDStore(
            storage, wal, testing=True
        )  # testing=True disables background maintenance

        logger.info("[Control Plane] CIDStore initialized successfully")

        # Create and start ZMQ server (data plane worker)
        zmq_server = AsyncZMQServer(store)
        zmq_task = asyncio.create_task(zmq_server.start())

        logger.info("[Control Plane] ZMQ workers started successfully")

    except Exception as ex:
        logger.error(f"[Control Plane] Startup failed: {ex}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Stop ZMQ workers and close CIDStore on shutdown."""
    global store, zmq_server, zmq_task

    logger.info("[Control Plane] Shutting down...")

    # Stop ZMQ server
    if zmq_server:
        try:
            await zmq_server.stop()
            logger.info("[Control Plane] ZMQ server stopped")
        except Exception as ex:
            logger.error(f"[Control Plane] Error stopping ZMQ server: {ex}")

    # Cancel ZMQ task
    if zmq_task:
        zmq_task.cancel()
        try:
            await zmq_task
        except asyncio.CancelledError:
            pass

    # Close CIDStore
    if store:
        try:
            await store.close()
            logger.info("[Control Plane] CIDStore closed")
        except Exception as ex:
            logger.error(f"[Control Plane] Error closing CIDStore: {ex}")

    logger.info("[Control Plane] Shutdown complete")


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


@app.get("/metrics/prometheus")
async def prometheus_metrics():
    if generate_latest is None:
        return PlainTextResponse("prometheus_client not installed", status_code=501)
    global store
    if store is None or not hasattr(store, "metrics_collector"):
        return PlainTextResponse("metrics not available", status_code=503)
    try:
        from prometheus_client import REGISTRY

        return PlainTextResponse(
            generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST
        )
    except Exception:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    global store
    if store is not None and hasattr(store, "metrics_collector"):
        metrics = store.metrics_collector.get_metrics()
        return "".join(f"cidstore_{k} {v}\n" for k, v in metrics.items())
    return "".join(f"cidstore_{k} {v}\n" for k, v in METRICS.items())


API_VERSION = "1.0"


def with_version(payload):
    if isinstance(payload, dict):
        payload = dict(payload)
        payload["version"] = API_VERSION
        return payload
    return payload


def error_response(code, msg):
    return {"error_code": code, "error_msg": msg, "version": API_VERSION}


@app.get("/config/promotion_threshold")
async def get_promotion_threshold():
    return with_version({"promotion_threshold": CONFIG["promotion_threshold"]})


@app.put("/config/promotion_threshold")
async def set_promotion_threshold(request: Request, data: dict):
    jwt_required(request)
    if "promotion_threshold" not in data:
        return error_response(400, "Missing promotion_threshold")
    CONFIG["promotion_threshold"] = int(data["promotion_threshold"])
    return with_version({"promotion_threshold": CONFIG["promotion_threshold"]})


@app.get("/config/batch_size")
async def get_batch_size():
    return with_version({"batch_size": CONFIG["batch_size"]})


@app.put("/config/batch_size")
async def set_batch_size(request: Request, data: dict):
    jwt_required(request)
    if "batch_size" not in data:
        return error_response(400, "Missing batch_size")
    CONFIG["batch_size"] = int(data["batch_size"])
    return with_version({"batch_size": CONFIG["batch_size"]})


@app.get("/debug/bucket/{bucket_id}")
async def debug_bucket(bucket_id: int, request: Request):
    internal_only(request)
    # TODO: Return real bucket metadata from store
    # 'ecc_state' was removed from the model; include a checksum field for validation consumers instead.
    return with_version({"bucket_id": bucket_id, "load": 0.5, "checksum": 0})


@app.get("/debug/get")
async def debug_get(high: int | None = None, low: int | None = None):
    """Debug endpoint: return values for a key specified by `high` and `low` 64-bit parts.

    Example: `/debug/get?high=123&low=456`
    """
    global store
    if store is None:
        return PlainTextResponse("store not initialized", status_code=503)
    if high is None or low is None:
        return error_response(400, "Missing high or low query parameter")
    try:
        from .keys import E

        key = E((int(high), int(low)))
        results = await store.get(key)
        # Serialize results as [[high, low], ...]
        serialized = []
        for r in results:
            try:
                serialized.append([int(r.high), int(r.low)])
            except Exception:
                if isinstance(r, (list, tuple)):
                    serialized.append(list(r))
                else:
                    serialized.append(r)
        return with_version({"results": serialized})
    except Exception as ex:
        return error_response(500, str(ex))


@app.post("/debug/delete")
async def debug_delete(data: dict):
    """Debug endpoint to delete a key or a specific value.

    Example JSON body: {"high": 100, "low": 200} or {"high":100,"low":200,"vhigh":300,"vlow":400}
    """
    global store
    if store is None:
        return PlainTextResponse("store not initialized", status_code=503)
    try:
        high = int(data.get("high"))
        low = int(data.get("low"))
    except Exception:
        return error_response(400, "Missing or invalid high/low")
    vhigh = data.get("vhigh")
    vlow = data.get("vlow")
    try:
        from .keys import E

        key = E((high, low))
        # Diagnostic print of incoming types
        print(
            "/debug/delete called with:", type(high), type(low), type(vhigh), type(vlow)
        )
        if vhigh is not None and vlow is not None:
            value = E((int(vhigh), int(vlow)))
            print("Calling store.delete_value with key=", key, "value=", value)
            await store.delete_value(key, value)
        else:
            print("Calling store.delete with key=", key)
            await store.delete(key)
        return with_version({"result": "ok"})
    except Exception as ex:
        import traceback

        tb = traceback.format_exc()
        # Print diagnostic info to stdout for the demo harness to capture
        print("/debug/delete error:")
        print("data:", repr(data))
        print("store:", repr(store))
        print(tb)
        return error_response(500, str(ex))
