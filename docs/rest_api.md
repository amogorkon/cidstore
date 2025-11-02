## CIDStore Control & Data-Plane Interface

This document is a handoff for an agent (or developer) building the CIDSem workload to integrate with CIDStore. It describes the REST control/monitoring API and the ZMQ/msgpack data plane (message formats, ports, examples, and operational notes).

Keep this file with the repository and share it with any integrations or test harnesses that need to talk to CIDStore.

---

## Overview

- Control & monitoring (control plane): FastAPI REST server, default port 8000.
- Data plane: ZeroMQ sockets with MessagePack-encoded messages for high-performance data operations. A compatibility REQ/REP endpoint is provided on port 5555 by default for simple clients and tests.

IMPORTANT: CIDSem must not rely on REST for bulk data operations. CIDSem should use ZMQ (push/req or dealer) with msgpack for inserts, reads, and bulk operations. REST is only for health, metrics, and light-weight control/configuration.

Defaults used in this repository:

- REST: http://<cidstore-host>:8000
- ZMQ compat REQ/REP: tcp://<cidstore-host>:5555
- ZMQ PUSH/PULL (mutations): tcp://<cidstore-host>:5557 (default in code uses loopback; see "Binding/Networking" section)
- ZMQ ROUTER/DEALER (reads): tcp://<cidstore-host>:5558
- ZMQ PUB/SUB (notifications): tcp://<cidstore-host>:5559

Environment variables that affect behavior (common):

- CIDSTORE_HDF5_PATH — path to HDF5 backing store (default: `/data/cidstore.h5`).
- CIDSTORE_ZMQ_ENDPOINT — compat REQ/REP endpoint (default: `tcp://0.0.0.0:5555`).
- CIDSTORE_ZMQ_IDLE_TIMEOUT — seconds of idle time after which ZMQ workers stop (default: `0` = never).
- CIDSTORE_PUSH_PULL_ADDR, CIDSTORE_ROUTER_DEALER_ADDR, CIDSTORE_PUB_SUB_ADDR — (recommended) configure PUSH/ROUTER/PUB addresses; if unset, code defaults may bind to loopback. See "Binding/Networking."

---

## REST Control API (monitoring & control)

The REST API is intended for health checks, metrics, runtime configuration, and debugging. It is not designed for bulk data operations (use ZMQ/msgpack for that).

Base server: FastAPI application in `src/cidstore/control_api.py`.

Health & readiness

- GET /health
  - Returns: 200 OK with plain text `ok` when control plane is running.

- GET /ready
  - Returns: 200 OK with plain text `ready` when service is ready.

Metrics

- GET /metrics/prometheus
  - Returns Prometheus text exposition format if `prometheus_client` is installed; otherwise returns 501.

- GET /metrics
  - Returns a simple text metrics listing (fallback dummy metrics if metrics collector unavailable).

Runtime configuration

- GET /config/promotion_threshold
  - Returns: {"promotion_threshold": <int>, "version": "1.0"}

- PUT /config/promotion_threshold
  - Body: JSON { "promotion_threshold": <int> }
  - Auth: Requires a JWT Authorization header (see Auth below)
  - Returns: {"promotion_threshold": <int>, "version": "1.0"}

- GET /config/batch_size
  - Returns: {"batch_size": <int>, "version": "1.0"}

- PUT /config/batch_size
  - Body: JSON { "batch_size": <int> }
  - Auth: JWT

Debug endpoints (internal-only)

- GET /debug/bucket/{bucket_id}
  - Internal-only: checks caller IP or X-Internal-Test header (or allow via env var in testing)
  - Returns bucket metadata for diagnostics (stubbed values in current code)

- GET /debug/get?high=<>&high_mid=<>&low_mid=<>&low=<>  (query by four 64-bit components)
  - Returns the serialized results from `store.get(...)` for the key composed of the four 64-bit components. For legacy clients, the endpoint may also accept `high` and `low` only (treated as 128-bit legacy format and zero-extended to 256 bits), but CIDSem should prefer the 4-part form.

-- POST /debug/delete
  - Body JSON:
    - Preferred 4-part form: {"high": <int>, "high_mid": <int>, "low_mid": <int>, "low": <int>} (and optionally `vhigh`/`vhigh_mid`/`vlow_mid`/`vlow` for value parts)
    - Legacy 2-part form still accepted: {"high": <int>, "low": <int>} (zero-extended)
  - Internal-only or auth protected in production.

Auth and internal-only checks

- The control API uses a simple JWT check helper `jwt_required(request)` in `src/cidstore/auth.py`. In the packaged demo it accepts any non-empty Bearer token; replace with real JWT verification in production.
- Internal-only endpoints check client IP and allow `X-Internal-Test: 1` header to facilitate testing.

Example: set batch size via curl

```bash
curl -X PUT http://localhost:8000/config/batch_size \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"batch_size": 5000}'
```

Example: reading metrics

```bash
curl http://localhost:8000/metrics
```

---

## ZMQ Data Plane (msgpack-encoded messages)

CIDStore implements a high-throughput data plane using ZeroMQ and MessagePack. The library provides multiple socket patterns for different semantics:

- Compat REQ/REP (default `CIDSTORE_ZMQ_ENDPOINT` — `tcp://0.0.0.0:5555`) — simple single-endpoint for tests and simple clients. MessagePack encoded request/response.
- PUSH/PULL (mutations queue) — single-writer pattern: producers PUSH mutations, CIDStore PULLs and applies them.
- ROUTER/DEALER (concurrent reads) — enables many concurrent readers with identity frames.
- PUB/SUB (notifications) — event/heartbeat notifications.

MessagePack conventions

- Use `msgpack.packb(obj, use_bin_type=True)` to serialize and `msgpack.unpackb(buf, raw=False)` to decode.
- All responses from the compat endpoint include a `version` field when returned as a dict (see `with_version`). Error responses follow `{"error_code": int, "error_msg": str, "version": "1.0"}`.

CID representation (SHA-256, 4×64-bit parts)

CIDStore uses full SHA-256 (256 bits) for content identifiers. These are represented as four 64-bit unsigned integers internally and in HDF5 storage. The canonical encodings accepted by the API are:

- String format: `"E(h,hm,lm,l)"` where `h`, `hm`, `lm`, `l` are the four 64-bit unsigned integer components of the 256-bit CID. Example: `"E(12345,67890,111213,141516)"`.
- List/Tuple: a 4-element list or tuple: `[high, high_mid, low_mid, low]`.
- JACK-style hexdigest string: any JACK hash string (e.g., `"j:abcd..."`) — the server will convert it to the E numeric form.

Notes on compatibility:
- The code retains legacy compatibility with 2-part (128-bit) CIDs in some places; these are zero-extended to 256-bit internally. However, integrations such as CIDSem should use the full 4-part format or the canonical JACK hexdigest string for unambiguous behavior.

Compatibility REQ/REP messages (recommended for the CIDSem agent)

Request examples (MessagePack payloads) — keys are examples; actual test harness in repo uses the same shapes:

- Batch insert

  Request structure:

  {
    "command": "batch_insert",
    "triples": [{"s": "E(1,2,3,4)", "p": "E(5,6,7,8)", "o": "E(9,10,11,12)"}, ...]
  }

  Response structure (success):

  {
    "status": "ok",
    "inserted": <int>,
    "version": "1.0"
  }

- Single insert

  Request:

  {
    "command": "insert",
    "s": "E(1,2,3,4)",
    "p": "E(5,6,7,8)",
    "o": "E(9,10,11,12)"
  }

  Response: {"status": "ok", "version": "1.0"}

- Query

  Request:

  {
    "command": "query",
    "s": "E(1,2,3,4)",
    "p": "E(5,6,7,8)",
    "o": "E(9,10,11,12)"  # optional
  }

  Response (success): {"results": [...], "version": "1.0"}

Server-side compat behavior

- The compat worker unpacks incoming msgpack messages and maps `command` or `op_code` to behavior.
- For `batch_insert`, it converts triples into the format expected by `store.insert_triples_batch` and returns inserted count.
- For `insert` it will call `store.insert(key, value)`.

REQ/REP client example (Python)

```python
import zmq, msgpack

ctx = zmq.Context()
sock = ctx.socket(zmq.REQ)
sock.connect('tcp://127.0.0.1:5555')

msg = {
    'command': 'batch_insert',
    'triples': [
      {'s': 'E(1,2,3,4)', 'p': 'E(5,6,7,8)', 'o': 'E(9,10,11,12)'},
    ]
}
sock.send(msgpack.packb(msg, use_bin_type=True))
resp = msgpack.unpackb(sock.recv(), raw=False)
print(resp)
```

PUSH/PULL mutation queue

- Mutations may also be submitted via PUSH to the mutations queue:

  - PUSH address default: `tcp://127.0.0.1:5557` in code (consider configuring to 0.0.0.0 for external access).
  - Mutations are msgpack objects with an `op_code` field: `INSERT`, `DELETE`, `BATCH_INSERT`, `BATCH_DELETE`.

  Example mutation (single insert):

  {
    "op_code": "INSERT",
    "key": "E(1,2,3,4)",
    "value": "E(5,6,7,8)"
  }

  Example batch mutation:

  {
    "op_code": "BATCH_INSERT",
    "entries": [{"key": "E(1,2,3,4)", "value": "E(5,6,7,8)"}, ...]
  }

Router/Dealer reads

- Address: default `tcp://127.0.0.1:5558` in code (see binding note)
- Client sends multipart frames: identity managed by ROUTER/DEALER; the server expects the 3rd frame to be the msgpack payload and replies with a multipart response [id, b"", packed_msg].
- Supported op_codes include `LOOKUP` and `METRICS`.

PUB/SUB notifications

- Address: default `tcp://127.0.0.1:5559` (see binding note)
- Notification format: `{"type": <string>, "args": [...]} ` packed with msgpack.
- A heartbeat note: `{"type": "heartbeat"}` is published every second while server is running.

Binding / networking note (important)

- The compat REQ/REP endpoint defaults to `0.0.0.0:5555` and is therefore reachable from other containers and hosts when the port is published.
- In the current code, PUSH/ROUTER/PUB addresses default to `127.0.0.1` — this restricts them to the container's loopback interface. If you need other containers (or remote processes) to connect directly to these sockets, bind them to `0.0.0.0` or an external interface. We recommend setting these addresses via env vars:

- CIDSTORE_PUSH_PULL_ADDR (e.g. `tcp://0.0.0.0:5557`)
- CIDSTORE_ROUTER_DEALER_ADDR (e.g. `tcp://0.0.0.0:5558`)
- CIDSTORE_PUB_SUB_ADDR (e.g. `tcp://0.0.0.0:5559`)

If you leave them bound to loopback but expose the ports at container runtime, Docker will publish them on the host — however, cross-container communication inside a user-defined Docker network usually expects binding to 0.0.0.0.

Security & production notes

- The JWT helper in `src/cidstore/auth.py` is a demo stub. Replace `verify_jwt` with proper JWT verification (PyJWT or other) and secure secrets via environment variables.
- Use network-level controls or mTLS or separate internal networks for ZMQ endpoints if you require strong isolation.

Docker / docker-compose snippets

Example `docker-compose.yml` service fragment (for an external test harness container to reach ZMQ & REST):

```yaml
services:
  cidstore:
    image: cidstore:latest
    ports:
      - 8000:8000    # REST
      - 5555:5555    # compat REQ/REP
      - 5557:5557    # PUSH/PULL (mutations) - only needed if external producers push directly
      - 5558:5558    # ROUTER/DEALER (reads)
      - 5559:5559    # PUB/SUB (notifications)
    environment:
      - CIDSTORE_HDF5_PATH=/data/cidstore.h5
      - CIDSTORE_ZMQ_ENDPOINT=tcp://0.0.0.0:5555
      - CIDSTORE_PUSH_PULL_ADDR=tcp://0.0.0.0:5557
      - CIDSTORE_ROUTER_DEALER_ADDR=tcp://0.0.0.0:5558
      - CIDSTORE_PUB_SUB_ADDR=tcp://0.0.0.0:5559

  cidsem-test:
    build: ./tests/cidsem_mockup
    depends_on:
      - cidstore

```

Operational recommendations

1. Use the compat REQ/REP `tcp://...:5555` endpoint for test harnesses and simple agents such as CIDSem mockups — it is easiest to use and already used by tests in this repo.
2. For maximal throughput in production, use the PUSH/PULL mutation queue and send pre-packed batches via msgpack.
3. Always use `use_bin_type=True` when packing and `raw=False` when unpacking to ensure stable handling of binary vs text types.
4. Monitor REST `/metrics` and consider installing `prometheus_client` for richer metrics at `/metrics/prometheus`.

Examples & troubleshooting

- If a test fails to connect to CIDStore's REST API, verify the container is up and the `/health` endpoint returns `ok`.
- If ZMQ connections are refused, verify the endpoint bindings and Docker port mappings. If sockets are bound to `127.0.0.1` inside the container, other containers cannot connect unless you change the binding to `0.0.0.0` or publish the port to the host and connect via host networking.

CIDSem integration checklist (quick handoff)

1. Health & readiness
   - Poll `http://<cidstore-host>:8000/health` until it returns `ok` before starting heavy operations.

2. Inserts (recommended path for tests)
   - Use REQ/REP to `tcp://<cidstore-host>:5555` and send a `batch_insert` msgpack message. Wait for `{"status":"ok"}` responses.

3. High-throughput inserts (production)
   - Use PUSH to `tcp://<cidstore-host>:5557` (or configured addr) and send `op_code: "BATCH_INSERT"` messages with `entries` array.

4. Queries
   - Use REQ/REP `query` command or ROUTER/DEALER for concurrent queries.

5. Notifications
   - Subscribe to PUB socket for heartbeat and event notifications if you need to react to store events.

6. Config/monitoring
   - Use REST `/config/*` endpoints to tune parameters at runtime and `/metrics` to collect metrics.

Contact / handoff

Share this file with the CIDSem agent author. If they need runnable examples, point them to `tests/cidsem_mockup/cidsem_test.py` for a working msgpack + zmq client implementation used for the system test.

---

End of document.
