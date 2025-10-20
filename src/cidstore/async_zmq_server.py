"""
Async ZMQ Server for CIDStore Microservice

Implements:
- PUSH/PULL for single-writer mutation queue
- ROUTER/DEALER for concurrent reads
- (Optional) PUB/SUB for notifications
- MessagePack serialization for all messages

Requires: pyzmq[asyncio], msgpack, asyncio

See: docs/spec 2 - Data Types and Structure.md for message schema
"""

import asyncio
import logging
import os

import msgpack
import zmq
import zmq.asyncio
import os

from cidstore.keys import E

# Placeholder import for AsyncCIDStore
# from src.cidstore.store import AsyncCIDStore

# ZMQ socket addresses (can be made configurable)
PUSH_PULL_ADDR = "tcp://127.0.0.1:5557"  # For mutations
ROUTER_DEALER_ADDR = "tcp://127.0.0.1:5558"  # For reads
PUB_SUB_ADDR = "tcp://127.0.0.1:5559"  # For notifications (optional)


API_VERSION = "1.0"


def with_version(payload):
    if isinstance(payload, dict):
        payload = dict(payload)
        payload["version"] = API_VERSION
        return payload
    return payload


def error_response(code, msg):
    return {"error_code": code, "error_msg": msg, "version": API_VERSION}


logger = logging.getLogger(__name__)


class AsyncZMQServer:
    async def wal_checkpoint_worker(self):
        """Periodically triggers WAL checkpointing (every 5s)."""
        while self.running:
            try:
                if hasattr(self.store, "wal_checkpoint"):
                    await self.store.wal_checkpoint()
            except Exception as ex:
                logger.error(f"[BG] WAL checkpoint error: {ex}")
            await asyncio.sleep(5)

    async def compaction_worker(self):
        """Periodically triggers compaction/merge/GC (every 10s)."""
        while self.running:
            try:
                if hasattr(self.store, "compact"):
                    await self.store.compact()
                if hasattr(self.store, "merge"):
                    await self.store.merge()
                if hasattr(self.store, "gc"):
                    await self.store.gc()
            except Exception as ex:
                logger.error(f"[BG] Compaction/Merge/GC error: {ex}")
            await asyncio.sleep(10)

    async def metrics_worker(self):
        """Periodically collects metrics and triggers auto-tuning (every 2s)."""
        while self.running:
            try:
                if hasattr(self.store, "metrics"):
                    metrics = await self.store.metrics()
                    # Optionally, implement auto-tuning logic here
                    if hasattr(self.store, "auto_tune"):
                        await self.store.auto_tune(metrics)
            except Exception as ex:
                logger.error(f"[BG] Metrics/Auto-tune error: {ex}")
            await asyncio.sleep(2)

    def __init__(self, store):
        self.store = store
        self.ctx = zmq.asyncio.Context.instance()
        self.running = False
        # Idle timeout for ZMQ sockets in seconds.
        # If 0 (default), do not stop the server on inactivity.
        try:
            self.zmq_idle_timeout = int(os.environ.get("CIDSTORE_ZMQ_IDLE_TIMEOUT", "0"))
        except Exception:
            self.zmq_idle_timeout = 0
        # Compatibility endpoint for REQ/REP-based clients (cidsem mockup)
        # Default: tcp://0.0.0.0:5555
        self.zmq_endpoint = os.environ.get("CIDSTORE_ZMQ_ENDPOINT", "tcp://0.0.0.0:5555")

    async def mutation_worker(self):
        """Single-writer: consumes mutations from PUSH socket and applies to store.
        If `CIDSTORE_ZMQ_IDLE_TIMEOUT` is > 0, the worker will stop after that
        many seconds of inactivity. If 0 (default), the worker will not stop
        automatically."""
        pull_sock = self.ctx.socket(zmq.PULL)
        pull_sock.bind(PUSH_PULL_ADDR)
        while self.running:
            try:
                if self.zmq_idle_timeout and self.zmq_idle_timeout > 0:
                    msg = await asyncio.wait_for(
                        pull_sock.recv(), timeout=self.zmq_idle_timeout
                    )
                else:
                    # No timeout configured: wait indefinitely
                    msg = await pull_sock.recv()
            except asyncio.TimeoutError:
                logger.info(
                    f"[ZMQ] No mutation received for {self.zmq_idle_timeout}s, stopping server."
                )
                self.running = False
                break
            try:
                req = msgpack.unpackb(msg, raw=False)
                op = req.get("op_code")
                # Validate op_code and fields
                resp = None
                if op == "INSERT":
                    key = self._parse_cid(req["key"])
                    value = self._parse_cid(req["value"])
                    await self.store.insert(key, value)
                    await self._publish_notification("insert", key, value)
                    resp = with_version({"result": "ok"})
                elif op == "DELETE":
                    key = self._parse_cid(req["key"])
                    value = (
                        self._parse_cid(req.get("value"))
                        if req.get("value") is not None
                        else None
                    )
                    await self.store.delete(key, value)
                    await self._publish_notification("delete", key, value)
                    resp = with_version({"result": "ok"})
                elif op == "BATCH_INSERT":
                    entries = [
                        {
                            "key": self._parse_cid(e["key"]),
                            "value": self._parse_cid(e["value"]),
                        }
                        for e in req["entries"]
                    ]
                    await self.store.batch_insert(entries)
                    await self._publish_notification("batch_insert", entries)
                    resp = with_version({"result": "ok"})
                elif op == "BATCH_DELETE":
                    entries = [
                        {
                            "key": self._parse_cid(e["key"]),
                            "value": self._parse_cid(e.get("value"))
                            if e.get("value") is not None
                            else None,
                        }
                        for e in req["entries"]
                    ]
                    await self.store.batch_delete(entries)
                    await self._publish_notification("batch_delete", entries)
                    resp = with_version({"result": "ok"})
                else:
                    resp = error_response(400, f"Unknown mutation op_code: {op}")
                if resp is not None:
                    # Optionally send response on a reply socket if needed
                    pass
            except Exception as ex:
                logger.error(f"[ZMQ] Error processing mutation: {ex}")
                # Optionally send error response

    async def compat_worker(self):
        """Compatibility worker for REQ/REP clients.

        Many simple test clients (including the CIDSem mockup) use a REQ
        socket to a single endpoint. This worker binds a REP socket to
        `CIDSTORE_ZMQ_ENDPOINT` and handles common commands such as
        `batch_insert`, `insert`, and `query`.
        """
        rep_sock = self.ctx.socket(zmq.REP)
        rep_sock.bind(self.zmq_endpoint)
        while self.running:
            try:
                msg = await rep_sock.recv()
            except Exception as ex:
                logger.error(f"[ZMQ] compat_worker recv error: {ex}")
                break
            try:
                req = msgpack.unpackb(msg, raw=False)
                cmd = req.get("command") or req.get("op_code")
                if cmd in ("batch_insert", "BATCH_INSERT"):
                    triples = req.get("triples", [])
                    # Use CIDStore.insert_triples_batch for efficient bulk insertion
                    try:
                        # Convert from dict format to tuple format: (subject, predicate, object)
                        triple_list = [
                            (self._parse_cid(t.get("s")), 
                             self._parse_cid(t.get("p")),
                             self._parse_cid(t.get("o")))
                            for t in triples
                        ]
                        result = await self.store.insert_triples_batch(triple_list, atomic=False)
                        resp = with_version({"status": "ok", "inserted": result.get("inserted", 0)})
                    except Exception as ex:
                        logger.error(f"[ZMQ] batch_insert error: {ex}")
                        resp = error_response(500, str(ex))
                elif cmd in ("insert", "INSERT"):
                    # Single insert: expect s,p,o
                    try:
                        key = self._parse_cid(req.get("s") or req.get("key"))
                        value = self._parse_cid(req.get("o") or req.get("value"))
                        await self.store.insert(key, value)
                        resp = with_version({"status": "ok"})
                    except Exception as ex:
                        resp = error_response(500, str(ex))
                elif cmd in ("query", "LOOKUP"):
                    try:
                        key = self._parse_cid(req.get("s") or req.get("key"))
                        # Use store.lookup or store.query as available
                        if hasattr(self.store, "lookup"):
                            results = await self.store.lookup(key)
                            resp = with_version({"results": results})
                        else:
                            resp = error_response(501, "lookup not implemented")
                    except Exception as ex:
                        resp = error_response(500, str(ex))
                else:
                    resp = error_response(400, f"Unknown command: {cmd}")
            except Exception as ex:
                resp = error_response(500, str(ex))

            try:
                await rep_sock.send(msgpack.packb(resp, use_bin_type=True))
            except Exception as ex:
                logger.error(f"[ZMQ] compat_worker send error: {ex}")

    async def read_worker(self):
        """Handles concurrent read requests via ROUTER/DEALER.
        If `CIDSTORE_ZMQ_IDLE_TIMEOUT` is > 0, the worker will stop after that
        many seconds of inactivity. If 0 (default), the worker will not stop
        automatically."""
        router_sock = self.ctx.socket(zmq.ROUTER)
        router_sock.bind(ROUTER_DEALER_ADDR)
        while self.running:
            try:
                if self.zmq_idle_timeout and self.zmq_idle_timeout > 0:
                    ident, empty, msg = await asyncio.wait_for(
                        router_sock.recv_multipart(), timeout=self.zmq_idle_timeout
                    )
                else:
                    ident, empty, msg = await router_sock.recv_multipart()
            except asyncio.TimeoutError:
                logger.info(
                    f"[ZMQ] No read received for {self.zmq_idle_timeout}s, stopping server."
                )
                self.running = False
                break
            try:
                req = msgpack.unpackb(msg, raw=False)
                op = req.get("op_code")
                if op == "LOOKUP":
                    key = self._parse_cid(req["key"])
                    results = await self.store.lookup(key)
                    resp = with_version({
                        "results": [
                            list(r) if isinstance(r, tuple) else r for r in results
                        ]
                    })
                elif op == "METRICS":
                    metrics = await self.store.metrics()
                    resp = with_version(metrics)
                else:
                    resp = error_response(400, "Unknown op_code")
            except Exception as ex:
                resp = error_response(500, str(ex))
            await router_sock.send_multipart([
                ident,
                b"",
                msgpack.packb(resp, use_bin_type=True),
            ])

    def _parse_cid(self, cid):
        """Convert a CID from string/list/tuple to E instance, or None."""
        if cid is None:
            return None
        # Handle string format like "E(12345,67890)"
        if isinstance(cid, str):
            if cid.startswith("E(") and cid.endswith(")"):
                # Extract the numbers from "E(high,low)"
                inner = cid[2:-1]  # Remove "E(" and ")"
                parts = inner.split(",")
                if len(parts) == 2:
                    high = int(parts[0])
                    low = int(parts[1])
                    return E([high, low])  # E constructor handles [high, low] format
                else:
                    raise ValueError(f"Invalid CID format (expected 2 parts): {cid}")
            else:
                # Try to parse as a JACK hash string
                return E(cid)
        # Handle list/tuple format
        if isinstance(cid, (list, tuple)) and len(cid) == 2:
            return E([int(cid[0]), int(cid[1])])
        raise ValueError(f"Invalid CID format: {cid}")

    async def _publish_notification(self, event, *args):
        """Publish a notification event on the PUB socket if enabled."""
        if hasattr(self, "pub_sock") and self.pub_sock is not None:
            note = {"type": event, "args": args}
            try:
                await self.pub_sock.send(msgpack.packb(note, use_bin_type=True))
            except Exception as ex:
                logger.error(f"[ZMQ] Failed to publish notification: {ex}")

    async def notification_worker(self):
        """Publishes heartbeat notifications on PUB socket every 1s and enables data notifications."""
        self.pub_sock = self.ctx.socket(zmq.PUB)
        self.pub_sock.bind(PUB_SUB_ADDR)
        while self.running:
            await asyncio.sleep(1)
            note = {"type": "heartbeat"}
            try:
                await self.pub_sock.send(msgpack.packb(note, use_bin_type=True))
            except Exception as ex:
                logger.error(f"[ZMQ] Failed to publish heartbeat: {ex}")
        self.pub_sock.close()

    async def start(self):
        self.running = True
        # Always start core worker tasks
        self._tasks = [
            asyncio.create_task(self.mutation_worker()),
            asyncio.create_task(self.read_worker()),
            asyncio.create_task(self.compat_worker()),
            asyncio.create_task(self.notification_worker()),
        ]
        # Only start maintenance workers if not in testing/debugging mode
        # CIDStore stores the testing flag as 'debugging'
        if not getattr(self.store, "debugging", False):
            self._tasks.extend([
                asyncio.create_task(self.wal_checkpoint_worker()),
                asyncio.create_task(self.compaction_worker()),
                asyncio.create_task(self.metrics_worker()),
            ])
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        self.running = False
        # Cancel all running tasks to ensure prompt shutdown
        if hasattr(self, "_tasks"):
            for t in self._tasks:
                t.cancel()
            await asyncio.sleep(0.1)
        self.ctx.term()


# Example usage (for testing only)
if __name__ == "__main__":

    async def main():
        # Create a real CIDStore instance for the ZMQ server
        from cidstore.storage import Storage
        from cidstore.wal import WAL
        from cidstore.store import CIDStore
        
        # Use HDF5 path from environment or default to Docker path
        hdf5_path = os.environ.get("CIDSTORE_HDF5_PATH", "/data/cidstore.h5")
        
        logger.info(f"[ZMQ] Initializing CIDStore with HDF5 path: {hdf5_path}")
        
        storage = Storage(hdf5_path)
        wal = WAL(None)  # In-memory WAL for simplicity
        store = CIDStore(storage, wal, testing=True)  # Use testing=True to disable background maintenance
        
        logger.info("[ZMQ] CIDStore initialized, starting ZMQ server")
        
        server = AsyncZMQServer(store)
        await server.start()

    asyncio.run(main())
