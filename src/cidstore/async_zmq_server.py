"""
Async ZMQ Server for CIDStore Microservice

Implements:
- PUSH/PULL for single-writer mutation queue
- ROUTER/DEALER for concurrent reads
- (Optional) PUB/SUB for notifications
- MessagePack serialization for all messages

See: spec 2 - Data Types and Structure.md for message schema
"""

import asyncio
import logging

import msgpack
import zmq
import zmq.asyncio

from .keys import E

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
        assert hasattr(self.store, "wal_checkpoint"), (
            "Store must implement 'wal_checkpoint' method."
        )
        while self.running:
            try:
                await self.store.wal_checkpoint()
            except Exception as ex:
                logger.error(f"[BG] WAL checkpoint error: {ex}")
            await asyncio.sleep(5)

    async def compaction_worker(self):
        """Periodically triggers compaction/merge/GC (every 10s)."""
        assert hasattr(self.store, "compact"), "Store must implement 'compact' method."
        while self.running:
            try:
                await self.store.compact()
                await self.store.merge()
                await self.store.gc()
            except Exception as ex:
                logger.error(f"[BG] Compaction/Merge/GC error: {ex}")
            await asyncio.sleep(10)

    async def metrics_worker(self):
        """Periodically collects metrics and triggers auto-tuning (every 2s)."""
        auto_tune = getattr(self.store, "auto_tune", None)
        while self.running:
            try:
                metrics = await self.store.metrics()
                # Optionally, implement auto-tuning logic here
                if auto_tune is not None:
                    await auto_tune(metrics)
            except Exception as ex:
                logger.error(f"[BG] Metrics/Auto-tune error: {ex}")
            await asyncio.sleep(2)

    def __init__(self, store):
        self.store = store
        self.ctx = zmq.asyncio.Context.instance()
        self.running = False

    async def mutation_worker(self):
        """Single-writer: consumes mutations from PUSH socket and applies to store. Runs indefinitely."""
        assert hasattr(self.store, "insert"), "Store must implement 'insert' method."
        assert hasattr(self.store, "delete"), "Store must implement 'delete' method."
        pull_sock = self.ctx.socket(zmq.PULL)
        pull_sock.bind(PUSH_PULL_ADDR)
        while self.running:
            msg = await pull_sock.recv()
            try:
                req = msgpack.unpackb(msg, raw=False)
                op = req.get("op_code")
                # Validate op_code and fields
                resp = None
                if op == "INSERT":
                    key_cid = self._parse_cid(req["key"])
                    value_cid = self._parse_cid(req["value"])
                    # Convert to E objects expected by the store
                    try:
                        key_obj = E(key_cid)
                        value_obj = E(value_cid)
                    except Exception:
                        # Fallback: if already proper object, use it
                        key_obj = key_cid
                        value_obj = value_cid
                    await self.store.insert(key_obj, value_obj)
                    await self._publish_notification("insert", key_obj, value_obj)
                    resp = with_version({"result": "ok"})
                elif op == "DELETE":
                    key_cid = self._parse_cid(req["key"])
                    value_cid = (
                        self._parse_cid(req.get("value"))
                        if req.get("value") is not None
                        else None
                    )
                    try:
                        key_obj = E(key_cid)
                    except Exception:
                        key_obj = key_cid
                    if value_cid is not None:
                        try:
                            value_obj = E(value_cid)
                        except Exception:
                            value_obj = value_cid
                        # Delete a specific value
                        await self.store.delete_value(key_obj, value_obj)
                    else:
                        # Delete all values for the key
                        await self.store.delete(key_obj)
                    await self._publish_notification("delete", key_obj, value_cid)
                    resp = with_version({"result": "ok"})
                else:
                    resp = error_response(400, f"Unknown mutation op_code: {op}")
            except Exception as ex:
                logger.error(f"[ZMQ] Error processing mutation: {ex}")
                # Optionally send error response

    async def read_worker(self):
        """Handles concurrent read requests via ROUTER/DEALER. Runs indefinitely."""
        assert hasattr(self.store, "get"), "Store must implement 'get' method."
        router_sock = self.ctx.socket(zmq.ROUTER)
        router_sock.bind(ROUTER_DEALER_ADDR)
        while self.running:
            parts = await router_sock.recv_multipart()
            assert len(parts) == 2
            ident, msg = parts

            try:
                req = msgpack.unpackb(msg, raw=False)
                op = req.get("op_code")
                if op == "LOOKUP":
                    key_cid = self._parse_cid(req["key"])
                    try:
                        key_obj = E(key_cid)
                    except Exception:
                        key_obj = key_cid
                    results = await self.store.get(key_obj)
                    # results is a list of E objects — serialize to [high, low]
                    serialized = []
                    for r in results:
                        try:
                            serialized.append([int(r.high), int(r.low)])
                        except Exception:
                            # If r is tuple/list already
                            if isinstance(r, (list, tuple)):
                                serialized.append(list(r))
                            else:
                                serialized.append(r)
                    resp = with_version({"results": serialized})
                elif op == "METRICS":
                    try:
                        metrics = await self.store.metrics()
                    except Exception:
                        metrics = {}
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
        """Convert a CID from list or tuple to tuple of two ints, or None."""
        if cid is None:
            return None
        if isinstance(cid, (list, tuple)) and len(cid) == 2:
            return (int(cid[0]), int(cid[1]))
        raise ValueError(f"Invalid CID format: {cid}")

    async def _publish_notification(self, event, *args):
        """Publish a notification event on the PUB socket if enabled."""
        assert hasattr(self, "pub_sock"), (
            "AsyncZMQServer must have 'pub_sock' attribute."
        )
        if self.pub_sock is not None:
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
        self._tasks = [
            asyncio.create_task(self.mutation_worker()),
            asyncio.create_task(self.read_worker()),
            asyncio.create_task(self.notification_worker()),
            asyncio.create_task(self.wal_checkpoint_worker()),
            asyncio.create_task(self.compaction_worker()),
            asyncio.create_task(self.metrics_worker()),
        ]
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
