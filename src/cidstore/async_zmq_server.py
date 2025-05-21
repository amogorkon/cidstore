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

import msgpack
import zmq
import zmq.asyncio

# Placeholder import for AsyncCIDStore
# from src.cidstore.store import AsyncCIDStore

# ZMQ socket addresses (can be made configurable)
PUSH_PULL_ADDR = "tcp://127.0.0.1:5557"  # For mutations
ROUTER_DEALER_ADDR = "tcp://127.0.0.1:5558"  # For reads
PUB_SUB_ADDR = "tcp://127.0.0.1:5559"  # For notifications (optional)


class AsyncZMQServer:
    async def wal_checkpoint_worker(self):
        """Periodically triggers WAL checkpointing (every 5s)."""
        while self.running:
            try:
                if hasattr(self.store, 'wal_checkpoint'):
                    await self.store.wal_checkpoint()
            except Exception as ex:
                print(f"[BG] WAL checkpoint error: {ex}")
            await asyncio.sleep(5)

    async def compaction_worker(self):
        """Periodically triggers compaction/merge/GC (every 10s)."""
        while self.running:
            try:
                if hasattr(self.store, 'compact'):
                    await self.store.compact()
                if hasattr(self.store, 'merge'):
                    await self.store.merge()
                if hasattr(self.store, 'gc'):
                    await self.store.gc()
            except Exception as ex:
                print(f"[BG] Compaction/Merge/GC error: {ex}")
            await asyncio.sleep(10)

    async def metrics_worker(self):
        """Periodically collects metrics and triggers auto-tuning (every 2s)."""
        while self.running:
            try:
                if hasattr(self.store, 'metrics'):
                    metrics = await self.store.metrics()
                    # Optionally, implement auto-tuning logic here
                    if hasattr(self.store, 'auto_tune'):
                        await self.store.auto_tune(metrics)
            except Exception as ex:
                print(f"[BG] Metrics/Auto-tune error: {ex}")
            await asyncio.sleep(2)
    def __init__(self, store):
        self.store = store
        self.ctx = zmq.asyncio.Context.instance()
        self.running = False

    async def mutation_worker(self):
        """Single-writer: consumes mutations from PUSH socket and applies to store. Stops after 2s inactivity."""
        pull_sock = self.ctx.socket(zmq.PULL)
        pull_sock.bind(PUSH_PULL_ADDR)
        while self.running:
            try:
                msg = await asyncio.wait_for(pull_sock.recv(), timeout=2)
            except asyncio.TimeoutError:
                print("[ZMQ] No mutation received for 2s, stopping server.")
                self.running = False
                break
            try:
                req = msgpack.unpackb(msg, raw=False)
                op = req.get("op_code")
                # Validate op_code and fields
                if op == "INSERT":
                    key = self._parse_cid(req["key"])
                    value = self._parse_cid(req["value"])
                    await self.store.insert(key, value)
                    await self._publish_notification('insert', key, value)
                elif op == "DELETE":
                    key = self._parse_cid(req["key"])
                    value = self._parse_cid(req.get("value")) if req.get("value") is not None else None
                    await self.store.delete(key, value)
                    await self._publish_notification('delete', key, value)
                elif op == "BATCH_INSERT":
                    entries = [
                        {"key": self._parse_cid(e["key"]), "value": self._parse_cid(e["value"])}
                        for e in req["entries"]
                    ]
                    await self.store.batch_insert(entries)
                    await self._publish_notification('batch_insert', entries)
                elif op == "BATCH_DELETE":
                    entries = [
                        {"key": self._parse_cid(e["key"]), "value": self._parse_cid(e.get("value")) if e.get("value") is not None else None}
                        for e in req["entries"]
                    ]
                    await self.store.batch_delete(entries)
                    await self._publish_notification('batch_delete', entries)
                else:
                    print(f"[ZMQ] Unknown mutation op_code: {op}")
            except Exception as ex:
                print(f"[ZMQ] Error processing mutation: {ex}")

    async def read_worker(self):
        """Handles concurrent read requests via ROUTER/DEALER. Stops after 2s inactivity."""
        router_sock = self.ctx.socket(zmq.ROUTER)
        router_sock.bind(ROUTER_DEALER_ADDR)
        while self.running:
            try:
                ident, empty, msg = await asyncio.wait_for(router_sock.recv_multipart(), timeout=2)
            except asyncio.TimeoutError:
                print("[ZMQ] No read received for 2s, stopping server.")
                self.running = False
                break
            try:
                req = msgpack.unpackb(msg, raw=False)
                op = req.get("op_code")
                if op == "LOOKUP":
                    key = self._parse_cid(req["key"])
                    results = await self.store.lookup(key)
                    resp = {"results": [list(r) if isinstance(r, tuple) else r for r in results]}
                elif op == "METRICS":
                    metrics = await self.store.metrics()
                    resp = metrics
                else:
                    resp = {"error_code": 400, "error_msg": "Unknown op_code"}
            except Exception as ex:
                resp = {"error_code": 500, "error_msg": str(ex)}
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
        if hasattr(self, 'pub_sock') and self.pub_sock is not None:
            note = {"type": event, "args": args}
            try:
                await self.pub_sock.send(msgpack.packb(note, use_bin_type=True))
            except Exception as ex:
                print(f"[ZMQ] Failed to publish notification: {ex}")

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
                print(f"[ZMQ] Failed to publish heartbeat: {ex}")
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
        if hasattr(self, '_tasks'):
            for t in self._tasks:
                t.cancel()
            await asyncio.sleep(0.1)
        self.ctx.term()


# Example usage (for testing only)
if __name__ == "__main__":

    async def main():
        # store = AsyncCIDStore(...)
        store = None  # Replace with actual store
        server = AsyncZMQServer(store)
        await server.start()

    asyncio.run(main())
