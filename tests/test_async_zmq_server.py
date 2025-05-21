
import asyncio
import sys
if sys.platform.startswith("win"):
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import msgpack
import zmq
import zmq.asyncio

from cidstore.async_zmq_server import AsyncZMQServer


class DummyStore:
    def __init__(self):
        self.data = {}

    async def insert(self, key, value):
        self.data[tuple(key)] = value

    async def lookup(self, key):
        return [self.data.get(tuple(key))] if tuple(key) in self.data else []

    async def delete(self, key, value=None):
        self.data.pop(tuple(key), None)

    async def batch_insert(self, entries):
        for entry in entries:
            await self.insert(entry["key"], entry["value"])

    async def batch_delete(self, entries):
        for entry in entries:
            await self.delete(entry["key"], entry.get("value"))

    async def metrics(self):
        return {
            "latency_p99": 0.1,
            "throughput_ops": 1000,
            "buffer_occupancy": 0,
            "flush_duration": 0.01,
            "lock_contention_ratio": 0.0,
            "error_rate": 0.0,
        }


async def run_test():
    store = DummyStore()
    server = AsyncZMQServer(store)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.2)  # Let server start

    ctx = zmq.asyncio.Context.instance()
    # Test mutation (INSERT)
    push = ctx.socket(zmq.PUSH)
    push.connect("tcp://127.0.0.1:5557")
    insert_msg = {
        "op_code": "INSERT",
        "key": [1, 2],
        "value": [3, 4],
    }

    try:
        await asyncio.wait_for(push.send(msgpack.packb(insert_msg, use_bin_type=True)), timeout=2)
    except asyncio.TimeoutError:
        print("Timeout during PUSH send")
        await server.stop()
        server_task.cancel()
        return
    await asyncio.sleep(0.2)

    # Test read (LOOKUP)
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect("tcp://127.0.0.1:5558")
    lookup_msg = {
        "op_code": "LOOKUP",
        "key": [1, 2],
    }

    try:
        await asyncio.wait_for(dealer.send_multipart([b"", msgpack.packb(lookup_msg, use_bin_type=True)]), timeout=2)
        ident, resp = await asyncio.wait_for(dealer.recv_multipart(), timeout=2)
        result = msgpack.unpackb(resp, raw=False)
        print("LOOKUP response:", result)
    except asyncio.TimeoutError:
        print("Timeout during DEALER send/recv")
    

    # Shutdown
    await server.stop()
    try:
        await asyncio.wait_for(server_task, timeout=2)
    except asyncio.CancelledError:
        pass
    except asyncio.TimeoutError:
        server_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(asyncio.wait_for(run_test(), timeout=5))
    except asyncio.TimeoutError:
        print("Test timed out after 5 seconds.")
