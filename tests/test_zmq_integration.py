import msgpack
import pytest
import zmq
import zmq.asyncio

pytestmark = pytest.mark.asyncio


@pytest.mark.asyncio
async def test_zmq_push_pull():
    ctx = zmq.asyncio.Context()
    # Assume server is running on localhost:5555 (PUSH/PULL)
    pull = ctx.socket(zmq.PULL)
    pull.bind("tcp://127.0.0.1:5555")
    push = ctx.socket(zmq.PUSH)
    push.connect("tcp://127.0.0.1:5555")
    msg = {"op": "insert", "key": b"foo", "value": b"bar"}
    await push.send(msgpack.packb(msg))
    data = await pull.recv()
    unpacked = msgpack.unpackb(data)
    assert unpacked["op"] == b"insert" or unpacked["op"] == "insert"
    assert unpacked["key"] == b"foo"
    assert unpacked["value"] == b"bar"
    push.close()
    pull.close()
    ctx.term()


@pytest.mark.asyncio
async def test_zmq_router_dealer():
    ctx = zmq.asyncio.Context()
    router = ctx.socket(zmq.ROUTER)
    router.bind("tcp://127.0.0.1:5557")
    dealer = ctx.socket(zmq.DEALER)
    dealer.connect("tcp://127.0.0.1:5557")
    # Dealer sends request
    req = {"op": "get", "key": b"foo"}
    await dealer.send(msgpack.packb(req))
    ident, msg = await router.recv_multipart()
    unpacked = msgpack.unpackb(msg)
    assert unpacked["op"] == b"get" or unpacked["op"] == "get"
    assert unpacked["key"] == b"foo"
    # Router replies
    reply = {"status": "ok", "value": b"bar"}
    await router.send_multipart([ident, msgpack.packb(reply)])
    data = await dealer.recv()
    unpacked = msgpack.unpackb(data)
    assert unpacked["status"] == "ok"
    assert unpacked["value"] == b"bar"
    dealer.close()
    router.close()
    ctx.term()
