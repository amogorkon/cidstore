import contextlib
import os
import time

import msgpack
import requests
import zmq

from cidstore.keys import E

BASE_URL = "http://localhost:8000"
PUSH_PULL_ADDR = "tcp://127.0.0.1:5557"
ROUTER_DEALER_ADDR = "tcp://127.0.0.1:5558"

keys = [E.from_str(x) for x in ["key1", "key2", "key3"]]
values = [E.from_str(x) for x in ["value1", "value2", "value3"]]


def main():
    ctx = zmq.Context()
    push_sock = ctx.socket(zmq.PUSH)
    push_sock.connect(PUSH_PULL_ADDR)
    dealer_sock = ctx.socket(zmq.DEALER)
    dealer_sock.connect(ROUTER_DEALER_ADDR)

    try:
        _send_and_verify(push_sock, dealer_sock)
    finally:
        push_sock.close()
        dealer_sock.close()
        ctx.term()
        temp_files = ["temp_file1", "temp_file2"]
        for temp_file in temp_files:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_file)


def _send_and_verify(push_sock, dealer_sock):
    for k, v in zip(keys, values):
        msg = {
            "op_code": "INSERT",
            "k_high": k.high,
            "k_low": k.low,
            "v_high": v.high,
            "v_low": v.low,
        }
        push_sock.send(msgpack.packb(msg, use_bin_type=True))
        time.sleep(0.05)

    for k, v in zip(keys, values):
        msg = {
            "op_code": "LOOKUP",
            "k_high": k.high,
            "k_low": k.low,
        }
        dealer_sock.send_multipart([b"", msgpack.packb(msg, use_bin_type=True)])
        reply_parts = dealer_sock.recv_multipart()
        resp = msgpack.unpackb(reply_parts[1], raw=False)
        results = resp.get("results", [])
        # Compare with expected value tuple
        expected = [v.high, v.low]
        assert any(r in [expected, tuple(expected), list(expected)] for r in results), (
            "Lookup value mismatch"
        )

    response = requests.put(
        f"{BASE_URL}/config/promotion_threshold", json={"promotion_threshold": 128}
    )
    assert response.status_code == 200

    k = keys[0]
    msg = {
        "op_code": "LOOKUP",
        "k_high": k.high,
        "k_low": k.low,
    }
    dealer_sock.send_multipart([b"", msgpack.packb(msg, use_bin_type=True)])
    reply_parts = dealer_sock.recv_multipart()
    reply = reply_parts[1] if len(reply_parts) == 2 else reply_parts[0]
    resp = msgpack.unpackb(reply, raw=False)
    promoted_status = resp.get("promoted", False)
    assert promoted_status

    print("Prototype executed successfully.")


if __name__ == "__main__":
    main()
