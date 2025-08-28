"""LLM Playground demo for CIDStore

This script starts the CIDStore service (the same code as `src/cidstore/main.py`) in a subprocess,
waits for it to be ready, then performs a small set of primary-function demonstrations:

- HTTP health/readiness checks
- Send a ZMQ PUSH mutation (INSERT)
- Send a ZMQ DEALER read (LOOKUP)
- Query the control API for metrics and config
- Send a DELETE and verify lookup returns empty

Requires the project's Python environment (see repository `requirements.txt`).

Run from repository root:

    python llm_playground/run_demo.py

"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time

import msgpack
import requests
import zmq

ROOT = os.path.dirname(os.path.dirname(__file__))
SERVER_PY = os.path.join(ROOT, "src", "cidstore", "main.py")


def start_server():
    # Start the server as a subprocess. Use unbuffered output for timely logs.
    # Start in a new process group so we can kill all children if needed.
    cmd = [sys.executable, "-u", SERVER_PY]
    env = os.environ.copy()
    # Force Python utf-8 mode and stdout encoding to avoid Windows logging errors
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    # Windows: use creationflags; POSIX: use preexec_fn
    # Capture stdout/stderr so we can stream server logs into this process for debugging
    if os.name == "nt":
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    else:
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            env=env,
            preexec_fn=os.setsid,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    # Start a thread to stream process output to our stdout for visibility
    def _stream_output(p):
        try:
            for line in iter(p.stdout.readline, b""):
                try:
                    print(line.decode(errors="replace"), end="")
                except Exception:
                    pass
        except Exception:
            pass

    t = threading.Thread(target=_stream_output, args=(proc,), daemon=True)
    t.start()
    return proc


def wait_ready(timeout=20.0):
    import socket

    hosts = ["127.0.0.1", "localhost"]
    paths = ["/ready", "/health"]
    start = time.time()
    while time.time() - start < timeout:
        for h in hosts:
            # First try HTTP readiness/health endpoints
            for p in paths:
                url = f"http://{h}:8000{p}"
                try:
                    r = requests.get(url, timeout=1.0)
                    if r.status_code == 200 and r.text.strip() in ("ready", "ok"):
                        return True
                except Exception as ex:
                    # Do not spam logs; keep a short message for debugging
                    print(f"wait_ready: {url} -> {ex}")
            # If HTTP didn't respond, try a TCP connect as a fallback
            try:
                with socket.create_connection((h, 8000), timeout=1.0):
                    # Port open — consider service ready enough to proceed
                    print(f"wait_ready: TCP port 8000 open on {h}")
                    return True
            except Exception:
                pass
        time.sleep(0.5)
    return False


def zmq_insert(key, value):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.connect("tcp://127.0.0.1:5557")
    pkt = {"op_code": "INSERT", "key": list(key), "value": list(value)}
    sock.send(msgpack.packb(pkt, use_bin_type=True))
    sock.close()
    ctx.term()


def zmq_delete(key, value=None):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.connect("tcp://127.0.0.1:5557")
    pkt = {"op_code": "DELETE", "key": list(key)}
    if value is not None:
        pkt["value"] = list(value)
    sock.send(msgpack.packb(pkt, use_bin_type=True))
    sock.close()
    ctx.term()


def zmq_lookup(key, timeout=5.0):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.RCVTIMEO = int(timeout * 1000)
    sock.connect("tcp://127.0.0.1:5558")
    pkt = {"op_code": "LOOKUP", "key": list(key)}
    sock.send(msgpack.packb(pkt, use_bin_type=True))
    try:
        parts = sock.recv_multipart()
    except zmq.error.Again:
        sock.close()
        ctx.term()
        raise TimeoutError("ZMQ lookup timed out")
    # server may send an empty delimiter frame then payload
    payload = parts[-1]
    resp = msgpack.unpackb(payload, raw=False)
    sock.close()
    ctx.term()
    return resp


def http_get(path: str):
    url = f"http://127.0.0.1:8000{path}"
    r = requests.get(url, timeout=3.0)
    try:
        return r.status_code, r.text
    except Exception:
        return r.status_code, r.content


def main():
    print("Starting CIDStore server (this will run in background)...")
    proc = start_server()
    try:
        print("Waiting for server to become ready...")
        try:
            ok = wait_ready(timeout=25.0)
        except KeyboardInterrupt:
            print("KeyboardInterrupt during wait; shutting down.")
            return
        if not ok:
            print("Server did not become ready in time. See process output for errors.")
            proc.terminate()
            proc.wait(timeout=3)
            return
        # If wait_ready returned via TCP fallback, ensure HTTP endpoints are available
        # by polling /ready and /health for a short grace period.
        http_ok = False
        for _ in range(10):
            try:
                s, b = http_get("/ready")
                if s == 200 and b.strip() == "ready":
                    http_ok = True
                    break
                s, b = http_get("/health")
                if s == 200 and b.strip() == "ok":
                    http_ok = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if not http_ok:
            print(
                "HTTP control endpoints not available after TCP open — continuing but some HTTP calls may 404."
            )
        # Always try to fetch OpenAPI to inspect registered routes for debugging
        try:
            s, b = http_get("/openapi.json")
            print("OpenAPI status:", s)
            if s == 200:
                try:
                    j = json.loads(b)
                    paths = list(j.get("paths", {}).keys())
                    print("OpenAPI paths:", paths)
                except Exception:
                    pass
        except Exception as ex:
            print("Failed to fetch openapi.json:", ex)

        print("Server ready — running demo operations.")

        print("HTTP /health:")
        print(http_get("/health"))

        print("Inserting key (100,200) -> value (300,400) via ZMQ PUSH")
        zmq_insert((100, 200), (300, 400))
        time.sleep(0.5)

        print("Lookup key (100,200) via ZMQ DEALER")
        res = zmq_lookup((100, 200))
        print("Lookup response:", json.dumps(res))

        print("Querying /metrics and /config/batch_size")
        print(http_get("/metrics"))
        print(http_get("/config/batch_size"))

        print(
            "Deleting the value (via HTTP debug endpoint) and verifying lookup returns empty"
        )
        # Use the debug HTTP endpoint to ensure deletion is applied synchronously
        try:
            r = requests.post(
                "http://127.0.0.1:8000/debug/delete",
                json={"high": 100, "low": 200, "vhigh": 300, "vlow": 400},
                timeout=5.0,
            )
            print("delete response:", r.status_code, r.text)
        except Exception as ex:
            print("Failed to call debug/delete:", ex)
        # Wait for backend to apply deletion. Poll the debug HTTP endpoint until empty.
        timeout = 8.0
        start = time.time()
        res2 = None
        while time.time() - start < timeout:
            try:
                status, body = http_get("/debug/get?high=100&low=200")
                if status == 200:
                    try:
                        data = json.loads(body)
                        results = data.get("results", [])
                        if not results:
                            res2 = data
                            break
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(0.5)
        if res2 is None:
            # Final attempt via ZMQ lookup
            try:
                res2 = zmq_lookup((100, 200))
            except Exception as ex:
                res2 = {"error": str(ex)}
        print("Post-delete lookup response:", json.dumps(res2))

        print("Demo complete — shutting down server.")
    except KeyboardInterrupt:
        print("Interrupted by user — shutting down server.")
    finally:
        # Robust shutdown: escalate from SIGINT to SIGTERM to SIGKILL, kill process group if needed
        print("Shutting down server...")
        killed = False
        try:
            if os.name == "nt":
                # Windows: try CTRL_BREAK_EVENT first
                try:
                    proc.send_signal(signal.CTRL_BREAK_EVENT)
                except Exception:
                    pass
            else:
                # POSIX: send SIGINT to process group
                import signal as _signal

                try:
                    os.killpg(os.getpgid(proc.pid), _signal.SIGINT)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            try:
                if os.name == "nt":
                    # On Windows, try taskkill to remove process tree
                    try:
                        subprocess.run(
                            ["taskkill", "/PID", str(proc.pid), "/T", "/F"], check=False
                        )
                    except Exception:
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                else:
                    try:
                        os.killpg(os.getpgid(proc.pid), _signal.SIGTERM)
                    except Exception:
                        pass
                proc.wait(timeout=3)
            except Exception:
                try:
                    if os.name == "nt":
                        proc.kill()
                    else:
                        try:
                            os.killpg(os.getpgid(proc.pid), _signal.SIGKILL)
                        except Exception:
                            pass
                    try:
                        proc.wait(timeout=2)
                    except Exception:
                        killed = True
                    else:
                        killed = False
                except Exception:
                    killed = True
        if killed:
            print("[WARNING] Server process was force-killed.")
        # On Windows ensure the entire process tree is removed as a final safeguard
        try:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
        except Exception:
            pass


if __name__ == "__main__":
    main()
