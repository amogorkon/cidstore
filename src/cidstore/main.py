from __future__ import annotations
from zvic import constrain_this_module
constrain_this_module()

import asyncio
import threading
from pathlib import Path

from cidstore import async_zmq_server, control_api
from cidstore.storage import Storage
from cidstore.store import CIDStore
from cidstore.wal import WAL

if __name__ == "__main__":
    import platform

    if platform.system().startswith("Windows"):
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    h5_path = str(Path.cwd() / "cidstore.h5")
    storage = Storage(h5_path)
    wal = WAL(None)
    control_api.store = CIDStore(storage, wal)

    def run_zmq():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server = async_zmq_server.AsyncZMQServer(control_api.store)
        loop.run_until_complete(server.start())

    zmq_thread = threading.Thread(target=run_zmq, daemon=True)
    zmq_thread.start()

    import uvicorn

    uvicorn.run(control_api.app, host="0.0.0.0", port=8000)
