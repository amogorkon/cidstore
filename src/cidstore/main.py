"""main.py - Example usage and entry point for CIDTree"""

import asyncio

from .config import HDF5_FILE
from .keys import E
from .storage import Storage
from .store import CIDStore
from .wal import WAL


async def main():
    storage = Storage(HDF5_FILE)
    wal = WAL(storage)
    tree = CIDStore(storage, wal)

    key = E(0x12345678)
    value = E(0x9ABCDEF0)
    await tree.insert(key, value)
    vals = list(await tree.get(key))
    print(f"Values for {key}: {vals}")


if __name__ == "__main__":
    asyncio.run(main())
