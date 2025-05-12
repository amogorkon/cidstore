"""main.py"""

from .tree import BPlusTree
from .storage import StorageManager

if __name__ == "__main__":
    storage = StorageManager()
    tree = BPlusTree(storage)

    # Example inserts
    key = (0x1234, 0x5678)
    value = (0x9ABC, 0xDEF0)
    tree.insert(key, value)

    # Lookup example
    vals = tree.lookup(key)
    print(f"Values for {key}: {vals}")
