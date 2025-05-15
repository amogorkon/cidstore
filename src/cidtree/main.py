"""main.py - Example usage and entry point for CIDTree"""



from .config import HDF5_FILE
from .storage import StorageManager
from .tree import CIDTree


if __name__ == "__main__":
    storage = StorageManager(HDF5_FILE)
    tree = CIDTree(storage)

    key = (0x1234, 0x5678)
    value = (0x9ABC, 0xDEF0)
    tree.insert(key, value)
    vals = tree.lookup(key)
    print(f"Values for {key}: {vals}")
