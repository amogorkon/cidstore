"""tree.py - Main CIDTree class, directory, bucket, and ValueSet logic (Spec 2)"""

"""tree.py - Main CIDTree class, directory, bucket, and ValueSet logic (Spec 2)"""

import io

import h5py

from .keys import E
from .storage import StorageManager
from .wal import WAL


class CIDTree:
    SPLIT_THRESHOLD = 128

    def __init__(self, storage):
        if isinstance(storage, str):
            self.storage = StorageManager(storage)
            self.file = self.storage.open()
        else:
            self.storage = storage
            self.file = self.storage.open() if self.storage else None
        self.directory = {}
        self.buckets = {}
        self._bucket_counter = 0
        self.root = self._load_root()

    def _get_bucket_id(self, key):
        return hash(key) % max(1, len(self.buckets)) if self.buckets else 0

    def insert(self, key, value):
        if key not in self.directory:
            if not self.buckets:
                bucket_id = self._bucket_counter
                self.buckets[bucket_id] = {}
                self._bucket_counter += 1
            else:
                bucket_id = self._get_bucket_id(key)
            self.directory[key] = bucket_id
        else:
            bucket_id = self.directory[key]
        bucket = self.buckets[bucket_id]
        # ValueSet: set of values for the key
        if key not in bucket:
            bucket[key] = set()
        bucket[key].add(value)
        # Split bucket if needed
        if len(bucket) > self.SPLIT_THRESHOLD:
            self._split_bucket(bucket_id)

    def lookup(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return []
        bucket = self.buckets.get(bucket_id, {})
        return list(bucket.get(key, []))

    def delete(self, key, value=None):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return
        bucket = self.buckets.get(bucket_id, {})
        if key in bucket:
            if value is None:
                del bucket[key]
            else:
                bucket[key].discard(value)
                if not bucket[key]:
                    del bucket[key]
        if not bucket:
            del self.buckets[bucket_id]
        if key in self.directory:
            del self.directory[key]

    def get_entry(self, key):
        bucket_id = self.directory.get(key)
        if bucket_id is None:
            return None
        bucket = self.buckets.get(bucket_id, {})
        values = list(bucket.get(key, []))
        try:
            ek = E.from_str(key) if isinstance(key, str) else E(key)
            key_high = ek.high if hasattr(ek, "high") else 0
            key_low = ek.low if hasattr(ek, "low") else 0
        except Exception:
            ek = key
            key_high = 0
            key_low = 0
        return {
            "key": ek,
            "key_high": key_high,
            "key_low": key_low,
            "values": values,
            "value": values[0] if values else None,
        }

    def _split_bucket(self, bucket_id):
        # Split the bucket into two when over threshold
        bucket = self.buckets[bucket_id]
        items = list(bucket.items())
        mid = len(items) // 2
        left_items = items[:mid]
        right_items = items[mid:]
        # Create new bucket
        new_bucket_id = self._bucket_counter
        self._bucket_counter += 1
        self.buckets[new_bucket_id] = dict(right_items)
        # Update directory for moved keys
        for k, _ in right_items:
            self.directory[k] = new_bucket_id
        # Shrink original bucket
        self.buckets[bucket_id] = dict(left_items)

    def _load_root(self):
        # Dummy for TDD
        return None

    def __init__(self, storage):
        # Accept either a StorageManager, a file path, or a BytesIO object
        if isinstance(storage, io.BytesIO):
            self._h5buffer = storage
            self.file = h5py.File(self._h5buffer, "a")
            self.storage = None
        elif isinstance(storage, str):
            self.storage = StorageManager(storage)
            self.file = self.storage.open()
        else:
            self.storage = storage
            self.file = self.storage.open()
        self.wal = WAL(self.storage) if self.storage else None
        self.root = self._load_root()

    # (all logic now handled above; removed legacy _memstore logic)
