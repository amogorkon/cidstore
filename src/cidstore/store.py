"""store.py - Main CIDStore class, directory, bucket, and ValueSet logic (Spec 2)"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sys
import threading
from collections.abc import Iterable
from typing import Any

import numpy as np

from .keys import E
from .storage import Storage
from .wal import WAL, OpType, unpack_record

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

CONF = "/config"


class CIDStore:
    """
    CIDStore: Main entry point for the CIDStore hash directory (fully async interface, sync logic).
    All public methods are async and log to WAL asynchronously.
    """

    SPLIT_THRESHOLD: int = 128

    def __init__(self, hdf: Storage, wal: WAL) -> None:
        assert isinstance(hdf, Storage), "hdf must be a Storage"
        assert isinstance(wal, WAL), "wal must be a WAL"
        self.hdf = hdf
        self.wal = wal
        self.dir: dict[E, int] = {}
        self._writer_lock = threading.RLock()
        self.hdf._init_hdf5_layout()
        self._directory_mode = "attr"
        self._directory_dataset = None
        self._directory_attr_threshold = 1000
        with self.hdf as f:
            if "/buckets" not in f:
                f.create_group("/buckets")
        self._load_directory()
        self._bucket_counter = 0
        self._wal_consumer_task: asyncio.Task
        self.wal.wal_apply = self.apply

    async def async_init(self) -> None:
        self._wal_consumer_task = asyncio.create_task(self.wal.consume())

    async def wal_checkpoint(self):
        await asyncio.to_thread(self._wal_checkpoint_sync)

    def _wal_checkpoint_sync(self):
        if hasattr(self, "wal") and hasattr(self.wal, "checkpoint"):
            self.wal.checkpoint()
        else:
            if hasattr(self, "hdf") and hasattr(self.hdf, "file"):
                self.hdf.file.flush()
            if hasattr(self, "wal") and hasattr(self.wal, "flush"):
                self.wal.flush()

    async def compact(self):
        await asyncio.to_thread(self._compact_all_sync)

    def _compact_all_sync(self):
        if hasattr(self, "dir"):
            for key in list(self.dir.keys()):
                self.compact_key(key)

    async def merge(self):
        await asyncio.to_thread(self.background_maintenance)

    async def auto_tune(self, metrics):
        await asyncio.to_thread(self._auto_tune_sync, metrics)

    def _auto_tune_sync(self, metrics):
        if not hasattr(self, "_autotune_state"):
            self._autotune_state = {
                "batch_size": 64,
                "flush_interval": 1.0,
                "error_violations": 0,
                "latency_violations": 0,
                "last_latency": None,
                "integral": 0.0,
                "prev_error": 0.0,
            }
        state = self._autotune_state
        kp, ki, kd = 0.5, 0.1, 0.3
        latency_target = 0.0001
        min_batch, max_batch = 32, 1024
        latency = metrics.get("latency_p99", 0.0)
        throughput = metrics.get("throughput_ops", 0.0)
        error_rate = metrics.get("error_rate", 0.0)
        buffer_occupancy = metrics.get("buffer_occupancy", 0)
        error = latency - latency_target
        state["integral"] += error
        derivative = error - state["prev_error"]
        state["prev_error"] = error
        adjustment = kp * error + ki * state["integral"] + kd * derivative
        batch_size = state["batch_size"]
        if error > 0 or error_rate > 0.01:
            state["latency_violations"] += 1
        else:
            state["latency_violations"] = 0
        if state["latency_violations"] >= 3 or error_rate > 0.05:
            batch_size = max(min_batch, batch_size // 2)
            state["latency_violations"] = 0
        elif adjustment < 0 and buffer_occupancy > 0.8:
            batch_size = min(max_batch, batch_size + 32)
        state["batch_size"] = batch_size
        flush_interval = state["flush_interval"]
        if error > 0:
            flush_interval = min(2.0, flush_interval * 1.1)
        else:
            flush_interval = max(0.1, flush_interval * 0.95)
        state["flush_interval"] = flush_interval
        if latency > 0.00015:
            state["error_violations"] += 1
        else:
            state["error_violations"] = 0
        if state["error_violations"] >= 5:
            state["batch_size"] = 64
            state["flush_interval"] = 1.0
            state["error_violations"] = 0
        print(
            f"[AutoTune] batch_size={state['batch_size']} flush_interval={state['flush_interval']} latency={latency:.6f} throughput={throughput} error_rate={error_rate}"
        )

    async def batch_insert(self, items: list[tuple[E, E]]) -> None:
        assert all(isinstance(k, E) and isinstance(v, E) for k, v in items)
        await self.wal.batch_insert([
            (key.high, key.low, value.high, value.low) for key, value in items
        ])

    async def batch_delete(self, keys: list[E]) -> None:
        assert all(isinstance(k, E) for k in keys)
        await self.wal.batch_delete([(key.high, key.low) for key in keys])

    async def insert(self, key: E, value: E) -> None:
        logger.info(f"[CIDStore.insert] Called with {key=} {value=}")
        assert isinstance(key, E)
        assert isinstance(value, E)
        assert key != E(0) != value
        V = await self.get(key)
        if V and value in V:
            logger.info(f"[CIDStore.insert] Value already present for {key=}")
            return
        await self.wal.log_insert(key.high, key.low, value.high, value.low)
        logger.info(f"[CIDStore.insert] Inserted value for {key=}")

    async def delete(self, key: E) -> None:
        """
        Delete a key and all its values.
        Args:
            key: The key to delete (E).
        Removes all values (inline and spill) for the key.
        """
        assert isinstance(key, E)
        await self.wal.log_delete(key.high, key.low)

    async def get(self, key: E) -> Iterable[E]:
        logger.info(f"[CIDStore.get] Called with {key=}")
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            logger.info(f"[CIDStore.get] No bucket found for {key=}")
            return iter([])
        bucket_name = f"bucket_{bucket_id}"
        bucket = self.buckets.get(bucket_name)
        if bucket is None:
            logger.info(f"[CIDStore.get] No bucket object found for {key=}")
            return iter([])
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                if bucket[i]["state_mask"] != 0:
                    logger.info(f"[CIDStore.get] Returning inline values for {key=}")
                    return (E(slot) for slot in bucket[i]["slots"] if slot != 0)
                spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                values_group = self.hdf.file["/values/sp"]
                if spill_ds_name in values_group:
                    logger.info(f"[CIDStore.get] Returning spill values for {key=}")
                    return (E(v) for v in values_group[spill_ds_name][:] if v != 0)
                else:
                    logger.info(f"[CIDStore.get] No spill values for {key=}")
                    return iter([])
        logger.info(f"[CIDStore.get] Key not found in any bucket: {key=}")
        return iter([])

    async def get_entry(self, key: E):
        """
        Retrieve the entry for a key, including all canonical fields.
        Args:
            key: Key to retrieve (E).
        Returns:
            dict or None: Entry dict with key, key_high, key_low, slots, state_mask, version, values, value.
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return None
        bucket_name = f"bucket_{bucket_id}"
        bucket = self.buckets.get(bucket_name)
        if bucket is None:
            return None
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                entry = {
                    "key": key,
                    "key_high": int(bucket[i]["key_high"]),
                    "key_low": int(bucket[i]["key_low"]),
                    "slots": bucket[i]["slots"].copy(),
                }
                if bucket[i]["state_mask"] == 0:
                    spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                    values_group = self.hdf.file["/values/sp"]
                    values = (
                        [v for v in values_group[spill_ds_name][:] if v != 0]
                        if spill_ds_name in values_group
                        else []
                    )
                else:
                    values = [slot for slot in bucket[i]["slots"] if slot != 0]
                entry["values"] = values
                entry["value"] = values[0] if values else None
                return entry
        return None

    async def delete_value(self, key: E, value: E) -> None:
        await self.wal.log_delete_value(key.high, key.low, value.high, value.low)

    def _load_directory(self) -> None:
        """
        Load directory from HDF5 attributes or canonical dataset (Spec 3).
        """
        with self.hdf as f:
            if "directory" in f:
                ds = f["directory"]
                self._directory_mode = "ds"
                self._directory_dataset = ds
                self.dir = {
                    E((int(row["key_high"]) << 64) | int(row["key_low"])): int(
                        row["bucket_id"]
                    )
                    for row in ds
                }
            elif CONF in f and "directory" in f[CONF].attrs:
                self._directory_mode = "attr"
                attr = f[CONF].attrs["directory"]
                if isinstance(attr, bytes):
                    attr = attr.decode("utf-8")
                try:
                    loaded = json.loads(attr)
                except Exception:
                    loaded = {}
                self.dir = {
                    E.from_str(k) if hasattr(E, "from_str") else E(int(k)): v
                    for k, v in loaded.items()
                }
            else:
                self._directory_mode = "attr"
                self.dir = {}

    async def _save_directory(self) -> None:
        """
        Save directory to HDF5 attributes or canonical dataset (Spec 3).
        """
        if self._directory_mode == "attr":
            self.hdf.file[CONF].attrs.modify("dir", json.dumps(self.dir))
        elif self._directory_mode == "ds":
            ds = self._directory_dataset
            ds.resize((len(self.dir),))
            for i, (k, v) in enumerate(self.dir.items()):
                key_int = int(k)
                key_high = key_int >> 64
                key_low = key_int & ((1 << 64) - 1)
                ds[i]["key_high"] = key_high
                ds[i]["key_low"] = key_low
                ds[i]["bucket_id"] = v
                ds[i]["spill_ptr"] = b""
                ds[i]["state_mask"] = 0
                ds[i]["version"] = 1
        self.hdf.file.flush()

    async def _maybe_migrate_directory(self) -> None:
        """
        Migrate directory from attribute to canonical, scalable, sharded/hybrid dataset.
        Only migrates if the directory exceeds the attribute threshold.
        """
        if (
            self._directory_mode != "attr"
            or len(self.dir) <= self._directory_attr_threshold
        ):
            return
        f = self.hdf.file
        dt = np.dtype([
            ("key_high", "<u8"),
            ("key_low", "<u8"),
            ("bucket_id", "<i8"),
            ("spill_ptr", "S32"),
            ("state_mask", "u1"),
            ("version", "<u4"),
        ])
        # Hybrid: single dataset if >1M buckets else sharded directory
        SHARD_THRESHOLD = 1_000_000
        items = list(self.dir.items())
        if len(items) > SHARD_THRESHOLD:
            self._shard_items_into_directory(items, f, dt)
        else:
            ds = f.create_dataset(
                "dir",
                shape=(len(items),),
                maxshape=(None,),
                dtype=dt,
                chunks=True,
                track_times=False,
            )
            for i, (k, v) in enumerate(items):
                ds[i]["key_high"] = k.high
                ds[i]["key_low"] = k.low
                ds[i]["bucket_id"] = v
                ds[i]["spill_ptr"] = b""
                ds[i]["state_mask"] = 0
                ds[i]["version"] = 1
            self._directory_mode = "ds"
            self._directory_dataset = ds
        # Remove old attribute
        if CONF in f and "dir" in f[CONF].attrs:
            f[CONF].attrs.modify("dir", None)
        f.flush()

    async def _shard_items_into_directory(self, items, f, dt):
        SHARD_SIZE = 100_000
        # Sharded directory: /directory/shard_{i}
        num_shards = (len(items) + SHARD_SIZE - 1) // SHARD_SIZE
        dir_group = f.require_group("dir")
        for k in list(dir_group.keys()):
            del dir_group[k]
        for shard_idx in range(num_shards):
            start = shard_idx * SHARD_SIZE
            end = min((shard_idx + 1) * SHARD_SIZE, len(items))
            shard_items = items[start:end]
            ds = dir_group.create_dataset(
                f"shard_{shard_idx:04d}",
                shape=(len(shard_items),),
                maxshape=(None,),
                dtype=dt,
                chunks=True,
                track_times=False,
            )
            for i, (k, v) in enumerate(shard_items):
                ek = E.from_str(k) if isinstance(k, str) else E(k)
                ds[i]["key_high"] = ek.high
                ds[i]["key_low"] = ek.low
                ds[i]["bucket_id"] = v
                ds[i]["spill_ptr"] = b""
                ds[i]["state_mask"] = 0
                ds[i]["version"] = 1
        self._directory_mode = "sharded"
        self._directory_dataset = dir_group

    async def _get_bucket_id(self, key: E) -> int:
        """
        Compute the bucket ID for a given key.
        Args:
            key: The key to hash (E).
        Returns:
            int: Bucket ID.
        """
        return hash(key) % max(1, len(self.buckets)) if self.buckets else 0

    def __contains__(self, key: E) -> bool:
        """
        Check if a key is in the directory.
        Args:
            key: The key to check (E).
        Returns:
            bool: True if the key is in the directory, False otherwise.
        """
        assert isinstance(key, E)
        return key in self.dir

    def __getitem__(self, key: E) -> set[E]:
        """
        Get the value for a key.
        Args:
            key: The key to lookup (E).
        Returns:
            The value associated with the key.
        Raises:
            KeyError: If the key is not found.
        """
        assert isinstance(key, E)
        entry = asyncio.run(self.get(key))
        if entry is not None:
            return {E(e) for e in entry}
        raise KeyError(f"Key not found: {key}")

    def __setitem__(self, key: E, value: E) -> None:
        """
        Set the value for a key.
        Args:
            key: The key to set.
            value: The value to add to the set of values of key.
        """
        assert isinstance(key, E)
        assert isinstance(value, E)
        asyncio.run(self.insert(key, value))

    def __delitem__(self, key: E) -> None:
        """
        Delete a key.
        Args:
            key: The key to delete.
        If the key has multiple values, removes all values (inline and spill).
        """
        assert isinstance(key, E)
        asyncio.run(self.delete(key))

    def get_tombstone_count(self, key: E) -> int:
        """
        Count tombstones (zeros) in the spill dataset for a key.
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return 0
        bucket = self.buckets.get(f"bucket_{bucket_id}")
        if bucket is None:
            return 0
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                if bucket[i]["state_mask"] == 0:
                    spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                    values_group = self.hdf.file["/values/sp"]
                    if spill_ds_name in values_group:
                        ds = values_group[spill_ds_name]
                        return int((ds[:] == 0).sum())
                return 0
        return 0

    async def is_spilled(self, key: E) -> bool:
        """
        Check if a key has spilled values (i.e., if a spill dataset exists).
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return False
        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
        values_group = self.hdf.file["/values/sp"]
        return spill_ds_name in values_group

    async def demote_if_possible(self, key: E) -> None:
        """
        Move spilled values back to inline slots if possible (<=4 values).
        If the spill dataset has <= 4 values, moves them back to inline slots and deletes the spill.
        """
        assert isinstance(key, E)
        bucket_id = self.dir.get(key)
        if bucket_id is None:
            return
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return
        for i in range(bucket.shape[0]):
            if bucket[i]["key_high"] == key.high and bucket[i]["key_low"] == key.low:
                if bucket[i]["state_mask"] == 0:
                    spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                    values_group = self.hdf.file["/values/sp"]
                    if spill_ds_name in values_group:
                        ds = values_group[spill_ds_name]
                        arr = ds[:]
                        arr = arr[arr != 0]
                        if len(arr) <= 4:
                            bucket[i]["slots"][:] = 0
                            for j, v in enumerate(arr):
                                bucket[i]["slots"][j] = v
                            del values_group[spill_ds_name]
                break

    async def gc(self) -> None:
        """
        Run background GC (orphan/tombstone cleanup).
        Compacts all buckets and removes empty spill datasets.
        """
        for key in list(self.dir.keys()):
            await self.compact(key)
        values_group = self.hdf.file["/values/sp"]
        for dsname in list(values_group.keys()):
            ds = values_group[dsname]
            if ds.shape[0] == 0 or all(ds[:] == 0):
                del values_group[dsname]
        self.hdf.file.flush()

    async def maintain(self) -> None:
        """
        Run background merge/sort/compaction for all buckets.
        Merges unsorted regions, merges underfilled buckets, and runs GC.
        """
        await self.background_bucket_maintenance()
        await self._maybe_merge_buckets()
        await self.run_gc_once()

    def expose_metrics(self) -> list[Any]:
        """
        Expose metrics for Prometheus scraping.
        Returns:
            list: Prometheus metric objects if available, else empty list.
        """
        try:
            from prometheus_client import Gauge, Info

            # Define metrics

            cidstore_info = Info("cidstore_info", "CIDStore information")
            bucket_gauge = Gauge("cidstore_buckets", "Number of buckets")
            directory_size_gauge = Gauge("cidstore_directory_size", "Directory size")
            split_events_counter = Gauge("cidstore_split_events", "Split events count")
            merge_events_counter = Gauge("cidstore_merge_events", "Merge events count")
            gc_runs_counter = Gauge("cidstore_gc_runs", "GC runs count")
            last_error_info = Info("cidstore_last_error", "Last error info")

            # Set metrics values
            cidstore_info.info({"version": "1.0"})
            bucket_gauge.set(len(self.buckets))
            directory_size_gauge.set(len(self.dir))
            split_events_counter.set(getattr(self, "_split_events", 0))
            merge_events_counter.set(getattr(self, "_merge_events", 0))
            gc_runs_counter.set(getattr(self, "_gc_runs", 0))
            last_error_info.info({"error": getattr(self, "_last_error", "")})

            # Return all defined metrics for scraping
            return [
                cidstore_info,
                bucket_gauge,
                directory_size_gauge,
                split_events_counter,
                merge_events_counter,
                gc_runs_counter,
                last_error_info,
            ]
        except ImportError:
            # prometheus_client not available, return empty list
            return []

    async def _load_root(self) -> dict[str, Any] | None:
        """
        Load the root node (if any) from the HDF5 file.
        Returns:
            dict or None: Root node data, or None if not present.
        """
        return dict(self.hdf.file["/root"]) if "/root" in self.hdf.file else None

    def __enter__(self) -> CIDStore:
        """
        Enter the runtime context related to this object.
        Returns:
            CIDStore: The CIDStore instance.
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Exit the runtime context related to this object.
        Closes the CIDStore instance.
        """
        asyncio.run(self.close())

    def __repr__(self) -> str:
        """
        Return a string representation of the CIDStore instance.
        Returns:
            str: String representation.
        """
        return f"<CIDStore buckets={len(self.buckets)} directory_size={len(self.dir)}>"

    def __len__(self) -> int:
        """
        Return the number of keys in the directory.
        Returns:
            int: Number of keys.
        """
        return len(self.dir)

    def debug_dump(self) -> str:
        """
        Debug: Dump the entire tree structure.
        Returns:
            str: String representation of the entire tree.
        """
        output = [f"CIDStore debug dump (buckets={len(self.buckets)})"]
        output.extend(self.debug_dump_bucket(bucket_id) for bucket_id in self.buckets)
        return "\n".join(output)

    def debug_dump_bucket(self, bucket_id: int) -> str:
        """
        Debug: Dump the contents of a bucket.
        Args:
            bucket_id: The ID of the bucket to dump.
        Returns:
            str: String representation of the bucket contents.
        """
        bucket = self.buckets.get(bucket_id)
        if bucket is None:
            return "Bucket not found"
        sorted_count = int(bucket.attrs.get("sorted_count", 0))
        unsorted_count = bucket.shape[0] - sorted_count
        return (
            f"Bucket {bucket_id}:\n"
            f"  Sorted count: {sorted_count}\n"
            f"  Unsorted count: {unsorted_count}\n"
            f"  Entries:\n"
            + "\n".join(
                f"    {i}: key_high={bucket[i]['key_high']} key_low={bucket[i]['key_low']} "
                f"slots={bucket[i]['slots']} state_mask={bucket[i]['state_mask']} "
                f"version={bucket[i]['version']}"
                for i in range(bucket.shape[0])
            )
        )

    def _add_value_to_bucket_entry(
        self, bucket, entry_idx: int, key: E, value: E
    ) -> None:
        """
        Helper to add a value to a bucket entry, handling slots, spill promotion, and state mask update.
        """
        slots = bucket[entry_idx]["slots"]
        for j in range(4):
            if slots[j] == 0:
                slots[j] = int(value)
                bucket[entry_idx]["version"] += 1
                bucket.file.flush()
                return
        # All slots full, promote to spill
        bucket_id = self.dir[key]
        spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
        values_group = self.file["/values/sp"]
        if spill_ds_name not in values_group:
            slot_values = [v for v in slots if v != 0]
            all_values = slot_values + [int(value)]
            ds = values_group.create_dataset(
                spill_ds_name,
                shape=(len(all_values),),
                maxshape=(None,),
                dtype="<u8",
                chunks=True,
            )
            ds[:] = all_values
            for j in range(4):
                slots[j] = 0
        else:
            ds = values_group[spill_ds_name]
            arr = ds[:]
            if int(value) not in arr:
                ds.resize((ds.shape[0] + 1,))
                ds[-1] = int(value)
        bucket[entry_idx]["state_mask"] = 0

    async def _auto_tune(self, metrics):
        if not hasattr(self, "_autotune_state"):
            self._autotune_state = {
                "batch_size": 64,
                "flush_interval": 1.0,
                "error_violations": 0,
                "latency_violations": 0,
                "last_latency": None,
                "integral": 0.0,
                "prev_error": 0.0,
            }
        state = self._autotune_state
        kp, ki, kd = 0.5, 0.1, 0.3
        latency_target = 0.0001
        min_batch, max_batch = 32, 1024
        latency = metrics.get("latency_p99", 0.0)
        throughput = metrics.get("throughput_ops", 0.0)
        error_rate = metrics.get("error_rate", 0.0)
        buffer_occupancy = metrics.get("buffer_occupancy", 0)
        error = latency - latency_target
        state["integral"] += error
        derivative = error - state["prev_error"]
        state["prev_error"] = error
        adjustment = kp * error + ki * state["integral"] + kd * derivative
        batch_size = state["batch_size"]
        if error > 0 or error_rate > 0.01:
            state["latency_violations"] += 1
        else:
            state["latency_violations"] = 0
        if state["latency_violations"] >= 3 or error_rate > 0.05:
            batch_size = max(min_batch, batch_size // 2)
            state["latency_violations"] = 0
        elif adjustment < 0 and buffer_occupancy > 0.8:
            batch_size = min(max_batch, batch_size + 32)
        state["batch_size"] = batch_size
        flush_interval = state["flush_interval"]
        if error > 0:
            flush_interval = min(2.0, flush_interval * 1.1)
        else:
            flush_interval = max(0.1, flush_interval * 0.95)
        state["flush_interval"] = flush_interval
        if latency > 0.00015:
            state["error_violations"] += 1
        else:
            state["error_violations"] = 0
        if state["error_violations"] >= 5:
            state["batch_size"] = 64
            state["flush_interval"] = 1.0
            state["error_violations"] = 0

        print(
            f"[AutoTune] batch_size={state['batch_size']} flush_interval={state['flush_interval']} latency={latency:.6f} throughput={throughput} error_rate={error_rate}"
        )

    async def close(self):
        self._wal_consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._wal_consumer_task
        with self._writer_lock:
            await self._save_directory()
            await self._save_wal()
            self.gc_thread.stop()
            await asyncio.to_thread(self.hdf.close)

    async def apply(self):
        logger.info("[CIDStore.apply] Processing WAL records")
        with self.wal._lock:
            tail = self.wal._get_tail()
            head = self.wal._get_head()
            current_pos = tail
            new_ops = []
            while current_pos != head:
                rec_bytes = self.wal._mmap[current_pos : current_pos + WAL.REC_SIZE]
                rec = unpack_record(rec_bytes)
                if rec is not None:
                    new_ops.append(rec)
                current_pos += WAL.REC_SIZE
                if current_pos >= self.wal.size:
                    current_pos = WAL.HEADER_SIZE
            if new_ops:
                logger.info(
                    f"[CIDStore.apply] Found {len(new_ops)} new operations to process"
                )
                for op in new_ops:
                    op_type = op["op_type"]
                    if op_type == OpType.INSERT.value:
                        k_high, k_low = op["k_high"], op["k_low"]
                        v_high, v_low = op["v_high"], op["v_low"]
                        key = E((k_high << 64) | k_low)
                        value = E((v_high << 64) | v_low)
                        logger.info(
                            f"[CIDStore.apply] Processing INSERT operation for {key=} {value=}"
                        )

                        # Get the key's bucket
                        bucket_id = self.dir.get(key)
                        if bucket_id is None:
                            # Need to create a new entry in a bucket
                            bucket_id = await self._get_bucket_id(key)
                            self.dir[key] = bucket_id
                            bucket_name = f"bucket_{bucket_id}"

                            # Create bucket if it doesn't exist
                            if bucket_name not in self.buckets:
                                with self.hdf as f:
                                    f.create_group(f"/buckets/{bucket_name}")

                            # Add entry to bucket
                            bucket = self.buckets[bucket_name]
                            for i in range(bucket.shape[0]):
                                if (
                                    bucket[i]["key_high"] == 0
                                    and bucket[i]["key_low"] == 0
                                ):
                                    bucket[i]["key_high"] = key.high
                                    bucket[i]["key_low"] = key.low
                                    bucket[i]["slots"][0] = int(value)
                                    bucket[i]["state_mask"] = 1
                                    bucket[i]["version"] = 1
                                    break
                            else:
                                # No empty slot found, add a new one
                                idx = bucket.shape[0]
                                bucket.resize((idx + 1,))
                                bucket[idx]["key_high"] = key.high
                                bucket[idx]["key_low"] = key.low
                                bucket[idx]["slots"][0] = int(value)
                                bucket[idx]["state_mask"] = 1
                                bucket[idx]["version"] = 1
                        else:
                            # Key exists, add value to its entry
                            bucket_name = f"bucket_{bucket_id}"
                            bucket = self.buckets.get(bucket_name)
                            if bucket is not None:
                                for i in range(bucket.shape[0]):
                                    if (
                                        bucket[i]["key_high"] == key.high
                                        and bucket[i]["key_low"] == key.low
                                    ):
                                        # Found the entry, add the value
                                        slots = bucket[i]["slots"]
                                        # Check if value is already in slots
                                        if int(value) in slots:
                                            break
                                        for j in range(4):
                                            if slots[j] == 0:
                                                slots[j] = int(value)
                                                bucket[i]["state_mask"] = 1
                                                bucket[i]["version"] += 1
                                                break
                                        else:
                                            # All slots full, promote to spill
                                            spill_ds_name = (
                                                f"sp_{bucket_id}_{key.high}_{key.low}"
                                            )
                                            values_group = self.hdf.file["/values/sp"]
                                            if spill_ds_name not in values_group:
                                                # Create spill dataset with existing values + new value
                                                slot_values = [
                                                    v for v in slots if v != 0
                                                ]
                                                all_values = slot_values + [int(value)]
                                                ds = values_group.create_dataset(
                                                    spill_ds_name,
                                                    shape=(len(all_values),),
                                                    maxshape=(None,),
                                                    dtype="<u8",
                                                    chunks=True,
                                                )
                                                ds[:] = all_values
                                                for j in range(4):
                                                    slots[j] = 0
                                                bucket[i]["state_mask"] = 0
                                            else:
                                                # Add to existing spill dataset
                                                ds = values_group[spill_ds_name]
                                                arr = ds[:]
                                                if int(value) not in arr:
                                                    ds.resize((ds.shape[0] + 1,))
                                                    ds[-1] = int(value)
                                        break
                    elif op_type == OpType.DELETE.value:
                        k_high, k_low = op["k_high"], op["k_low"]
                        v_high, v_low = op["v_high"], op["v_low"]
                        key = E((k_high << 64) | k_low)
                        logger.info(
                            f"[CIDStore.apply] Processing DELETE operation for {key=}"
                        )

                        bucket_id = self.dir.get(key)
                        if bucket_id is not None:
                            bucket_name = f"bucket_{bucket_id}"
                            bucket = self.buckets.get(bucket_name)
                            if bucket is not None:
                                # If v_high and v_low are both 0, delete the entire key
                                if v_high == 0 and v_low == 0:
                                    for i in range(bucket.shape[0]):
                                        if (
                                            bucket[i]["key_high"] == key.high
                                            and bucket[i]["key_low"] == key.low
                                        ):
                                            # Clear the entry
                                            bucket[i]["key_high"] = 0
                                            bucket[i]["key_low"] = 0
                                            bucket[i]["slots"][:] = 0
                                            bucket[i]["state_mask"] = 0
                                            bucket[i]["version"] = 0

                                            # Delete spill dataset if exists
                                            spill_ds_name = (
                                                f"sp_{bucket_id}_{key.high}_{key.low}"
                                            )
                                            values_group = self.hdf.file["/values/sp"]
                                            if spill_ds_name in values_group:
                                                del values_group[spill_ds_name]

                                            # Remove from directory
                                            if key in self.dir:
                                                del self.dir[key]
                                            break
                                else:
                                    # Delete specific value
                                    value = E((v_high << 64) | v_low)
                                    for i in range(bucket.shape[0]):
                                        if (
                                            bucket[i]["key_high"] == key.high
                                            and bucket[i]["key_low"] == key.low
                                        ):
                                            slots = bucket[i]["slots"]
                                            if bucket[i]["state_mask"] != 0:
                                                # Value is inline
                                                for j in range(4):
                                                    if slots[j] == int(value):
                                                        slots[j] = 0
                                                        # Shift remaining values
                                                        for k in range(j, 3):
                                                            slots[k] = slots[k + 1]
                                                        slots[3] = 0
                                                        break
                                            else:
                                                # Value is in spill dataset
                                                spill_ds_name = f"sp_{bucket_id}_{key.high}_{key.low}"
                                                values_group = self.hdf.file[
                                                    "/values/sp"
                                                ]
                                                if spill_ds_name in values_group:
                                                    ds = values_group[spill_ds_name]
                                                    arr = ds[:]
                                                    mask = arr != int(value)
                                                    if not all(mask):
                                                        # Value exists, remove it
                                                        new_arr = arr[mask]
                                                        if len(new_arr) <= 4:
                                                            # Can demote back to inline
                                                            for j, val in enumerate(
                                                                new_arr
                                                            ):
                                                                slots[j] = val
                                                            for j in range(
                                                                len(new_arr), 4
                                                            ):
                                                                slots[j] = 0
                                                            bucket[i]["state_mask"] = 1
                                                            del values_group[
                                                                spill_ds_name
                                                            ]
                                                        else:
                                                            # Keep as spill but update
                                                            ds[:] = 0  # Clear
                                                            ds.resize((len(new_arr),))
                                                            ds[:] = new_arr

                self.wal._set_tail(current_pos)
                self.wal._mmap.flush()
                logger.info("[CIDStore.apply] Finished processing WAL records")
