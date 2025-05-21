"""wal.py - Write Ahead Log (WAL) Buffer for CIDTree"""

from __future__ import annotations

import asyncio
import logging
import mmap
import os
import struct
import threading
import time
import zlib
from enum import Enum
from typing import Any

try:
    from prometheus_client import Counter, Gauge

    PROMETHEUS_AVAILABLE = True
    wal_records_appended = Counter(
        "cidtree_wal_records_appended_total",
        "Total number of records appended to the WAL",
        ["operation"],
    )
    wal_replay_count = Counter(
        "cidtree_wal_replay_total", "Total number of WAL replays completed"
    )
    wal_crc_failures = Counter(
        "cidtree_wal_crc_failures_total",
        "Total CRC checksum failures detected during replay",
    )
    wal_truncate_count = Counter(
        "cidtree_wal_truncate_total", "Total number of WAL truncations completed"
    )
    wal_error_count = Counter(
        "cidtree_wal_error_total",
        "Total WAL errors encountered",
        ["type"],
    )
    wal_head_position = Gauge(
        "cidtree_wal_head_position",
        "Current head pointer position (byte offset) in the WAL buffer",
    )
    wal_tail_position = Gauge(
        "cidtree_wal_tail_position",
        "Current tail pointer position (byte offset) in the WAL buffer",
    )
    wal_buffer_capacity_bytes = Gauge(
        "cidtree_wal_buffer_capacity_bytes",
        "Total size of the WAL memory-mapped buffer",
    )
    wal_records_in_buffer = Gauge(
        "cidtree_wal_records_in_buffer",
        "Number of records currently in the WAL buffer (head - tail) / record_size",
    )
except ImportError:
    PROMETHEUS_AVAILABLE = False

import numpy as np

HASH_ENTRY_DTYPE = np.dtype([
    ("key_high", "<u8"),
    ("key_low", "<u8"),
    ("slots", "<u8", 4),
    ("state_mask", "u1"),
    ("version", "<u4"),
])

logger = logging.getLogger(__name__)

MAX_TXN_RECORDS = 1000
RECORD_SIZE = 64
RECORD_CORE_SIZE = 50
CHECKSUM_SIZE = 4
PADDING_SIZE = RECORD_SIZE - RECORD_CORE_SIZE - CHECKSUM_SIZE


class OpType(Enum):
    INSERT = 1
    DELETE = 2
    TXN_START = 3
    TXN_COMMIT = 4
    TXN_ABORT = 5

    @classmethod
    def from_value(cls, value: int) -> OpType:
        if not 0 <= value <= 0x3F:
            raise ValueError(f"OpType value {value} out of 6-bit range (0-63)")
        try:
            return cls(value)
        except ValueError as e:
            logger.warning(f"Unknown OpType value encountered: {value}")
            raise ValueError(f"Unknown OpType value: {value}") from e


class OpVersion(Enum):
    PAST = 0
    CURRENT = 1
    NEXT = 2
    FUTURE = 3

    @classmethod
    def from_value(cls, value: int) -> "OpVersion":
        if not 0 <= value <= 0x03:
            raise ValueError(f"OpVersion value {value} out of 2-bit range (0-3)")
        return cls(value)


class WAL:
    async def consume_async(self, store, poll_interval: float = 0.1):
        """
        Async WAL consumer loop.
        Continuously reads new records and applies them to the store using internal, non-logging methods.
        Args:
            store: The CIDStore instance (must have an async _wal_apply method or use asyncio.to_thread).
            poll_interval: Time in seconds to wait between polling for new records.
        """
        last_tail = self._get_tail()
        while True:
            await asyncio.sleep(poll_interval)
            with self._lock:
                tail = self._get_tail()
                head = self._get_head()
                current_pos = tail
                new_ops = []
                while current_pos != head:
                    rec_bytes = self._mmap[current_pos : current_pos + WAL.RECORD_SIZE]
                    rec = self._unpack_record(rec_bytes)
                    if rec is not None:
                        new_ops.append(rec)
                    current_pos += WAL.RECORD_SIZE
                    if current_pos >= self.size:
                        current_pos = WAL.HEADER_SIZE
                if new_ops:
                    # Apply each op to the store (off main thread)
                    for op in new_ops:
                        await asyncio.to_thread(store._wal_apply, op)
                    self._set_tail(current_pos)
                    self._mmap.flush()
            # Optionally, add a stop condition or external cancellation

    HEADER_SIZE = 64
    DEFAULT_WAL_SIZE = 64 * 1024 * 1024
    RECORD_SIZE = 64
    RECORD_CORE_SIZE = 50
    CHECKSUM_SIZE = 4
    PADDING_SIZE = RECORD_SIZE - RECORD_CORE_SIZE - CHECKSUM_SIZE

    def __init__(
        self,
        storage: Any | None = None,
        path: str = ":memory:",
        size: int | None = None,
    ) -> None:
        self.storage: Any | None = storage
        self.path = path
        self.size = size or self.DEFAULT_WAL_SIZE
        min_size = WAL.HEADER_SIZE + WAL.RECORD_SIZE
        if self.size < min_size:
            self.size = min_size
            logger.warning(
                f"WAL size too small, increased to minimum {self.size} bytes."
            )
        record_area_size = self.size - WAL.HEADER_SIZE
        if record_area_size % WAL.RECORD_SIZE != 0:
            new_record_area_size = (
                record_area_size // WAL.RECORD_SIZE
            ) * WAL.RECORD_SIZE
            self.size = WAL.HEADER_SIZE + new_record_area_size
            logger.warning(
                f"WAL record area size not a multiple of RECORD_SIZE. Adjusted size to {self.size} bytes."
            )
        self._file: Any | None = None
        self._mmap: Any | None = None
        self._wal_fd: Any | None = None
        self._lock = threading.RLock()
        self._open()
        self._head: int = 0
        self._tail: int = 0
        self._init_header()
        logger.info(
            f"WAL initialized (mmap, path={self.path}, size={self.size} bytes, "
            f"record_size={WAL.RECORD_SIZE} bytes). Head: {self._get_head()}, Tail: {self._get_tail()}"
        )
        if PROMETHEUS_AVAILABLE:
            wal_buffer_capacity_bytes.set(self.size)
            wal_head_position.set(self._get_head())
            wal_tail_position.set(self._get_tail())
            wal_records_in_buffer.set(
                (self._get_head() - self._get_tail())
                % (self.size - WAL.HEADER_SIZE)
                // WAL.RECORD_SIZE
            )

    def _open(self):
        if self.path == ":memory:":
            self._file = None
            self._wal_fd = None
            self._mmap = mmap.mmap(-1, self.size)
            logger.debug(f"Created in-memory WAL of size {self.size}")
        else:
            exists = os.path.exists(self.path)
            flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
            mode = 0o666
            try:
                if not exists:
                    self._wal_fd = os.open(self.path, flags, mode)
                    os.ftruncate(self._wal_fd, self.size)
                    logger.debug(f"Created WAL file {self.path} of size {self.size}")
                else:
                    self._wal_fd = os.open(self.path, flags)
                    statinfo = os.fstat(self._wal_fd)
                    if statinfo.st_size != self.size:
                        logger.warning(
                            f"WAL file {self.path} size mismatch. Expected {self.size}, "
                            f"found {statinfo.st_size}. Adjusting size to match file."
                        )
                        self.size = statinfo.st_size
                        record_area_size = self.size - self.HEADER_SIZE
                        if (
                            record_area_size < self.RECORD_SIZE
                            or record_area_size % self.RECORD_SIZE != 0
                        ):
                            logger.error(
                                f"Existing WAL file {self.path} has invalid size/alignment after header."
                            )
                            os.close(self._wal_fd)
                            raise RuntimeError(
                                f"Existing WAL file {self.path} has invalid size/alignment."
                            )
                    logger.debug(f"Opened WAL file {self.path} of size {self.size}")
                self._mmap = mmap.mmap(
                    self._wal_fd, self.size, access=mmap.ACCESS_WRITE
                )
                logger.debug(f"Memory-mapped WAL file {self.path}")
            except Exception as e:
                logger.error(f"Failed to open or memory-map WAL file {self.path}: {e}")
                if self._wal_fd is not None:
                    os.close(self._wal_fd)
                raise RuntimeError(
                    f"Failed to open or memory-map WAL file {self.path}"
                ) from e

    def _init_header(self):
        initial_pos = WAL.HEADER_SIZE
        try:
            with self._lock:
                current_head = int.from_bytes(self._mmap[:8], "little")
                current_tail = int.from_bytes(self._mmap[8:16], "little")
        except Exception:
            current_head = 0
            current_tail = 0
        record_area_end = self.size

        def is_valid_pointer(p):
            return (
                p >= WAL.HEADER_SIZE
                and p < record_area_end
                and (p - WAL.HEADER_SIZE) % WAL.RECORD_SIZE == 0
            )

        if (
            not is_valid_pointer(current_head)
            or not is_valid_pointer(current_tail)
            or (current_head == 0 and current_tail == 0)
        ):
            with self._lock:
                self._mmap[:8] = initial_pos.to_bytes(8, "little")
                self._mmap[8:16] = initial_pos.to_bytes(8, "little")
                self._mmap[16 : WAL.HEADER_SIZE] = b"\x00" * (WAL.HEADER_SIZE - 16)
                self._mmap.flush()
            self._head = initial_pos
            self._tail = initial_pos
        else:
            self._head = current_head
            self._tail = current_tail

    def _get_head(self):
        return int.from_bytes(self._mmap[:8], "little")

    def _set_head(self, value):
        self._mmap[:8] = int(value).to_bytes(8, "little")
        if PROMETHEUS_AVAILABLE:
            wal_head_position.set(value)

    def _get_tail(self):
        return int.from_bytes(self._mmap[8:16], "little")

    def _set_tail(self, value):
        self._mmap[8:16] = int(value).to_bytes(8, "little")
        if PROMETHEUS_AVAILABLE:
            wal_tail_position.set(value)

    def _pack_record(self, rec: dict[str, Any]) -> bytes:
        version = rec.get("version", OpVersion.CURRENT.value) & 0x03
        op_type = rec.get("op_type", 0) & 0x3F
        version_op = (version << 6) | op_type
        reserved = rec.get("reserved", 0) & 0xFF
        nanos = rec.get("nanos", 0)
        seq = rec.get("seq", 0)
        shard_id = rec.get("shard_id", 0)
        key_high = rec.get("key_high", 0)
        key_low = rec.get("key_low", 0)
        value_high = rec.get("value_high", 0)
        value_low = rec.get("value_low", 0)
        packed_core = struct.pack(
            "<BBQIIQQQQ",
            version_op,
            reserved,
            nanos,
            seq,
            shard_id,
            key_high,
            key_low,
            value_high,
            value_low,
        )
        assert len(packed_core) == WAL.RECORD_CORE_SIZE
        checksum = zlib.crc32(packed_core) & 0xFFFFFFFF
        full_packed = (
            packed_core
            + checksum.to_bytes(WAL.CHECKSUM_SIZE, "little")
            + b"\x00" * WAL.PADDING_SIZE
        )
        assert len(full_packed) == WAL.RECORD_SIZE
        return full_packed

    def _unpack_record(self, rec_bytes: bytes) -> dict[str, Any] | None:
        if len(rec_bytes) != WAL.RECORD_SIZE:
            logger.error(
                f"Attempted to unpack record of incorrect size: {len(rec_bytes)} bytes."
            )
            if PROMETHEUS_AVAILABLE:
                wal_error_count.labels(type="unpack_size_mismatch").inc()
            return None
        packed_core = rec_bytes[: WAL.RECORD_CORE_SIZE]
        stored_checksum = int.from_bytes(
            rec_bytes[WAL.RECORD_CORE_SIZE : WAL.RECORD_CORE_SIZE + WAL.CHECKSUM_SIZE],
            "little",
        )
        computed_checksum = zlib.crc32(packed_core) & 0xFFFFFFFF
        if computed_checksum != stored_checksum:
            logger.warning(
                f"WAL record checksum mismatch. Expected {stored_checksum}, computed {computed_checksum}."
            )
            if PROMETHEUS_AVAILABLE:
                wal_crc_failures.inc()
            return None
        try:
            (
                version_op,
                reserved,
                nanos,
                seq,
                shard_id,
                key_high,
                key_low,
                value_high,
                value_low,
            ) = struct.unpack("<BBQIIQQQQ", packed_core)
        except struct.error as e:
            logger.error(f"Error unpacking WAL record struct: {e}")
            if PROMETHEUS_AVAILABLE:
                wal_error_count.labels(type="unpack_struct_error").inc()
            return None
        version = (version_op >> 6) & 0x03
        op_type = version_op & 0x3F
        return {
            "version": version,
            "op_type": op_type,
            "reserved": reserved,
            "nanos": nanos,
            "seq": seq,
            "shard_id": shard_id,
            "key_high": key_high,
            "key_low": key_low,
            "value_high": value_high,
            "value_low": value_low,
            "checksum": stored_checksum,
        }

    def append(self, records: list[dict[str, Any]]) -> None:
        assert isinstance(records, list), "records must be a list"
        if not records:
            return
        with self._lock:
            self._check_and_append_wal_records(records)

    def _check_and_append_wal_records(self, records):
        head = self._get_head()
        tail = self._get_tail()
        record_area_size = self.size - WAL.HEADER_SIZE
        records_in_buffer = (
            (head - tail + record_area_size) % record_area_size // WAL.RECORD_SIZE
        )
        space_needed = len(records) * WAL.RECORD_SIZE
        available_space = record_area_size - records_in_buffer * WAL.RECORD_SIZE
        if space_needed > available_space:
            logger.error(
                f"WAL full. Needed {space_needed} bytes, available {available_space}."
            )
            if PROMETHEUS_AVAILABLE:
                wal_error_count.labels(type="wal_full").inc()
            raise RuntimeError("WAL file full; checkpoint required")
        for rec in records:
            packed = self._pack_record(rec)
            assert len(packed) == WAL.RECORD_SIZE
            write_pos = head
            self._mmap[write_pos : write_pos + WAL.RECORD_SIZE] = packed
            head += WAL.RECORD_SIZE
            if head >= self.size:
                head = WAL.HEADER_SIZE
                logger.debug("WAL head wrapped around.")
        self._set_head(head)
        self._mmap.flush()
        logger.info(f"Appended {len(records)} WAL record(s). New head: {head}.")
        if PROMETHEUS_AVAILABLE:
            wal_records_appended.labels(operation="append").inc(len(records))
            wal_records_in_buffer.set(
                (head - tail + record_area_size) % record_area_size // WAL.RECORD_SIZE
            )

    def replay(self) -> list[dict[str, Any]]:
        ops: list[dict[str, Any]] = []
        with self._lock:
            tail = self._get_tail()
            head = self._get_head()
            current_pos = tail
            while current_pos != head:
                rec_bytes = self._mmap[current_pos : current_pos + WAL.RECORD_SIZE]
                rec = self._unpack_record(rec_bytes)
                if rec is not None:
                    ops.append(rec)
                current_pos += WAL.RECORD_SIZE
                if current_pos >= self.size:
                    current_pos = WAL.HEADER_SIZE
            self._set_tail(head)
            self._mmap.flush()
            logger.info(
                f"Replay complete. Processed {len(ops)} records. New tail: {head}."
            )
            if PROMETHEUS_AVAILABLE:
                wal_replay_count.inc()
                wal_tail_position.set(head)
                wal_records_in_buffer.set(0)
        return ops

    def truncate(self, confirmed_through_pos: int) -> None:
        assert isinstance(confirmed_through_pos, int), (
            "confirmed_through_pos must be an integer byte offset"
        )
        with self._lock:
            record_area_start = WAL.HEADER_SIZE
            record_area_end = self.size

            def is_valid_truncate_pos(p):
                return (
                    p >= record_area_start
                    and p < record_area_end
                    and (p - record_area_start) % WAL.RECORD_SIZE == 0
                )

            if not is_valid_truncate_pos(confirmed_through_pos):
                logger.warning(
                    f"Invalid truncate position provided: {confirmed_through_pos}. Must be >= {record_area_start}, < {record_area_end}, and multiple of {WAL.RECORD_SIZE} from {record_area_start}. Truncate skipped."
                )
                if PROMETHEUS_AVAILABLE:
                    wal_error_count.labels(type="truncate_invalid_pos").inc()
                return
            self._set_tail(confirmed_through_pos)
            self._mmap.flush()
            logger.info(f"WAL truncated. New tail: {confirmed_through_pos}.")
            if PROMETHEUS_AVAILABLE:
                wal_truncate_count.inc()
                wal_tail_position.set(confirmed_through_pos)
                head = self._get_head()
                wal_records_in_buffer.set(
                    (head - confirmed_through_pos + (self.size - WAL.HEADER_SIZE))
                    % (self.size - WAL.HEADER_SIZE)
                    // WAL.RECORD_SIZE
                )

    def close(self) -> None:
        with self._lock:
            if self._mmap:
                try:
                    self._mmap.flush()
                    self._mmap.close()
                    logger.debug("WAL mmap closed.")
                except Exception as e:
                    logger.error(f"Error closing WAL mmap: {e}")
                finally:
                    self._mmap = None
            if self._wal_fd is not None:
                try:
                    os.close(self._wal_fd)
                    logger.debug(f"WAL file descriptor {self._wal_fd} closed.")
                except Exception as e:
                    logger.error(
                        f"Error closing WAL file descriptor {self._wal_fd}: {e}"
                    )
                finally:
                    self._wal_fd = None
            self._file = None

    @staticmethod
    def prometheus_metrics() -> list[Any]:
        if not PROMETHEUS_AVAILABLE:
            return []
        return [
            wal_records_appended,
            wal_replay_count,
            wal_crc_failures,
            wal_truncate_count,
            wal_error_count,
            wal_head_position,
            wal_tail_position,
            wal_buffer_capacity_bytes,
            wal_records_in_buffer,
        ]

    def wal_record(
        self,
        version: OpVersion,
        op_type: OpType,
        time: tuple[int, int, int],
        key_high,
        key_low,
        value_high,
        value_low,
        *,
        version_override=None,
    ):
        # Allow explicit version override for schema evolution testing
        v = version if version_override is None else version_override
        assert isinstance(op_type, OpType)
        op_val = op_type.value
        version_op = ((v & 0x03) << 6) | (op_val & 0x3F)
        nanos, seq, shard = time
        reserved = 0
        record = {
            "version_op": version_op,
            "reserved": reserved,
            "nanos": nanos,
            "seq": seq,
            "shard_id": shard,
            "key_high": key_high,
            "key_low": key_low,
            "value_high": value_high,
            "value_low": value_low,
        }
        # Compute CRC32 over all fields except checksum
        packed = (
            record["version_op"].to_bytes(1, "little")
            + record["reserved"].to_bytes(1, "little")
            + record["nanos"].to_bytes(8, "little")
            + record["seq"].to_bytes(4, "little")
            + record["shard_id"].to_bytes(4, "little")
            + record["key_high"].to_bytes(8, "little")
            + record["key_low"].to_bytes(8, "little")
            + record["value_high"].to_bytes(8, "little")
            + record["value_low"].to_bytes(8, "little")
        )
        record["checksum"] = zlib.crc32(packed) & 0xFFFFFFFF
        # Pad to 64 bytes as per spec (WAL record must be 64 bytes)
        # Fields above: 1+1+8+4+4+8+8+8+8+4 = 54 bytes, so pad with 10 bytes
        full_packed = packed + record["checksum"].to_bytes(4, "little") + (b"\x00" * 10)
        assert len(full_packed) == 64, (
            f"WAL record must be 64 bytes, got {len(full_packed)}"
        )
        return record

    def _next_hybrid_time(self):
        """
        Generate a hybrid time tuple (nanos, seq, shard_id) for WAL records.
        Uses time.time_ns() and a per-shard sequence counter.
        """
        nanos = int(time.time_ns())
        self._wal_seq += 1
        return nanos, self._wal_seq, self.shard_id

    def log_insert(self, key_high, key_low, value_high, value_low, time=None):
        """
        Append a TXN_START, INSERT, TXN_COMMIT batch for a single insert.
        """
        if time is None:
            time = self._next_hybrid_time()
        self.append([
            self.wal_record(
                OpVersion.CURRENT.value, OpType.TXN_START, time, 0, 0, 0, 0
            ),
            self.wal_record(
                OpVersion.CURRENT.value,
                OpType.INSERT,
                time,
                key_high,
                key_low,
                value_high,
                value_low,
            ),
            self.wal_record(
                OpVersion.CURRENT.value, OpType.TXN_COMMIT, time, 0, 0, 0, 0
            ),
        ])
