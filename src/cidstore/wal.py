"""wal.py - Write Ahead Log (WAL) Buffer for CIDTree"""

from __future__ import annotations

import mmap
import os
import struct
import threading
import time
import zlib
from asyncio import Event
from pathlib import Path

from .constants import OP, OpType, OpVer
from .logger import get_logger
from .metrics import (
    PROMETHEUS_AVAILABLE,
    wal_buffer_capacity_bytes,
    wal_crc_failures,
    wal_error_count,
    wal_head_position,
    wal_records_appended,
    wal_records_in_buffer,
    wal_replay_count,
    wal_tail_position,
    wal_truncate_count,
)
from .utils import assumption

logger = get_logger(__name__)


class WAL:
    HEADER_SIZE = 64
    DEFAULT_WAL_SIZE = 64 * 1024 * 1024
    REC_SIZE = 64
    RECORD_CORE_SIZE = 50
    CHECKSUM_SIZE = 4
    PADDING_SIZE = REC_SIZE - RECORD_CORE_SIZE - CHECKSUM_SIZE

    def __init__(
        self,
        path: Path | None = None,
        size: int | None = None,
    ) -> None:
        self.path = path
        self.size = size or self.DEFAULT_WAL_SIZE
        self._wal_seq: int = 0
        self.shard_id: int = 0
        self._wal_fd = None  # Always define _wal_fd
        min_size = WAL.HEADER_SIZE + WAL.REC_SIZE
        if self.size < min_size:
            self.size = min_size
            logger.warning(
                f"WAL size too small, increased to minimum {self.size} bytes."
            )
        record_area_size = self.size - WAL.HEADER_SIZE
        if record_area_size % WAL.REC_SIZE != 0:
            new_record_area_size = (record_area_size // WAL.REC_SIZE) * WAL.REC_SIZE
            self.size = WAL.HEADER_SIZE + new_record_area_size
            logger.warning(
                f"WAL record area size not a multiple of RECORD_SIZE. Adjusted size to {self.size} bytes."
            )
        self._lock = threading.RLock()
        self._mmap: mmap.mmap

        # Event for async consumers to be notified of new records
        self._new_record_event = Event()

        self.open()
        self._head: int = 0
        self._tail: int = 0
        self._init_header()
        logger.info(
            f"WAL initialized (mmap, path={self.path}, size={self.size} bytes, "
            f"record_size={WAL.REC_SIZE} bytes). Head: {self._get_head()}, Tail: {self._get_tail()}"
        )
        if PROMETHEUS_AVAILABLE:
            wal_buffer_capacity_bytes.set(self.size)
            wal_head_position.set(self._get_head())
            wal_tail_position.set(self._get_tail())
            wal_records_in_buffer.set(
                (self._get_head() - self._get_tail())
                % (self.size - WAL.HEADER_SIZE)
                // WAL.REC_SIZE
            )

    def open(self):
        if self.path is None:
            self._mmap = mmap.mmap(-1, self.size)
            logger.debug(f"Created in-memory WAL of size {self.size}")
        else:
            wal_path = Path(self.path)
            exists = wal_path.exists()
            flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
            mode = 0o666
            try:
                if not exists:
                    self._wal_fd = os.open(str(wal_path), flags, mode)
                    os.ftruncate(self._wal_fd, self.size)
                    logger.debug(f"Created WAL file {wal_path} of size {self.size}")
                else:
                    self._wal_fd = os.open(str(wal_path), flags)
                    statinfo = os.fstat(self._wal_fd)
                    if statinfo.st_size != self.size:
                        logger.warning(
                            f"WAL file {wal_path} size mismatch. Expected {self.size}, "
                            f"found {statinfo.st_size}. Adjusting size to match file."
                        )
                        self.size = statinfo.st_size
                        record_area_size = self.size - self.HEADER_SIZE
                        if (
                            record_area_size < self.REC_SIZE
                            or record_area_size % self.REC_SIZE != 0
                        ):
                            logger.error(
                                f"Existing WAL file {wal_path} has invalid size/alignment after header."
                            )
                            os.close(self._wal_fd)
                            raise RuntimeError(
                                f"Existing WAL file {wal_path} has invalid size/alignment."
                            )
                    logger.debug(f"Opened WAL file {wal_path} of size {self.size}")
                self._mmap = mmap.mmap(
                    self._wal_fd, self.size, access=mmap.ACCESS_WRITE
                )
                logger.debug(f"Memory-mapped WAL file {wal_path}")
            except Exception as e:
                logger.error(f"Failed to open or memory-map WAL file {wal_path}: {e}")
                if self._wal_fd is not None:
                    os.close(self._wal_fd)
                raise RuntimeError(
                    f"Failed to open or memory-map WAL file {wal_path}"
                ) from e

    async def consume_polling(self):
        """
        Async WAL consumer loop.
        Waits for new records using an event, with polling as fallback.
        """

        while True:
            await self._new_record_event.wait()
            await self.consume()
            self._new_record_event.clear()

    async def consume_once(self):
        """
        Process all new WAL records once as a single batch.
        """
        await self.consume()
        self._new_record_event.clear()

    async def consume(self):
        for op in self.replay():
            await self.apply(op)

    def apply(self, op: OP):
        raise NotImplementedError("Is assigned via store.")

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
                and (p - WAL.HEADER_SIZE) % WAL.REC_SIZE == 0
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

    async def append(self, records: list[bytes]):
        logger.info(f"[WAL.append] Called with {len(records)}")
        head = self._get_head()
        tail = self._get_tail()
        rec_area_size = self.size - WAL.HEADER_SIZE
        records_in_buffer = (
            (head - tail + rec_area_size) % rec_area_size // WAL.REC_SIZE
        )
        space_needed = len(records) * WAL.REC_SIZE
        available_space = rec_area_size - records_in_buffer * WAL.REC_SIZE
        if space_needed > available_space:
            logger.error(
                f"WAL full. Needed {space_needed} bytes, available {available_space}."
            )
            if PROMETHEUS_AVAILABLE:
                wal_error_count.labels(type="wal_full").inc()
            raise RuntimeError("WAL file full; checkpoint required")
        for rec in records:
            write_pos = head
            logger.debug(f"[WAL.append] Writing record at position {write_pos}")
            self._mmap[write_pos : write_pos + WAL.REC_SIZE] = rec
            head += WAL.REC_SIZE
            if head >= self.size:
                head = WAL.HEADER_SIZE
                logger.debug("WAL head wrapped around.")
        self._set_head(head)
        self._mmap.flush()
        logger.info(f"[WAL.append] Appended {len(records)}, {head=}")
        if PROMETHEUS_AVAILABLE:
            wal_records_appended.labels(operation="append").inc(len(records))
            wal_records_in_buffer.set(
                (head - tail + rec_area_size) % rec_area_size // WAL.REC_SIZE
            )
        self._new_record_event.set()

    def replay(self) -> list[OP]:
        logger.info("[WAL.replay] Called")
        ops: list[OP] = []
        with self._lock:
            tail = self._get_tail()
            head = self._get_head()

            logger.debug(f"[WAL.replay] Starting at tail={tail}, head={head}")
            while tail != head:
                rec_bytes = self._mmap[tail : tail + WAL.REC_SIZE]
                rec = unpack_record(rec_bytes)
                if rec is not None:
                    ops.append(rec)
                tail += WAL.REC_SIZE
                if tail >= self.size:
                    tail = WAL.HEADER_SIZE
            self._set_tail(head)
            self._mmap.flush()
            logger.info(
                f"[WAL.replay] Replayed {len(ops)} record(s). Tail set to head {head}."
            )
            if PROMETHEUS_AVAILABLE:
                wal_replay_count.inc()
                wal_tail_position.set(head)
                wal_records_in_buffer.set(0)
        return ops

    def truncate(self, confirmed_through_pos: int) -> None:
        assumption(confirmed_through_pos, int)
        with self._lock:
            record_area_start = WAL.HEADER_SIZE
            record_area_end = self.size

            def is_valid_truncate_pos(p):
                return (
                    p >= record_area_start
                    and p < record_area_end
                    and (p - record_area_start) % WAL.REC_SIZE == 0
                )

            if not is_valid_truncate_pos(confirmed_through_pos):
                logger.warning(
                    f"Invalid truncate position provided: {confirmed_through_pos}. Must be >= {record_area_start}, < {record_area_end}, and multiple of {WAL.REC_SIZE} from {record_area_start}. Truncate skipped."
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
                    // WAL.REC_SIZE
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

    def _next_hybrid_time(self):
        """
        Generate a hybrid time tuple (nanos, seq, shard_id) for WAL records.
        Uses time.time_ns() and a per-shard sequence counter.
        """
        nanos = int(time.time_ns())
        self._wal_seq += 1
        return nanos, self._wal_seq, self.shard_id

    async def log_insert(self, k_high, k_low, v_high, v_low, time=None):
        logger.info(
            f"[WAL.log_insert] Called with {k_high=}, {k_low=}, {v_high=}, {v_low=}"
        )
        if time is None:
            time = self._next_hybrid_time()
        await self.append([
            pack_record(OpVer.NOW, OpType.TXN_START, time, 0, 0, 0, 0),
            pack_record(OpVer.NOW, OpType.INSERT, time, k_high, k_low, v_high, v_low),
            pack_record(OpVer.NOW, OpType.TXN_COMMIT, time, 0, 0, 0, 0),
        ])
        logger.info(
            f"[WAL.log_insert] Inserted record for {k_high=}, {k_low=}, {v_high=}, {v_low=}"
        )
        await self.consume_once()

    def flush(self) -> None:
        """
        Flush the WAL memory-mapped file to disk.
        """
        with self._lock:
            if self._mmap is not None:
                self._mmap.flush()
                logger.debug("WAL flushed to disk.")

    def _checkpoint(self) -> None:
        """
        Perform a checkpoint operation on the WAL.
        This typically means truncating the WAL up to the current head position (all operations up to this point are considered durable).
        """
        with self._lock:
            head = self._get_head()
            self._set_tail(head)
            self.flush()
            logger.info(f"WAL checkpoint: truncated up to head {head}.")
            if PROMETHEUS_AVAILABLE:
                wal_tail_position.set(head)
                wal_records_in_buffer.set(0)

    async def batch_insert(self, items: list[tuple[int, int, int, int]]) -> None:
        """
        Atomically log a batch of inserts as a single WAL transaction (TXN_START, multiple INSERT, TXN_COMMIT).
        Each item is a tuple: (k_high, k_low, v_high, v_low)
        """
        logger.info(f"[WAL.batch_insert] Called with {items=}")
        if not items:
            logger.info("[WAL.batch_insert] No items to insert.")
            return
        time_tuple = self._next_hybrid_time()
        records = [pack_record(OpVer.NOW, OpType.TXN_START, time_tuple, 0, 0, 0, 0)]
        records.extend(
            pack_record(
                OpVer.NOW,
                OpType.INSERT,
                time_tuple,
                k_high,
                k_low,
                v_high,
                v_low,
            )
            for k_high, k_low, v_high, v_low in items
        )
        records.append(
            pack_record(OpVer.NOW, OpType.TXN_COMMIT, time_tuple, 0, 0, 0, 0)
        )
        await self.append(records)
        logger.info(f"[WAL.batch_insert] Inserted {items=}")

    async def batch_delete(self, items: list[tuple[int, int]]) -> None:
        """
        Atomically log a batch of deletes as a single WAL transaction (TXN_START, multiple DELETE, TXN_COMMIT).
        Each item is a tuple: (k_high, k_low)
        """
        logger.info(f"[WAL.batch_delete] Called with {items=}")
        if not items:
            logger.info("[WAL.batch_delete] No items to delete.")
            return
        time_tuple = self._next_hybrid_time()
        records = [pack_record(OpVer.NOW, OpType.TXN_START, time_tuple, 0, 0, 0, 0)]
        records.extend(
            pack_record(
                OpVer.NOW,
                OpType.DELETE,
                time_tuple,
                k_high,
                k_low,
                0,
                0,
            )
            for k_high, k_low in items
        )
        records.append(
            pack_record(OpVer.NOW, OpType.TXN_COMMIT, time_tuple, 0, 0, 0, 0)
        )
        await self.append(records)
        logger.info(f"[WAL.batch_delete] Deleted {items=}")

    async def log_delete(self, k_high, k_low, time=None):
        logger.info(f"[WAL.log_delete] Called with {k_high=}, {k_low=}")
        if time is None:
            time = self._next_hybrid_time()
        await self.append([
            pack_record(OpVer.NOW, OpType.TXN_START, time, 0, 0, 0, 0),
            pack_record(
                OpVer.NOW,
                OpType.DELETE,
                time,
                k_high,
                k_low,
                0,
                0,
            ),
            pack_record(OpVer.NOW, OpType.TXN_COMMIT, time, 0, 0, 0, 0),
        ])
        logger.info(
            f"[WAL.log_delete] Deleted record for k_high={k_high}, k_low={k_low}"
        )

    async def log_delete_value(self, k_high, k_low, v_high, v_low, time=None):
        """
        Log the deletion of a specific value from a valueset as a WAL transaction.
        Args:
            k_high, k_low: The key to delete from.
            v_high, v_low: The value to delete.
        """
        logger.info(
            f"[WAL.log_delete_value] Called with k_high={k_high}, k_low={k_low}, v_high={v_high}, v_low={v_low}"
        )
        if time is None:
            time = self._next_hybrid_time()
        await self.append([
            pack_record(OpVer.NOW, OpType.TXN_START, time, 0, 0, 0, 0),
            pack_record(OpVer.NOW, OpType.DELETE, time, k_high, k_low, v_high, v_low),
            pack_record(OpVer.NOW, OpType.TXN_COMMIT, time, 0, 0, 0, 0),
        ])
        logger.info(
            f"[WAL.log_delete_value] Deleted value for k_high={k_high}, k_low={k_low}, v_high={v_high}, v_low={v_low}"
        )


def pack_record(
    version: OpVer,
    op_type: OpType,
    time: tuple[int, int, int],
    k_high,
    k_low,
    v_high,
    v_low,
) -> bytes:
    ver = int(version.value)
    op = int(op_type.value)
    version_op = ((ver & 0x03) << 6) | (op & 0x3F)
    nanos, seq, shard_id = time
    reserved = 0
    # Compute CRC32 over all fields except checksum
    packed_core = struct.pack(
        "<BBQIIQQQQ",
        version_op,
        reserved,
        nanos,
        seq,
        shard_id,
        k_high,
        k_low,
        v_high,
        v_low,
    )
    checksum = zlib.crc32(packed_core) & 0xFFFFFFFF
    # Pad to 64 bytes as per spec (WAL record must be 64 bytes)
    # Fields above: 1+1+8+4+4+8+8+8+8+4 = 54 bytes, so pad with 10 bytes
    full_packed: bytes = packed_core + checksum.to_bytes(4, "little") + (b"\x00" * 10)
    assert len(full_packed) == 64, (
        f"WAL record must be 64 bytes, got {len(full_packed)}"
    )
    return full_packed


def unpack_record(rec: bytes) -> OP | None:
    if len(rec) != WAL.REC_SIZE:
        logger.error(
            f"Attempted to unpack record of incorrect size: {len(rec)} bytes. {rec}"
        )
        if PROMETHEUS_AVAILABLE:
            wal_error_count.labels(type="unpack_size_mismatch").inc()
        return None
    packed_core = rec[: WAL.RECORD_CORE_SIZE]
    stored_checksum = int.from_bytes(
        rec[WAL.RECORD_CORE_SIZE : WAL.RECORD_CORE_SIZE + WAL.CHECKSUM_SIZE],
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
            k_high,
            k_low,
            v_high,
            v_low,
        ) = struct.unpack("<BBQIIQQQQ", packed_core)
    except struct.error as e:
        logger.error(f"Error unpacking WAL record struct: {e}")
        if PROMETHEUS_AVAILABLE:
            wal_error_count.labels(type="unpack_struct_error").inc()
        return None
    version = OpVer((version_op >> 6) & 0x03)
    optype = OpType(version_op & 0x3F)
    return OP(
        version=version,
        optype=optype,
        reserved=reserved,
        nanos=nanos,
        seq=seq,
        shard_id=shard_id,
        k_high=k_high,
        k_low=k_low,
        v_high=v_high,
        v_low=v_low,
        checksum=stored_checksum,
    )
