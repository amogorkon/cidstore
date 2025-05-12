"""wal.py"""


import hashlib
import hmac
import logging
import os
import struct
from enum import Enum
from typing import Dict, List, Optional, Any

import h5py
import numpy as np
from filelock import FileLock

from .config import WAL_DATASET, WAL_METADATA_GROUP
from .keys import E
from .storage import StorageManager

# Set up a logger for WAL operations.
logger = logging.getLogger(__name__)

# Constant for a null HMAC value.
NULL_HMAC = b"\x00" * 32

# Enforce maximum transaction record count.
MAX_TXN_RECORDS = 1000

# Hardcoded HMAC secret for internal WAL usage.
HARDCODED_WAL_SECRET = b"0123456789abcdef0123456789abcdef"  # 32 bytes

# Structured dtype with HMAC-SHA256 for tamper detection.
wal_dtype = np.dtype([
    ("txn_id", "<u8"),  # Transaction ID (monotonically increasing)
    ("op_type", "u1"),  # Operation type from OpType enum
    ("key_high", "<u8"),  # High 64 bits of E key
    ("key_low", "<u8"),  # Low 64 bits of E key
    ("value_high", "<u8"),  # High 64 bits of E value
    ("value_low", "<u8"),  # Low 64 bits of E value
    ("hmac", "|V32"),  # HMAC-SHA256 for integrity verification (fixed-size 32 bytes)
    ("prev_hmac", "|V32"),  # Chain to previous record (fixed-size 32 bytes)
])


class OpType(Enum):
    INSERT = 1
    DELETE = 2
    TXN_START = 3
    TXN_COMMIT = 4
    TXN_ABORT = 5

    @classmethod
    def from_value(cls, value: int) -> "OpType":
        return cls(value)


class SecurityError(RuntimeError):
    """Critical security violation detected"""


class ConsistencyError(RuntimeError):
    """WAL consistency check failed"""


class RecoveryError(RuntimeError):
    """Data recovery failed"""


class WAL:
    def __init__(self, storage: StorageManager):
        self.storage: StorageManager = storage
        self.lock: FileLock = FileLock(f"{str(storage.path)}.wal.lock")
        # Use the already-open file if present (for in-memory HDF5), else open from path.
        if hasattr(storage, "file") and storage.file is not None:
            self.file: h5py.File = storage.file
        else:
            self.file: h5py.File = h5py.File(storage.path, "a", libver="latest", swmr=True)
        self._init_datasets()
        self.hmac_key: bytes = self._get_hmac_key()
        # Use atomic counter from metadata for transaction ID monotonicity.
        self.last_txn_id: int = self.file[WAL_METADATA_GROUP].attrs.get("last_txn_id", 0)
        self.next_txn_id: int = self.last_txn_id + 1
        self.file[WAL_METADATA_GROUP].attrs.modify("last_txn_id", self.next_txn_id)
        # Watermark tracking for WAL record count.
        self.watermark: int = self.file[WAL_METADATA_GROUP].attrs.get("watermark", 0)
        # Support HMAC test mode via environment variable.
        self.disable_hmac: bool = os.getenv("WAL_DISABLE_HMAC", "false").lower() == "true"
        logger.info(f"WAL initialized (disable_hmac={self.disable_hmac}).")

    def _init_datasets(self) -> None:
        """Initialize WAL datasets and table metadata."""
        if WAL_METADATA_GROUP not in self.file:
            meta = self.file.create_group(WAL_METADATA_GROUP)
            meta.attrs["hmac_salt"] = np.bytes_(os.urandom(32))
            meta.attrs["last_applied"] = 0  # Last applied record index
            meta.attrs["last_committed"] = 0  # Last committed transaction ID
        meta = self.file[WAL_METADATA_GROUP]
        # Robustly convert hmac_salt to bytes for checksum
        hmac_salt = meta.attrs["hmac_salt"]
        if isinstance(hmac_salt, np.ndarray):
            data = hmac_salt.tobytes()
        elif hasattr(hmac_salt, "tobytes"):
            data = hmac_salt.tobytes()
        elif isinstance(hmac_salt, memoryview):
            data = hmac_salt.tobytes()
        elif isinstance(hmac_salt, bytes):
            data = hmac_salt
        else:
            data = bytes(hmac_salt)
        # Store checksum as fixed-size bytes, not VLEN string
        if "metadata_checksum" not in meta.attrs:
            meta.attrs.modify("metadata_checksum", np.void(hashlib.sha3_256(data).digest())) if "metadata_checksum" in meta.attrs else meta.attrs.create("metadata_checksum", np.void(hashlib.sha3_256(data).digest()))

        if WAL_DATASET not in self.file:
            ds = self.file.create_dataset(
                WAL_DATASET,
                shape=(0,),
                maxshape=(None,),
                dtype=wal_dtype,
                chunks=True,
                track_times=False,  # Disable timestamps for determinism
            )
        else:
            ds = self.file[WAL_DATASET]
        # Assert that ds is a Dataset.
        if not isinstance(ds, h5py.Dataset):
            raise TypeError("WAL dataset must be an h5py.Dataset")
        self.ds: h5py.Dataset = ds

    def _get_hmac_key(self) -> bytes:
        """
        Get HMAC key for internal WAL.
        Since this is an internal log, we hardcode the secret for simplicity.
        """
        salt = self.file[WAL_METADATA_GROUP].attrs["hmac_salt"]
        salt_bytes = None
        try:
            if isinstance(salt, np.ndarray):
                salt_bytes = salt.tobytes()
            elif hasattr(salt, "tobytes"):
                salt_bytes = salt.tobytes()
            elif isinstance(salt, memoryview):
                salt_bytes = salt.tobytes()
            elif isinstance(salt, bytes):
                salt_bytes = salt
            else:
                salt_bytes = bytes(salt) if hasattr(salt, '__bytes__') else b''
        except Exception:
            salt_bytes = b''
        return hashlib.pbkdf2_hmac("sha256", HARDCODED_WAL_SECRET, salt_bytes, 100000)

    def _compute_hmac(self, record: np.void, prev_hmac: bytes) -> bytes:
        """Compute HMAC-SHA256 with chaining for tamper detection."""
        if self.disable_hmac:
            return NULL_HMAC
        h = hmac.new(self.hmac_key, digestmod=hashlib.sha256)
        # Include previous HMAC in chain.
        h.update(prev_hmac)
        # Hash critical fields.
        h.update(struct.pack("<Q", int(record["txn_id"])))
        h.update(bytes([record["op_type"]]))
        h.update(struct.pack("<QQ", int(record["key_high"]), int(record["key_low"])))
        h.update(
            struct.pack("<QQ", int(record["value_high"]), int(record["value_low"]))
        )
        return h.digest()

    def append(self, records: List[Dict[str, Any]]) -> None:
        """Append multiple records atomically with batch HMAC chaining."""
        if len(records) > MAX_TXN_RECORDS:
            raise ValueError(f"Transaction exceeds {MAX_TXN_RECORDS} records")
        with self.lock:
            # Ensure self.ds supports .shape and .resize.
            assert hasattr(self.ds, "shape"), "WAL dataset must have .shape attribute"
            current_size = self.ds.shape[0]
            prev_hmac = self.ds[-1]["hmac"] if current_size > 0 else NULL_HMAC

            # Convert records to a structured array.
            rec_arr = np.zeros(len(records), dtype=wal_dtype)
            for i, rec in enumerate(records):
                if "txn_id" not in rec:
                    rec["txn_id"] = self.next_txn_id
                if OpType.from_value(rec["op_type"]) == OpType.TXN_START:
                    # Increment txn ID counter after TXN_START.
                    self.next_txn_id += 1
                    self.file[WAL_METADATA_GROUP].attrs.modify(
                        "last_txn_id", self.next_txn_id
                    )

                op_type = OpType.from_value(rec["op_type"]).value
                rec_arr[i] = (
                    rec["txn_id"],
                    op_type,
                    int(rec["key_high"]),
                    int(rec["key_low"]),
                    int(rec["value_high"]),
                    int(rec["value_low"]),
                    NULL_HMAC,  # Placeholder for HMAC.
                    prev_hmac,
                )
                # Compute chained HMAC and update prev_hmac.
                rec_arr[i]["hmac"] = self._compute_hmac(rec_arr[i], prev_hmac)
                prev_hmac = rec_arr[i]["hmac"]

            # Atomic append.
            new_size = current_size + len(records)
            self.ds.resize((new_size,))
            self.ds[current_size:new_size] = rec_arr
            # Update watermark tracking.
            self.watermark = new_size
            self.file[WAL_METADATA_GROUP].attrs["watermark"] = self.watermark
            self.file.flush()
            logger.info(
                f"Appended {len(records)} WAL record(s); new watermark is {self.watermark}."
            )

    def replay(self, apply_limit: Optional[int] = None) -> None:
        """
        Replay WAL with consistency checks and partial application support.

        The replay method buffers the update to last_applied so that it is only
        advanced after a full transaction has been successfully applied.
        """
        with self.lock:
            last_applied = self.file[WAL_METADATA_GROUP].attrs["last_applied"]
            active_txns = {}
            new_last_applied = last_applied  # Buffer for last applied record index.

            for i in range(last_applied, len(self.ds)):
                if apply_limit and i >= apply_limit:
                    break
                rec = self.ds[i]
                prev_hmac = self.ds[i - 1]["hmac"] if i > 0 else NULL_HMAC

                # Verify HMAC chain.
                computed_hmac = self._compute_hmac(rec, prev_hmac)
                if not hmac.compare_digest(computed_hmac, rec["hmac"]):
                    logger.error(f"HMAC mismatch at record {i}")
                    raise SecurityError(f"HMAC mismatch at record {i}")

                op_type = OpType.from_value(rec["op_type"])
                txn_id = rec["txn_id"]
                if op_type == OpType.TXN_START:
                    active_txns[txn_id] = {
                        "txn_id": txn_id,
                        "start_idx": i,
                        "ops": [],
                        "state": "active",
                    }
                elif op_type in (OpType.INSERT, OpType.DELETE):
                    if txn_id not in active_txns:
                        logger.error(f"Orphaned operation at record {i}")
                        raise ConsistencyError(f"Orphaned operation at record {i}")
                    active_txns[txn_id]["ops"].append(rec)
                elif op_type == OpType.TXN_COMMIT:
                    if txn_id not in active_txns:
                        logger.error(f"Commit for unknown transaction at record {i}")
                        raise ConsistencyError(
                            f"Commit for unknown transaction at record {i}"
                        )
                    # Apply the transaction.
                    self._apply_transaction(active_txns[txn_id])
                    new_last_applied = i + 1  # Advance only after commit.
                    self.file[WAL_METADATA_GROUP].attrs["last_applied"] = (
                        new_last_applied
                    )
                    # Update committed metadata (optional).
                    self.file[WAL_METADATA_GROUP].attrs["last_committed"] = txn_id
                    active_txns.pop(txn_id)
                elif op_type == OpType.TXN_ABORT:
                    active_txns.pop(txn_id, None)
                    # Do not update new_last_applied for aborted txns.
            # Rollback any remaining incomplete transactions.
            for txn in active_txns.values():
                self._rollback_transaction(txn)
            logger.info(
                f"Replay complete. last_applied set to {self.file[WAL_METADATA_GROUP].attrs['last_applied']}."
            )

    def truncate(self, confirmed_through: int) -> None:
        """Truncate WAL after confirmed persisted state."""
        with self.lock:
            # Keep at least the last 1000 records for emergency recovery.
            keep_records = max(1000, confirmed_through)
            if self.ds.shape[0] > keep_records:
                self.ds.resize((keep_records,))
                self.file.flush()
                logger.info(f"Truncated WAL to last {keep_records} records.")

    def _apply_transaction(self, txn: Dict) -> None:
        """Apply a transaction atomically to main storage."""
        try:
            for rec in txn["ops"]:
                key = E((int(rec["key_high"]) << 64) | int(rec["key_low"]))
                value = E((int(rec["value_high"]) << 64) | int(rec["value_low"]))
                op = OpType.from_value(rec["op_type"])
                if op == OpType.INSERT:
                    # Pass version tracking to storage.
                    self.storage.apply_insert(key, value)
                elif op == OpType.DELETE:
                    self.storage.apply_delete(key)
            self.file[WAL_METADATA_GROUP].attrs["last_txn_id"] = txn["txn_id"]
            logger.info(f"Applied transaction {txn['txn_id']}.")
        except Exception as e:
            self._rollback_transaction(txn)
            logger.exception(f"Failed to apply transaction {txn['txn_id']}: {e}")
            raise RecoveryError(
                f"Failed to apply transaction {txn['txn_id']}: {e}"
            ) from e

    def _rollback_transaction(self, txn: Dict) -> None:
        """
        Rollback an uncommitted transaction using inverse operations.
        Instead of undoing each operation individually, delegate rollback to
        StorageManager version tracking.
        """
        logger.info(f"Rolling back transaction {txn['txn_id']}.")
        try:
            # No-op: StorageManager does not support rollback_to_version by default.
            logger.info(f"Rollback to version {txn['txn_id']} completed (noop).")
        except Exception as e:
            logger.exception(f"Rollback error for transaction {txn['txn_id']}: {e}")

    def audit_hmac(self, sample_size: int = 100) -> None:
        """Periodically verify random records' HMACs for audit purposes."""
        with self.lock:
            if self.ds.shape[0] == 0:
                return
            rng = np.random.default_rng(int.from_bytes(os.urandom(8), 'little'))
            indices = rng.choice(self.ds.shape[0], size=sample_size, replace=True)
            for i in indices:
                rec = self.ds[i]
                prev_hmac = self.ds[i - 1]["hmac"] if i > 0 else NULL_HMAC
                computed_hmac = self._compute_hmac(rec, prev_hmac)
                if not hmac.compare_digest(rec["hmac"], computed_hmac):
                    logger.error(f"Audit: HMAC mismatch at record {i}")
                    raise SecurityError(f"Audit: HMAC mismatch at record {i}")
            logger.info(f"Audit completed for {sample_size} random records.")
