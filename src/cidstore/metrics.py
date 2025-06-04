"""metrics.py - Metrics collection and auto-tuning functionality (extracted from store.py)"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

PROMETHEUS_AVAILABLE = False


try:
    from prometheus_client import CollectorRegistry, Counter, Gauge

    PROMETHEUS_AVAILABLE = True

    def create_wal_metrics(registry=None):
        registry = registry or None  # None means use default global registry
        wal_records_appended = Counter(
            "cidtree_wal_records_appended_total",
            "Total number of records appended to the WAL",
            ["operation"],
            registry=registry,
        )
        wal_replay_count = Counter(
            "cidtree_wal_replay_total",
            "Total number of WAL replays completed",
            registry=registry,
        )
        wal_crc_failures = Counter(
            "cidtree_wal_crc_failures_total",
            "Total CRC checksum failures detected during replay",
            registry=registry,
        )
        wal_truncate_count = Counter(
            "cidtree_wal_truncate_total",
            "Total number of WAL truncations completed",
            registry=registry,
        )
        wal_error_count = Counter(
            "cidtree_wal_error_total",
            "Total WAL errors encountered",
            ["type"],
            registry=registry,
        )
        wal_head_position = Gauge(
            "cidtree_wal_head_position",
            "Current head pointer position (byte offset) in the WAL buffer",
            registry=registry,
        )
        wal_tail_position = Gauge(
            "cidtree_wal_tail_position",
            "Current tail pointer position (byte offset) in the WAL buffer",
            registry=registry,
        )
        wal_buffer_capacity_bytes = Gauge(
            "cidtree_wal_buffer_capacity_bytes",
            "Total buffer capacity in bytes",
            registry=registry,
        )
        wal_records_in_buffer = Gauge(
            "cidtree_wal_records_in_buffer",
            "Current number of records in the WAL buffer",
            registry=registry,
        )
        return {
            "wal_records_appended": wal_records_appended,
            "wal_replay_count": wal_replay_count,
            "wal_crc_failures": wal_crc_failures,
            "wal_truncate_count": wal_truncate_count,
            "wal_error_count": wal_error_count,
            "wal_head_position": wal_head_position,
            "wal_tail_position": wal_tail_position,
            "wal_buffer_capacity_bytes": wal_buffer_capacity_bytes,
            "wal_records_in_buffer": wal_records_in_buffer,
        }

    # Default global metrics (for production)
    _default_metrics = create_wal_metrics()
    wal_records_appended = _default_metrics["wal_records_appended"]
    wal_replay_count = _default_metrics["wal_replay_count"]
    wal_crc_failures = _default_metrics["wal_crc_failures"]
    wal_truncate_count = _default_metrics["wal_truncate_count"]
    wal_error_count = _default_metrics["wal_error_count"]
    wal_head_position = _default_metrics["wal_head_position"]
    wal_tail_position = _default_metrics["wal_tail_position"]
    wal_buffer_capacity_bytes = _default_metrics["wal_buffer_capacity_bytes"]
    wal_records_in_buffer = _default_metrics["wal_records_in_buffer"]
except ImportError:
    # Create dummy objects for when prometheus_client is not available
    class DummyMetric:
        def inc(self, *args, **kwargs):
            pass

        def set(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

    wal_records_appended = DummyMetric()
    wal_replay_count = DummyMetric()
    wal_crc_failures = DummyMetric()
    wal_truncate_count = DummyMetric()
    wal_error_count = DummyMetric()
    wal_head_position = DummyMetric()
    wal_tail_position = DummyMetric()
    wal_buffer_capacity_bytes = DummyMetric()
    wal_records_in_buffer = DummyMetric()


def get_wal_prometheus_metrics(registry=None) -> list[Any]:
    """Get all WAL Prometheus metrics for a given registry (or default)."""
    if PROMETHEUS_AVAILABLE:
        metrics = create_wal_metrics(registry=registry)
        return list(metrics.values())
    return []


class MetricsCollector:
    """
    Collects and manages performance metrics for CIDStore.
    Provides data for monitoring and auto-tuning.
    """

    def __init__(self):
        self._metrics = {
            "latency_p99": 0.0,
            "throughput_ops": 0.0,
            "error_rate": 0.0,
            "buffer_occupancy": 0.0,
            "flush_duration": 0.0,
            "lock_contention_ratio": 0.0,
        }
        self._split_events = 0
        self._merge_events = 0
        self._gc_runs = 0
        self._last_error = ""

    def update_metric(self, name: str, value: float) -> None:
        """Update a specific metric value."""
        if name in self._metrics:
            self._metrics[name] = value

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        return self._metrics.copy()

    def increment_split_events(self) -> None:
        """Increment split events counter."""
        self._split_events += 1

    def increment_merge_events(self) -> None:
        """Increment merge events counter."""
        self._merge_events += 1

    def increment_gc_runs(self) -> None:
        """Increment GC runs counter."""
        self._gc_runs += 1

    def set_last_error(self, error: str) -> None:
        """Set the last error message."""
        self._last_error = error

    def expose_prometheus_metrics(self, registry=None) -> list[Any]:
        """
        Expose metrics for Prometheus scraping.
        Returns:
            list: Prometheus metric objects if available, else empty list.
        """
        try:
            from prometheus_client import Gauge, Info

            cidstore_info = Info(
                "cidstore_info", "CIDStore information", registry=registry
            )
            split_events_counter = Gauge(
                "cidstore_split_events", "Split events count", registry=registry
            )
            merge_events_counter = Gauge(
                "cidstore_merge_events", "Merge events count", registry=registry
            )
            gc_runs_counter = Gauge(
                "cidstore_gc_runs", "GC runs count", registry=registry
            )
            last_error_info = Info(
                "cidstore_last_error", "Last error info", registry=registry
            )

            # Set metrics values
            cidstore_info.info({"version": "1.0"})
            split_events_counter.set(self._split_events)
            merge_events_counter.set(self._merge_events)
            gc_runs_counter.set(self._gc_runs)
            last_error_info.info({"error": self._last_error})

            return [
                cidstore_info,
                split_events_counter,
                merge_events_counter,
                gc_runs_counter,
                last_error_info,
            ]
        except ImportError:
            return []


class AutoTuner:
    """
    Auto-tuning functionality for CIDStore performance optimization.
    Uses PID control loop to adjust batch size and flush intervals.
    """

    def __init__(self):
        self._autotune_state = {
            "batch_size": 64,
            "flush_interval": 1.0,
            "error_violations": 0,
            "latency_violations": 0,
            "last_latency": 0.0,
            "integral": 0.0,
            "prev_error": 0.0,
        }
        # PID controller parameters
        self.kp = 0.5
        self.ki = 0.1
        self.kd = 0.3
        self.latency_target = 0.0001  # 100μs target
        self.min_batch = 32
        self.max_batch = 1024

    async def auto_tune(self, metrics: Dict[str, Any]) -> None:
        """Async wrapper for auto-tuning logic."""
        await asyncio.to_thread(self._auto_tune_sync, metrics)

    def _auto_tune_sync(self, metrics: Dict[str, Any]) -> None:
        """
        Core auto-tuning logic using PID control.
        Adjusts batch_size and flush_interval based on performance metrics.
        """
        state = self._autotune_state

        # Extract metrics
        latency = metrics.get("latency_p99", 0.0)
        throughput = metrics.get("throughput_ops", 0.0)
        error_rate = metrics.get("error_rate", 0.0)
        buffer_occupancy = metrics.get("buffer_occupancy", 0.0)

        # PID control for batch size
        error = latency - self.latency_target
        state["integral"] += error
        derivative = error - state["prev_error"]
        state["prev_error"] = error
        adjustment = (
            self.kp * error + self.ki * state["integral"] + self.kd * derivative
        )

        # Adjust batch size based on latency and error rate
        batch_size = int(state["batch_size"])
        latency_violations = int(state["latency_violations"])

        if error > 0 or error_rate > 0.01:
            latency_violations += 1
        else:
            latency_violations = 0

        # Shrink batch size if too many violations or high error rate
        if latency_violations >= 3 or error_rate > 0.05:
            batch_size = max(self.min_batch, batch_size // 2)
            latency_violations = 0
        # Grow batch size if performance is good and buffer is filling
        elif adjustment < 0 and buffer_occupancy > 0.8:
            batch_size = min(self.max_batch, batch_size + 32)

        state["batch_size"] = batch_size
        state["latency_violations"] = latency_violations

        # Adjust flush interval based on error
        flush_interval = float(state["flush_interval"])
        if error > 0:
            flush_interval = min(2.0, flush_interval * 1.1)
        else:
            flush_interval = max(0.1, flush_interval * 0.95)
        state["flush_interval"] = flush_interval

        # Circuit breaker for high latency
        error_violations = int(state["error_violations"])
        if latency > 0.00015:  # 150μs
            error_violations += 1
        else:
            error_violations = 0
        state["error_violations"] = error_violations

        # Reset to safe defaults if too many violations
        if error_violations >= 5:
            state["batch_size"] = 64
            state["flush_interval"] = 1.0
            state["error_violations"] = 0

        print(
            f"[AutoTune] batch_size={state['batch_size']} flush_interval={state['flush_interval']} "
            f"latency={latency:.6f} throughput={throughput} error_rate={error_rate}"
        )

    def get_batch_size(self) -> int:
        """Get current batch size."""
        return int(self._autotune_state["batch_size"])

    def get_flush_interval(self) -> float:
        """Get current flush interval."""
        return float(self._autotune_state["flush_interval"])

    def get_autotune_state(self) -> Dict[str, Any]:
        """Get current auto-tune state for monitoring."""
        return self._autotune_state.copy()

    def expose_metrics(
        self, registry=None, metrics_collector=None, buckets=None, directory=None
    ) -> list[Any]:
        """
        Expose metrics for Prometheus scraping.
        Delegate to MetricsCollector with additional bucket/directory data.
        Returns:
            list: Prometheus metric objects if available, else empty list.
        """
        try:
            from prometheus_client import Gauge

            if metrics_collector is not None:
                base_metrics = metrics_collector.expose_prometheus_metrics(
                    registry=registry
                )
            else:
                base_metrics = []
            bucket_gauge = Gauge(
                "cidstore_buckets", "Number of buckets", registry=registry
            )
            directory_size_gauge = Gauge(
                "cidstore_directory_size", "Directory size", registry=registry
            )
            bucket_gauge.set(len(buckets) if buckets is not None else 0)
            directory_size_gauge.set(len(directory) if directory is not None else 0)
            return base_metrics + [bucket_gauge, directory_size_gauge]
        except ImportError:
            return []


def init_metrics_and_autotune(store_instance):
    """Initialize metrics and auto-tuner for a CIDStore instance."""
    store_instance.metrics_collector = MetricsCollector()
    store_instance.auto_tuner = AutoTuner()


def update_metric(store_instance, name: str, value: float) -> None:
    if hasattr(store_instance, "metrics_collector"):
        store_instance.metrics_collector.update_metric(name, value)


async def get_metrics(store_instance):
    if hasattr(store_instance, "metrics_collector"):
        return store_instance.metrics_collector.get_metrics()
    return {}


async def auto_tune(store_instance):
    if hasattr(store_instance, "auto_tuner") and hasattr(
        store_instance, "metrics_collector"
    ):
        metrics = store_instance.metrics_collector.get_metrics()
        await store_instance.auto_tuner.auto_tune(metrics)
