"""wal_analyzer.py - WAL pattern analysis functions (stateless)"""

from __future__ import annotations

try:
    # zvic is an optional development-time tool; allow missing imports in CI.
    from zvic import constrain_this_module  # type: ignore[import]
except Exception:
    # zvic is optional; provide a no-op fallback when it's not installed
    def constrain_this_module():
        return None


import time
from copy import replace
from dataclasses import dataclass
from functools import reduce
from operator import add
from typing import Any, Dict, List, Optional

from cidstore.constants import OpType


@dataclass
class Operation:
    """Represents a single WAL operation."""

    op_type: OpType
    bucket_id: int
    timestamp: float


@dataclass
class BucketStats:
    """Statistics for a single bucket."""

    operation_count: int = 0
    insert_timestamps: Optional[List[float]] = None
    last_access: float = 0.0

    def __post_init__(self):
        if self.insert_timestamps is None:
            self.insert_timestamps = []


def parse_bucket_id(bucket_name: str) -> Optional[int]:
    """Parse bucket ID from bucket name."""
    try:
        return int(bucket_name.split("_")[1])
    except (ValueError, IndexError):
        return None


def trim_list(lst: List[Any], max_size: int) -> List[Any]:
    """Functional helper to trim list to max size."""
    return lst[-max_size:] if len(lst) > max_size else lst


def calculate_frequency_score(operation_count: int) -> float:
    """Calculate score based on operation frequency."""
    return min(operation_count / 100.0, 5.0)


def calculate_insert_rate_score(
    insert_timestamps: List[float], current_time: float
) -> float:
    """Calculate score based on recent insert rate."""
    if len(insert_timestamps) < 10:
        return 0.0

    recent_inserts = [t for t in insert_timestamps if current_time - t < 60.0]
    insert_rate = len(recent_inserts) / 60.0  # Inserts per second
    return insert_rate * 10.0


def calculate_recency_score(last_access: float, current_time: float) -> float:
    """Calculate score based on recency of access."""
    if last_access == 0:
        return 0.0

    time_since_access = current_time - last_access
    if time_since_access >= 300:  # More than 5 minutes ago
        return 0.0

    recency_factor = (300 - time_since_access) / 300.0
    return recency_factor * 2.0


def calculate_sequential_score(
    bucket_id: int, operation_history: List[Operation]
) -> float:
    """Calculate score based on sequential access patterns."""
    bucket_operations = [
        op for op in operation_history[-100:] if op.bucket_id == bucket_id
    ]

    if len(bucket_operations) < 5:
        return 0.0

    recent_bucket_ids = [op.bucket_id for op in operation_history[-50:]]
    if not recent_bucket_ids:
        return 0.0

    sequential_count = len([
        i
        for i in range(1, len(recent_bucket_ids))
        if abs(recent_bucket_ids[i] - recent_bucket_ids[i - 1]) <= 1
    ])

    sequential_ratio = sequential_count / len(recent_bucket_ids)
    return sequential_ratio * 3.0


def calculate_danger_score(
    bucket_stats: BucketStats, bucket_id: int, operation_history: List[Operation]
) -> float:
    """Calculate danger score using functional composition."""
    current_time = time.time()

    # Calculate individual score components
    frequency_score = calculate_frequency_score(bucket_stats.operation_count)
    insert_rate_score = calculate_insert_rate_score(
        bucket_stats.insert_timestamps or [], current_time
    )
    recency_score = calculate_recency_score(bucket_stats.last_access, current_time)
    sequential_score = calculate_sequential_score(bucket_id, operation_history)

    # Combine scores functionally
    total_score = reduce(
        add, [frequency_score, insert_rate_score, recency_score, sequential_score]
    )
    return min(total_score, 20.0)  # Cap at 20.0


def get_adaptive_split_threshold(danger_score: float) -> int:
    """Calculate adaptive split threshold based on danger score."""
    base_threshold = 128

    # Use functional mapping for threshold calculation
    threshold_map = [
        (0.8, 0.5),  # Very aggressive
        (0.6, 0.625),  # Aggressive
        (0.4, 0.75),  # Moderate
        (0.2, 0.875),  # Conservative
        (0.0, 1.0),  # Default
    ]

    multiplier = next(
        (mult for threshold, mult in threshold_map if danger_score >= threshold),
        1.0,
    )
    return max(64, int(base_threshold * multiplier))


def get_high_danger_buckets(
    bucket_stats: Dict[int, BucketStats],
    operation_history: List[Operation],
    threshold: float = 0.5,
) -> List[str]:
    """Get list of bucket names with danger scores above threshold."""

    def to_bucket_name(bucket_id: int) -> str:
        return f"bucket_{bucket_id:04d}"

    high_danger_buckets = [
        to_bucket_name(bucket_id)
        for bucket_id, stats in bucket_stats.items()
        if calculate_danger_score(stats, bucket_id, operation_history) >= threshold
    ]

    # Sort by danger score (highest first)
    return sorted(
        high_danger_buckets,
        key=lambda name: get_danger_score_by_name(
            name, bucket_stats, operation_history
        ),
        reverse=True,
    )


def get_danger_score_by_name(
    bucket_name: str,
    bucket_stats: Dict[int, BucketStats],
    operation_history: List[Operation],
) -> float:
    """Get danger score for a bucket by name."""
    bucket_id = parse_bucket_id(bucket_name)
    if bucket_id is None or bucket_id not in bucket_stats:
        return 0.0
    return calculate_danger_score(bucket_stats[bucket_id], bucket_id, operation_history)


def should_preemptively_split(
    bucket_name: str,
    current_size: int,
    bucket_stats: Dict[int, BucketStats],
    operation_history: List[Operation],
) -> bool:
    """Determine if a bucket should be preemptively split."""
    danger_score = get_danger_score_by_name(
        bucket_name, bucket_stats, operation_history
    )
    adaptive_threshold = get_adaptive_split_threshold(danger_score)
    return current_size >= adaptive_threshold


def should_merge_buckets(
    bucket_name1: str,
    bucket_name2: str,
    size1: int,
    size2: int,
    bucket_stats: Dict[int, BucketStats],
    operation_history: List[Operation],
) -> bool:
    """Determine if buckets should be merged based on adaptive criteria."""
    score1 = get_danger_score_by_name(bucket_name1, bucket_stats, operation_history)
    score2 = get_danger_score_by_name(bucket_name2, bucket_stats, operation_history)

    # Don't merge hot buckets
    if max(score1, score2) > 8.0:
        return False

    # Calculate adaptive merge threshold
    avg_score = (score1 + score2) / 2.0
    base_threshold = 8
    activity_factor = avg_score / 5.0
    adaptive_threshold = max(4, base_threshold + int(activity_factor * 4))

    return max(size1, size2) <= adaptive_threshold


def get_maintenance_recommendations(
    bucket_stats: Dict[int, BucketStats], operation_history: List[Operation]
) -> Dict[str, Any]:
    """Get maintenance recommendations based on current patterns."""
    high_danger_buckets = get_high_danger_buckets(bucket_stats, operation_history)

    return {
        "high_danger_buckets": high_danger_buckets,
        "should_run_maintenance": len(high_danger_buckets) > 0,
        "suggested_actions": generate_action_suggestions(
            high_danger_buckets, operation_history
        ),
    }


def generate_action_suggestions(
    high_danger_buckets: List[str], operation_history: List[Operation]
) -> List[str]:
    """Generate maintenance action suggestions."""
    suggestions = []

    if high_danger_buckets:
        suggestions.append(f"High danger buckets detected: {high_danger_buckets[:5]}")

    # Check for sequential patterns
    if has_sequential_pattern(operation_history):
        suggestions.append(
            "Sequential access pattern detected - consider preemptive sorting"
        )

    return suggestions


def has_sequential_pattern(operation_history: List[Operation]) -> bool:
    """Check if recent operations show sequential access pattern."""
    recent_ops = operation_history[-50:]
    if len(recent_ops) <= 10:
        return False

    bucket_ids = [op.bucket_id for op in recent_ops]
    unique_buckets = len(set(bucket_ids))
    return unique_buckets < len(bucket_ids) * 0.3


def get_stats_summary(
    bucket_stats: Dict[int, BucketStats], operation_history: List[Operation]
) -> Dict[str, Any]:
    """Get a summary of current statistics."""
    return {
        "total_buckets_tracked": len(bucket_stats),
        "total_operations": len(operation_history),
        "avg_danger_score": sum(
            calculate_danger_score(stats, bucket_id, operation_history)
            for bucket_id, stats in bucket_stats.items()
        )
        / max(len(bucket_stats), 1),
        "max_danger_score": max(
            (
                calculate_danger_score(stats, bucket_id, operation_history)
                for bucket_id, stats in bucket_stats.items()
            ),
            default=0.0,
        ),
    }
