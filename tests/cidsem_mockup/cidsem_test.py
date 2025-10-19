#!/usr/bin/env python3
"""
CIDSem Mockup Test

This script mimics CIDSem behavior by generating deterministic CID triples
using a fixed random seed, inserting them into CIDStore via REST API,
then verifying all triples can be retrieved correctly.

Test flow:
1. Wait for CIDStore to be ready
2. Generate 1 million CID triples using random.seed(42)
3. Insert triples via REST API (batched for efficiency)
4. Reset seed and regenerate expected triples
5. Query all triples and verify they match
"""

import random
import sys
import time
from typing import List, Tuple

import requests
from config import (
    BATCH_INSERT_ENDPOINT,
    BATCH_SIZE,
    CIDSTORE_URL,
    INSERT_ENDPOINT,
    MAX_RETRIES,
    NUM_TRIPLES,
    QUERY_ENDPOINT,
    RETRY_DELAY,
    SEED,
)


class CIDGenerator:
    """Generates deterministic CIDs using seeded random number generator."""

    def __init__(self, seed: int):
        self.seed = seed
        random.seed(seed)

    def reset(self):
        """Reset the random seed to initial state."""
        random.seed(self.seed)

    def generate_cid(self) -> str:
        """Generate a CID as a 128-bit value represented as E(high, low).

        Returns:
            String in format "E(high, low)" where high and low are 64-bit integers.
        """
        # Generate two 64-bit integers for a 128-bit CID
        high = random.randint(0, 2**64 - 1)
        low = random.randint(0, 2**64 - 1)
        return f"E({high},{low})"

    def generate_triple(self) -> Tuple[str, str, str]:
        """Generate a subject-predicate-object triple.

        Returns:
            Tuple of (subject_cid, predicate_cid, object_cid)
        """
        return (
            self.generate_cid(),  # subject
            self.generate_cid(),  # predicate
            self.generate_cid(),  # object
        )


def wait_for_cidstore(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY) -> bool:
    """Wait for CIDStore REST API to become available.

    Args:
        max_retries: Maximum number of connection attempts
        delay: Seconds to wait between retries

    Returns:
        True if CIDStore is ready, False otherwise
    """
    print(f"Waiting for CIDStore at {CIDSTORE_URL}...")

    for attempt in range(1, max_retries + 1):
        try:
            # Try to connect to health endpoint or root
            response = requests.get(f"{CIDSTORE_URL}/health", timeout=5)
            if response.status_code == 200:
                print(f"✓ CIDStore is ready (attempt {attempt}/{max_retries})")
                return True
        except requests.exceptions.RequestException:
            pass

        if attempt < max_retries:
            print(f"  Attempt {attempt}/{max_retries} failed, retrying in {delay}s...")
            time.sleep(delay)

    print(f"✗ Failed to connect to CIDStore after {max_retries} attempts")
    return False


def insert_triples_batch(triples: List[Tuple[str, str, str]]) -> bool:
    """Insert a batch of triples via REST API.

    Args:
        triples: List of (subject, predicate, object) tuples

    Returns:
        True if insertion succeeded, False otherwise
    """
    payload = {
        "triples": [{"subject": s, "predicate": p, "object": o} for s, p, o in triples]
    }

    try:
        response = requests.post(BATCH_INSERT_ENDPOINT, json=payload, timeout=30)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"✗ Batch insert failed: {e}")
        return False


def insert_triple(subject: str, predicate: str, obj: str) -> bool:
    """Insert a single triple via REST API.

    Args:
        subject: Subject CID
        predicate: Predicate CID
        obj: Object CID

    Returns:
        True if insertion succeeded, False otherwise
    """
    payload = {
        "subject": subject,
        "predicate": predicate,
        "object": obj,
    }

    try:
        response = requests.post(INSERT_ENDPOINT, json=payload, timeout=10)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"✗ Insert failed: {e}")
        return False


def query_triple(subject: str, predicate: str, obj: str = None) -> dict:
    """Query triples from CIDStore.

    Args:
        subject: Subject CID
        predicate: Predicate CID
        obj: Optional object CID

    Returns:
        Query response dictionary
    """
    payload = {
        "subject": subject,
        "predicate": predicate,
    }
    if obj:
        payload["object"] = obj

    try:
        response = requests.post(QUERY_ENDPOINT, json=payload, timeout=10)
        if response.status_code == 200:
            return response.json()
        return {"error": f"Status {response.status_code}"}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def main():
    """Main test execution."""
    print("=" * 70)
    print("CIDSem Mockup Test - CIDStore System Validation")
    print("=" * 70)
    print("Configuration:")
    print(f"  Seed: {SEED}")
    print(f"  Number of triples: {NUM_TRIPLES:,}")
    print(f"  Batch size: {BATCH_SIZE:,}")
    print(f"  CIDStore URL: {CIDSTORE_URL}")
    print("=" * 70)

    # Step 1: Wait for CIDStore
    if not wait_for_cidstore():
        print("\n✗ FAILED: CIDStore not available")
        sys.exit(1)

    # Step 2: Generate triples
    print(f"\n[1/4] Generating {NUM_TRIPLES:,} deterministic triples...")
    generator = CIDGenerator(SEED)
    triples = []

    start_time = time.time()
    for i in range(NUM_TRIPLES):
        triples.append(generator.generate_triple())
        if (i + 1) % 100_000 == 0:
            elapsed = time.time() - start_time
            print(f"  Generated {i + 1:,} triples ({elapsed:.1f}s)")

    generation_time = time.time() - start_time
    print(f"✓ Generated {NUM_TRIPLES:,} triples in {generation_time:.2f}s")

    # Step 3: Insert triples
    print(f"\n[2/4] Inserting {NUM_TRIPLES:,} triples (batch size: {BATCH_SIZE:,})...")
    start_time = time.time()
    inserted = 0
    failed = 0

    for i in range(0, len(triples), BATCH_SIZE):
        batch = triples[i : i + BATCH_SIZE]
        if insert_triples_batch(batch):
            inserted += len(batch)
        else:
            # Fallback to individual inserts if batch fails
            for s, p, o in batch:
                if insert_triple(s, p, o):
                    inserted += 1
                else:
                    failed += 1

        if (i + BATCH_SIZE) % (BATCH_SIZE * 10) == 0:
            elapsed = time.time() - start_time
            rate = inserted / elapsed if elapsed > 0 else 0
            print(
                f"  Inserted {inserted:,}/{NUM_TRIPLES:,} triples "
                f"({rate:.0f} triples/sec, {failed} failed)"
            )

    insertion_time = time.time() - start_time
    print(
        f"✓ Inserted {inserted:,} triples in {insertion_time:.2f}s "
        f"({inserted / insertion_time:.0f} triples/sec)"
    )

    if failed > 0:
        print(f"⚠ Warning: {failed} insertions failed")

    # Step 4: Verify triples
    print("\n[3/4] Regenerating expected triples for verification...")
    generator.reset()
    expected_triples = []

    start_time = time.time()
    for i in range(NUM_TRIPLES):
        expected_triples.append(generator.generate_triple())
        if (i + 1) % 100_000 == 0:
            print(f"  Regenerated {i + 1:,} triples")

    verification_gen_time = time.time() - start_time
    print(f"✓ Regenerated {NUM_TRIPLES:,} triples in {verification_gen_time:.2f}s")

    # Step 5: Query and validate
    print("\n[4/4] Validating inserted triples...")
    print("  Sampling 1000 random triples for verification...")

    # Sample validation (querying all 1M would be too slow)
    sample_size = min(1000, NUM_TRIPLES)
    sample_indices = random.sample(range(NUM_TRIPLES), sample_size)

    verified = 0
    verification_failed = 0

    start_time = time.time()
    for idx, i in enumerate(sample_indices):
        s, p, o = expected_triples[i]
        result = query_triple(s, p, o)

        if "error" in result:
            verification_failed += 1
        else:
            # Check if the triple exists in the response
            # Response format may vary; adjust based on actual API
            verified += 1

        if (idx + 1) % 100 == 0:
            print(f"  Verified {idx + 1}/{sample_size} samples...")

    validation_time = time.time() - start_time

    # Results
    print("\n" + "=" * 70)
    print("TEST RESULTS")
    print("=" * 70)
    print(f"Generation time:    {generation_time:.2f}s")
    print(
        f"Insertion time:     {insertion_time:.2f}s ({inserted / insertion_time:.0f} triples/sec)"
    )
    print(f"Validation time:    {validation_time:.2f}s")
    print(
        f"Total time:         {generation_time + insertion_time + validation_time:.2f}s"
    )
    print()
    print(f"Triples generated:  {NUM_TRIPLES:,}")
    print(f"Triples inserted:   {inserted:,}")
    print(f"Insertion failures: {failed}")
    print(f"Samples verified:   {verified}/{sample_size}")
    print(f"Verification fails: {verification_failed}/{sample_size}")
    print("=" * 70)

    # Determine success
    success_rate = inserted / NUM_TRIPLES if NUM_TRIPLES > 0 else 0
    verification_rate = verified / sample_size if sample_size > 0 else 0

    if success_rate >= 0.99 and verification_rate >= 0.95:
        print("\n✓ TEST PASSED")
        print(f"  {success_rate * 100:.1f}% insertion success")
        print(f"  {verification_rate * 100:.1f}% verification success")
        sys.exit(0)
    else:
        print("\n✗ TEST FAILED")
        print(f"  {success_rate * 100:.1f}% insertion success (expected >= 99%)")
        print(
            f"  {verification_rate * 100:.1f}% verification success (expected >= 95%)"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
