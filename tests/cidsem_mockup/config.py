"""Configuration for CIDSem mockup test."""

import os

# CIDStore REST API endpoint (used only for health checks)
CIDSTORE_URL = os.getenv("CIDSTORE_URL", "http://cidstore:8000")

# CIDStore ZMQ endpoint (used for all data operations)
CIDSTORE_ZMQ_ENDPOINT = os.getenv("CIDSTORE_ZMQ_ENDPOINT", "tcp://cidstore:5555")

# Test configuration
SEED = 42
NUM_TRIPLES = 1_000_000

# Batch size for inserting triples
BATCH_SIZE = 1000

# Retry configuration for connecting to CIDStore
MAX_RETRIES = 30
RETRY_DELAY = 2  # seconds
