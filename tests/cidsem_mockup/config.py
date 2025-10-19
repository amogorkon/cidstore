"""Configuration for CIDSem mockup test."""

import os

# CIDStore REST API endpoint
CIDSTORE_URL = os.getenv("CIDSTORE_URL", "http://cidstore:8000")

# Test configuration
SEED = 42
NUM_TRIPLES = 1_000_000

# API endpoints
INSERT_ENDPOINT = f"{CIDSTORE_URL}/triple/insert"
QUERY_ENDPOINT = f"{CIDSTORE_URL}/triple/query"
BATCH_INSERT_ENDPOINT = f"{CIDSTORE_URL}/triple/batch_insert"

# Batch size for inserting triples
BATCH_SIZE = 1000

# Retry configuration for connecting to CIDStore
MAX_RETRIES = 30
RETRY_DELAY = 2  # seconds
