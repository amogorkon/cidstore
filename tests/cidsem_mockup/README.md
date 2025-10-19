# CIDSem Mockup Test

This directory contains a system-level integration test that mimics CIDSem behavior by generating deterministic CID triples and validating the complete CIDStore system.

## Overview

The test:
1. Generates 1 million deterministic CID triples using `random.seed(42)`
2. Inserts them into CIDStore via ZMQ (high-performance bulk operations with msgpack serialization)
3. Regenerates the same triples by resetting the seed
4. Samples and verifies that the triples were correctly stored via ZMQ queries

**Protocol Usage:**
- **ZMQ (port 5555)**: All data operations - insertions and queries using msgpack binary serialization
- **REST API (port 8000)**: Health checks only

## Quick Start

```bash
# From this directory
docker-compose up --build

# Clean up after test
docker-compose down -v
```

## Configuration

Edit `config.py` to customize:
- `SEED`: Random seed for deterministic generation (default: 42)
- `NUM_TRIPLES`: Number of triples to generate (default: 1,000,000)
- `BATCH_SIZE`: Batch size for insertions (default: 1,000)
- `CIDSTORE_URL`: CIDStore REST endpoint for health checks (default: http://cidstore:8000)
- `CIDSTORE_ZMQ_ENDPOINT`: CIDStore ZMQ endpoint for data operations (default: tcp://cidstore:5555)

## Files

- `cidsem_test.py`: Main test script
- `config.py`: Test configuration
- `Dockerfile`: Container for test script
- `docker-compose.yml`: Orchestrates CIDStore + test
- `requirements.txt`: Python dependencies

## Expected Output

The test outputs progress for each phase:
1. Triple generation
2. Batch insertion with throughput metrics
3. Triple regeneration for verification
4. Sample validation

Success criteria:
- ≥99% insertion success rate
- ≥95% verification success rate

## Troubleshooting

If the test fails to connect:
- Check `docker-compose logs cidstore` for CIDStore errors
- Ensure both REST health endpoint and ZMQ interface are running
- Verify ports 8000 (REST) and 5555 (ZMQ) are exposed
- Adjust `MAX_RETRIES` and `RETRY_DELAY` in `config.py`

If insertions fail:
- Verify ZMQ server is listening on port 5555
- Check ZMQ message format matches the expected protocol
- Review CIDStore logs for server-side errors
- Test ZMQ connectivity manually
