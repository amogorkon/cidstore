# CIDSem Mockup Test

This directory contains a system-level integration test that mimics CIDSem behavior by generating deterministic CID triples and validating the complete CIDStore system.

## Overview

The test:
1. Generates 1 million deterministic CID triples using `random.seed(42)`
2. Inserts them into CIDStore via REST API (batched for efficiency)
3. Regenerates the same triples by resetting the seed
4. Samples and verifies that the triples were correctly stored

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
- `CIDSTORE_URL`: CIDStore endpoint (default: http://cidstore:8000)

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
- Ensure the REST API is implemented and health endpoint exists
- Adjust `MAX_RETRIES` and `RETRY_DELAY` in `config.py`

If insertions fail:
- Verify REST API endpoint paths match your implementation
- Check payload format matches your API schema
- Review CIDStore logs for server-side errors
