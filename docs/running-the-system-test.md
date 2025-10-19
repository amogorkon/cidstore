# Running the CIDStore System Test

This guide explains how to run the complete system-level integration test that validates CIDStore functionality using a CIDSem-like workload.

## Overview

The system test validates CIDStore by:
1. **Deploying CIDStore** in a Docker container with ZMQ data interface and REST API for control
2. **Generating deterministic data** using a seeded random number generator
3. **Inserting 1 million triples** via ZMQ (high-performance bulk operations)
4. **Verifying correctness** by regenerating and querying triples via ZMQ

This mimics real-world CIDSem usage patterns and validates the complete stack from API to storage.

**Protocol Usage:**
- **ZMQ (port 5555)**: All data operations (insert, query, batch operations) - high throughput
- **REST API (port 8000)**: Health checks and control operations only - debugging/monitoring

## Prerequisites

### Required Software

- **Docker**: Version 20.10 or later
  - Install from [docker.com](https://www.docker.com/get-started)
  - Verify: `docker --version`
- **Docker Compose**: Version 2.0 or later (included with Docker Desktop)
  - Verify: `docker-compose --version`

### System Requirements

- **Memory**: Minimum 4GB RAM available for Docker
- **Disk**: 10GB free space (for container images + HDF5 data)
- **Network**: Internet access for pulling base images

## Step 1: Set Up CIDStore Docker Container

### 1.1 Build the CIDStore Image

The repository includes a complete `Dockerfile` in the root directory that:
- Uses Python 3.13 with free-threading and JIT support
- Installs all necessary dependencies (HDF5, pyzmq, msgpack, FastAPI, etc.)
- Exposes port 8000 for REST API (health checks/control)
- Exposes ports 5555, 5557-5559 for ZMQ data operations
- Starts both REST and ZMQ servers via `docker-entrypoint.sh`
- Includes health check endpoint for container orchestration

### 1.2 Build and Verify Docker Setup

Build the CIDStore image:

```bash
# From repository root
docker build -t cidstore:latest .
```

Test the image:

```bash
docker run -p 8000:8000 -p 5555:5555 cidstore:latest
```

Verify the server is running:

```bash
# Check REST health endpoint
curl http://localhost:8000/health

# Test ZMQ connection (optional, requires zmq tools)
# The actual test will validate ZMQ connectivity
```

You should see a successful response (e.g., `{"status": "healthy"}`).

Stop the test container: `Ctrl+C`

## Step 2: Prepare the System Test

### 2.1 Navigate to Test Directory

```bash
cd tests/cidsem_mockup
```

### 2.2 Review Configuration

Open `config.py` and verify/adjust settings:

```python
# Default configuration
SEED = 42                  # Random seed for reproducibility
NUM_TRIPLES = 1_000_000   # Number of triples to test
BATCH_SIZE = 1000         # Batch size for insertions
```

**For faster testing**, you can reduce `NUM_TRIPLES`:

```python
NUM_TRIPLES = 10_000  # Quick test with 10k triples
```

### 2.3 Understand the Test Flow

The test script (`cidsem_test.py`) performs these steps:

1. **Wait for CIDStore**: Retries REST health check up to 30 times (configurable)
2. **Connect ZMQ socket**: Establishes ZMQ REQ socket to `tcp://cidstore:5555`
3. **Generate triples**: Uses `random.seed(42)` to create deterministic CIDs
   - Format: `E(high, low)` where high/low are 64-bit integers
4. **Insert triples**: Batched ZMQ messages using msgpack serialization for high-performance bulk operations
   - Message: `{"command": "batch_insert", "triples": [{"s": "E(...)", "p": "E(...)", "o": "E(...)"}]}`
   - Serialized with: `msgpack.packb(message, use_bin_type=True)`
   - Response: `{"status": "ok"}` or error (msgpack encoded)
5. **Regenerate triples**: Resets seed and regenerates expected data
6. **Verify samples**: Queries 1000 random triples via ZMQ/msgpack to validate storage
   - Message: `{"command": "query", "s": "E(...)", "p": "E(...)", "o": "E(...)"}`

**Why ZMQ + msgpack?**
- **Performance**: 10-100x faster than REST for bulk operations
- **Efficiency**: Binary protocol, no HTTP overhead, compact msgpack serialization
- **Scalability**: CIDSem design pattern for high-throughput triple stores
- **Proper binary handling**: msgpack handles binary data correctly, unlike JSON

## Step 3: Run the System Test

### 3.1 Start Docker Compose

From the `tests/cidsem_mockup` directory:

```bash
docker-compose up --build
```

**What happens:**
1. Docker Compose builds both containers (CIDStore + test)
2. Starts CIDStore and waits for health check to pass
3. Starts the test container which connects to CIDStore
4. Test runs automatically and outputs progress

### 3.2 Monitor Test Progress

You'll see output like:

```
======================================================================
CIDSem Mockup Test - CIDStore System Validation
======================================================================
Configuration:
  Seed: 42
  Number of triples: 1,000,000
  Batch size: 1,000
  CIDStore URL: http://cidstore:8000
======================================================================
Waiting for CIDStore at http://cidstore:8000...
✓ CIDStore is ready (attempt 3/30)

Connecting to ZMQ endpoint: tcp://cidstore:5555
✓ ZMQ socket connected

[1/4] Generating 1,000,000 deterministic triples...
  Generated 100,000 triples (2.3s)
  Generated 200,000 triples (4.5s)
  ...
✓ Generated 1,000,000 triples in 22.87s

[2/4] Inserting 1,000,000 triples via ZMQ (batch size: 1,000)...
  Inserted 10,000/1,000,000 triples (8234 triples/sec, 0 failed)
  Inserted 20,000/1,000,000 triples (8891 triples/sec, 0 failed)
  ...
✓ Inserted 1,000,000 triples in 121.34s (8241 triples/sec)

[3/4] Regenerating expected triples for verification...
✓ Regenerated 1,000,000 triples in 22.91s

[4/4] Validating inserted triples...
  Sampling 1000 random triples for verification...
  Verified 100/1000 samples...
  ...

======================================================================
TEST RESULTS
======================================================================
Generation time:    22.87s
Insertion time:     121.34s (8241 triples/sec)
Validation time:    8.45s
Total time:         152.66s

Triples generated:  1,000,000
Triples inserted:   1,000,000
Insertion failures: 0
Samples verified:   1000/1000
Verification fails: 0/1000
======================================================================

✓ TEST PASSED
  100.0% insertion success
  100.0% verification success
```

### 3.3 Interpret Results

**Success criteria:**
- ✓ Insertion success rate ≥ 99%
- ✓ Verification success rate ≥ 95%

**If the test passes**: Your CIDStore implementation correctly handles:
- Large-scale batch insertions via ZMQ
- Deterministic storage and retrieval
- ZMQ data protocol and REST health endpoints
- Concurrent operations

**If the test fails**: See troubleshooting section below

## Step 4: Clean Up

After the test completes:

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (clears stored data)
docker-compose down -v
```

## Troubleshooting

### Test Can't Connect to CIDStore

**Symptoms:**
```
Waiting for CIDStore at http://cidstore:8000...
  Attempt 1/30 failed, retrying in 2s...
  ...
✗ Failed to connect to CIDStore after 30 attempts
✗ FAILED: CIDStore not available
```

**Solutions:**

1. **Check CIDStore logs:**
   ```bash
   docker-compose logs cidstore
   ```
   Look for errors during startup or API initialization

2. **Verify health endpoint:**
   ```bash
   docker exec -it cidstore curl http://localhost:8000/health
   ```
   Should return HTTP 200 with status message

3. **Check port mappings:**
   ```bash
   docker-compose ps
   ```
   Verify cidstore is running with both ports mapped:
   - `0.0.0.0:8000->8000/tcp` (REST health)
   - `0.0.0.0:5555->5555/tcp` (ZMQ data)

4. **Increase retry settings:**
   Edit `config.py`:
   ```python
   MAX_RETRIES = 60  # Increase from 30
   RETRY_DELAY = 5   # Increase delay
   ```

### Insertion Failures

**Symptoms:**
```
✗ Batch insert failed: Connection refused
✓ Inserted 850,000 triples in 180.45s (4722 triples/sec)
⚠ Warning: 150000 insertions failed
```

**Solutions:**

1. **Check ZMQ endpoint:**
   Verify your server is listening on ZMQ port 5555:
   ```bash
   docker exec -it cidstore netstat -ln | grep 5555
   ```
   Or check CIDStore logs for ZMQ binding messages

2. **Review ZMQ message format:**
   Ensure your ZMQ handler expects msgpack-encoded messages:
   ```python
   # Server side:
   message = msgpack.unpackb(socket.recv(), raw=False)
   # message = {
   #   "command": "batch_insert",
   #   "triples": [{"s": "E(...)", "p": "E(...)", "o": "E(...)"}]
   # }

   # Respond with msgpack:
   socket.send(msgpack.packb({"status": "ok"}, use_bin_type=True))
   ```

3. **Check CIDStore capacity:**
   ```bash
   docker stats cidstore
   ```
   Ensure sufficient memory and disk space

4. **View detailed errors:**
   ```bash
   docker-compose logs -f cidstore
   ```
   Watch for errors during insertions

### Verification Failures

**Symptoms:**
```
Samples verified:   650/1000
Verification fails: 350/1000
✗ TEST FAILED
  100.0% insertion success
  65.0% verification success (expected >= 95%)
```

**Solutions:**

1. **Check ZMQ query handler:**
   Verify your ZMQ server handles query commands (msgpack encoded):
   ```python
   # Message format:
   {"command": "query", "s": "E(...)", "p": "E(...)", "o": "E(...)"}
   # Server should respond with matching triples or empty results (msgpack encoded)
   ```

2. **Test ZMQ connection manually:**
   Use Python inside container:
   ```bash
   docker exec -it cidstore python3 -c "
import zmq
import msgpack
ctx = zmq.Context()
sock = ctx.socket(zmq.REQ)
sock.connect('tcp://localhost:5555')
msg = {'command': 'query', 's': 'E(1,2)', 'p': 'E(3,4)', 'o': 'E(5,6)'}
sock.send(msgpack.packb(msg, use_bin_type=True))
print(msgpack.unpackb(sock.recv(), raw=False))
"
   ```

3. **Review query logic:**
   The test expects exact matches for (S, P, O) tuples

4. **Check data persistence:**
   Ensure HDF5 file is being written correctly:
   ```bash
   docker exec -it cidstore ls -lh /data/
   ```

### Performance Issues

**Symptoms:**
- Very slow insertion rate (<1000 triples/sec)
- Test timeout before completion

**Solutions:**

1. **Reduce test size:**
   Edit `config.py`:
   ```python
   NUM_TRIPLES = 100_000  # Start smaller
   ```

2. **Increase batch size:**
   ```python
   BATCH_SIZE = 5000  # Larger batches
   ```

3. **Check Docker resources:**
   - Increase memory limit in Docker Desktop settings
   - Allocate more CPUs

4. **Optimize CIDStore:**
   - Enable write caching
   - Adjust HDF5 chunk sizes
   - Review WAL configuration

## Advanced Usage

### Running Specific Test Sizes

**Small test (quick validation):**
```bash
# Edit config.py
NUM_TRIPLES = 1_000
BATCH_SIZE = 100

docker-compose up --build
```

**Large test (stress testing):**
```bash
# Edit config.py
NUM_TRIPLES = 10_000_000
BATCH_SIZE = 10_000

docker-compose up --build
```

### Custom CIDStore Configuration

Modify `docker-compose.yml` to pass environment variables:

```yaml
services:
  cidstore:
    environment:
      - CIDSTORE_WAL_SIZE=134217728  # 128MB WAL
      - CIDSTORE_BATCH_SIZE=5000
      - CIDSTORE_LOG_LEVEL=DEBUG
```

### Running Without Docker Compose

**Start CIDStore manually:**
```bash
docker run -d \
  --name cidstore \
  -p 8000:8000 \
  -v cidstore-data:/data \
  cidstore:latest
```

**Run test manually:**
```bash
cd tests/cidsem_mockup
pip install -r requirements.txt
export CIDSTORE_URL=http://localhost:8000
python cidsem_test.py
```

### Viewing Logs

**Real-time logs:**
```bash
docker-compose logs -f
```

**CIDStore only:**
```bash
docker-compose logs -f cidstore
```

**Test only:**
```bash
docker-compose logs -f cidsem-test
```

## Integration with CI/CD

Add to GitHub Actions workflow:

```yaml
name: System Test

on: [push, pull_request]

jobs:
  system-test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Run CIDStore system test
        run: |
          cd tests/cidsem_mockup
          docker-compose up --build --abort-on-container-exit
          exit_code=$(docker-compose ps -q cidsem-test | xargs docker inspect -f '{{.State.ExitCode}}')
          docker-compose down -v
          exit $exit_code
```

## Performance Benchmarks

Expected performance on typical hardware:

| Configuration | Insertion Rate | Total Time (1M triples) |
|---------------|----------------|------------------------|
| Default (1K batch) | 8,000-12,000 triples/sec | 80-120 seconds |
| Large batch (10K) | 15,000-25,000 triples/sec | 40-60 seconds |
| Small batch (100) | 2,000-4,000 triples/sec | 250-500 seconds |

*Hardware: 4-core CPU, 8GB RAM, SSD storage*

## Next Steps

After successful system test:
1. **Tune performance**: Adjust batch sizes and CIDStore configuration
2. **Run stress tests**: Increase to 10M+ triples
3. **Test failure scenarios**: Kill containers mid-test to verify recovery
4. **Profile bottlenecks**: Use Docker stats and CIDStore metrics
5. **Implement monitoring**: Add Prometheus/Grafana for production observability

## References

- [CIDStore Architecture](../ARCHITECTURE.md)
- [REST API Documentation](./rest_api.md)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [HDF5 Performance Tuning](https://docs.h5py.org/en/stable/high/dataset.html#chunked-storage)
