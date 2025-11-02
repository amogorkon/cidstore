# CIDStore Standalone Docker Setup for External Repos

This guide shows how to run CIDStore from another repository (e.g., cidsem) using Docker.

## Quick Start

### Option 1: Use the Standalone Dockerfile (Recommended)

**Copy `Dockerfile.standalone` to your repo and rename it:**

```powershell
# In your cidsem repo
cp /path/to/cidstore/docs/Dockerfile.standalone ./Dockerfile.cidstore
```

**Build and run:**

```powershell
docker build -f Dockerfile.cidstore -t cidstore:latest .
docker run -d -p 8000:8000 -p 5555:5555 --name cidstore cidstore:latest
```

### Option 2: Use Docker Compose (Recommended for Multi-Service)

**File: `docker-compose.yml` (in your cidsem repo)**

```yaml
version: '3.9'

services:
  # CIDStore service
  cidstore:
    build:
      context: .
      dockerfile: Dockerfile.cidstore
    container_name: cidstore
    ports:
      - "8000:8000"   # REST API
      - "5555:5555"   # ZMQ compatibility
      - "5557:5557"   # ZMQ mutations
      - "5558:5558"   # ZMQ reads
      - "5559:5559"   # ZMQ notifications
    environment:
      - CIDSTORE_PORT=8000
      - CIDSTORE_ZMQ_PORT=5555
      - CIDSTORE_HDF5_PATH=/data/cidstore.h5
      - PYTHONUNBUFFERED=1
      - PYTHON_JIT=1
    volumes:
      - cidstore-data:/data
    networks:
      - app-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
    restart: unless-stopped

  # Your cidsem service
  cidsem:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: cidsem
    environment:
      - CIDSTORE_URL=http://cidstore:8000
      - CIDSTORE_ZMQ_ENDPOINT=tcp://cidstore:5555
    depends_on:
      cidstore:
        condition: service_healthy
    networks:
      - app-network
    restart: unless-stopped

volumes:
  cidstore-data:

networks:
  app-network:
    driver: bridge
```

**Start everything:**

```powershell
docker compose up --build
```

---

## Dockerfile.standalone Explained

The standalone Dockerfile installs CIDStore directly from GitHub, so you don't need to clone the cidstore repo.

```dockerfile
FROM python:3.13-slim

# Install system dependencies (HDF5, build tools, curl)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libhdf5-dev \
        libssl-dev \
        curl \
        git \
        && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

# Install cidstore from GitHub (latest main branch)
ARG CIDSTORE_VERSION=main
RUN pip install "git+https://github.com/amogorkon/cidstore.git@${CIDSTORE_VERSION}"

# Create data directory
RUN mkdir -p /data

# Expose ports
EXPOSE 8000 5555 5557 5558 5559

# Environment
ENV PYTHON_JIT=1
ENV CIDSTORE_HDF5_PATH=/data/cidstore.h5
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -f http://localhost:8000/health || exit 1

# Start script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Starting CIDStore..."\n\
python -m uvicorn cidstore.control_api:app --host 0.0.0.0 --port 8000\n\
' > /docker-entrypoint.sh && chmod +x /docker-entrypoint.sh

CMD ["/docker-entrypoint.sh"]
```

**Key points:**
- Installs cidstore via `pip install git+https://github.com/...`
- Uses `ARG CIDSTORE_VERSION=main` so you can pin a specific version/tag:
  ```powershell
  docker build --build-arg CIDSTORE_VERSION=v2025.35.1 -f Dockerfile.cidstore -t cidstore:2025.35.1 .
  ```
- Starts the REST API with embedded ZMQ workers via `uvicorn`

---

## Usage Examples

### Build with specific version:
```powershell
docker build --build-arg CIDSTORE_VERSION=v2025.35.1 -f Dockerfile.cidstore -t cidstore:2025.35.1 .
```

### Run standalone:
```powershell
docker run -d `
  -p 8000:8000 `
  -p 5555:5555 `
  -v cidstore-data:/data `
  --name cidstore `
  cidstore:latest
```

### Test connectivity:
```powershell
# Check health
curl http://localhost:8000/health

# Check from your cidsem container
docker exec cidsem curl http://cidstore:8000/health
```

### View logs:
```powershell
docker logs -f cidstore
```

### Stop and clean up:
```powershell
docker stop cidstore
docker rm cidstore
docker volume rm cidstore-data
```

---

## Connecting from CIDSem

Your cidsem application should connect using:

**REST API:**
```python
import requests

CIDSTORE_URL = "http://cidstore:8000"  # In docker-compose network
# or "http://localhost:8000" if running on host

response = requests.get(f"{CIDSTORE_URL}/health")
print(response.json())
```

**ZMQ (high-performance):**
```python
import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://cidstore:5555")  # In docker-compose network
# or "tcp://localhost:5555" if running on host

# Send request
socket.send_json({"op": "get", "key": "some_cid"})
result = socket.recv_json()
```

---

## Alternative: Pull Pre-Built Image

If the cidstore repo publishes images to a registry (Docker Hub, GitHub Container Registry, etc.):

**File: `docker-compose.yml`**

```yaml
services:
  cidstore:
    image: ghcr.io/amogorkon/cidstore:latest
    # ... rest of config (no build: section)
```

---

## Troubleshooting

### Build fails with "Could not find a version that satisfies zvic==2025.43.0"

The `zvic` dependency might not be publicly available. Options:
1. Contact the cidstore maintainer for access
2. Build cidstore locally and export the image:
   ```powershell
   # In cidstore repo
   docker build -t cidstore:latest .
   docker save cidstore:latest -o cidstore.tar

   # Copy to cidsem repo and load
   docker load -i cidstore.tar
   ```

### Health check fails

Check logs:
```powershell
docker logs cidstore
```

Common issues:
- Port conflict (8000 or 5555 already in use)
- Missing HDF5 libraries (should be installed in Dockerfile)
- Insufficient startup time (increase `start_period` in healthcheck)

### Connection refused from cidsem to cidstore

Ensure both services are on the same Docker network:
```powershell
docker network inspect app-network
```

Both containers should appear in the network. If using docker-compose, this is automatic.

---

## Summary

**To hand over to cidsem team:**

1. Copy `Dockerfile.standalone` from `docs/Dockerfile.standalone`
2. Rename to `Dockerfile.cidstore` in their repo
3. Add the docker-compose.yml snippet above
4. Run `docker compose up --build`

**Files to provide:**
- `Dockerfile.standalone` (this file)
- Example `docker-compose.yml` (see above)
- Connection examples (Python code above)

That's it! The cidsem team can now run cidstore without cloning the cidstore repo.
