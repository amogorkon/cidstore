# CIDStore Docker Handoff Package

## Quick Summary

CIDStore is now running successfully in Docker using `python:3.13-slim` base image with:
- ✅ REST API on port 8000
- ✅ Three dedicated ZMQ ports (5555, 5556, 5557)
- ✅ Health checks working
- ✅ HDF5 storage initialized

## Files to Hand Off

Copy these files to the cidsem repo:

1. **`docs/Dockerfile.standalone`** → rename as `Dockerfile.cidstore`
2. This README (handoff instructions)

## Build and Run Commands

### From cidsem repo (after copying Dockerfile.cidstore):

```powershell
# Build
docker build -f Dockerfile.cidstore -t cidstore:latest .

# Run standalone
docker run -d --name cidstore `
  -p 8000:8000 `
  -p 5555:5555 `
  -p 5556:5556 `
  -p 5557:5557 `
  cidstore:latest

# Test health
curl http://localhost:8000/health
```

## Docker Compose Integration

**File: `docker-compose.yml` (in cidsem repo)**

```yaml
version: '3.9'

services:
  cidstore:
    build:
      context: .
      dockerfile: Dockerfile.cidstore
    container_name: cidstore
    ports:
      - "8000:8000"   # REST API
      - "5555:5555"   # ZMQ REQ/REP (request-response)
      - "5556:5556"   # ZMQ PUSH/PULL (mutations/writes)
      - "5557:5557"   # ZMQ PUB/SUB (notifications)
    environment:
      - CIDSTORE_PORT=8000
      - CIDSTORE_ZMQ_REQREP_PORT=5555
      - CIDSTORE_ZMQ_PUSH_PORT=5556
      - CIDSTORE_ZMQ_PUB_PORT=5557
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

  cidsem:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: cidsem
    ports:
      - "8080:8080"  # Your cidsem API port
    environment:
      - CIDSTORE_REST_URL=http://cidstore:8000
      - CIDSTORE_ZMQ_REQREP_ENDPOINT=tcp://cidstore:5555
      - CIDSTORE_ZMQ_PUSH_ENDPOINT=tcp://cidstore:5556
      - CIDSTORE_ZMQ_PUB_ENDPOINT=tcp://cidstore:5557
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

### Start both services:
```powershell
docker compose up --build
```

## Connection Examples (Python)

### REST API (requests):
```python
import requests

CIDSTORE_URL = "http://cidstore:8000"  # In docker network
# or "http://localhost:8000" if running on host

response = requests.get(f"{CIDSTORE_URL}/health")
print(response.text)  # "ok"
```

### ZMQ REQ/REP (simple request-response):
```python
import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://cidstore:5555")

socket.send_json({"op": "ping"})
reply = socket.recv_json()
```

### ZMQ PUSH (mutations/writes):
```python
pusher = context.socket(zmq.PUSH)
pusher.connect("tcp://cidstore:5556")
pusher.send_json({"op": "put", "key": "abc", "value": "xyz"})
```

### ZMQ SUB (notifications):
```python
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://cidstore:5557")
subscriber.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all
msg = subscriber.recv_json()
```

## Port Reference

| Port | Protocol | Purpose |
|------|----------|---------|
| 8000 | REST/HTTP | Health checks, control API |
| 5555 | ZMQ REQ/REP | Simple request-response |
| 5556 | ZMQ PUSH/PULL | Mutations and writes |
| 5557 | ZMQ PUB/SUB | Notifications and broadcasts |

## Environment Variables (Configurable)

| Variable | Default | Description |
|----------|---------|-------------|
| `CIDSTORE_PORT` | 8000 | REST API port |
| `CIDSTORE_ZMQ_REQREP_PORT` | 5555 | ZMQ request-response port |
| `CIDSTORE_ZMQ_PUSH_PORT` | 5556 | ZMQ write/mutation port |
| `CIDSTORE_ZMQ_PUB_PORT` | 5557 | ZMQ pub/sub notification port |
| `CIDSTORE_HDF5_PATH` | /data/cidstore.h5 | HDF5 storage file path |
| `PYTHON_JIT` | 1 | Enable Python JIT |
| `PYTHONUNBUFFERED` | 1 | Force unbuffered logging |

## Tested and Verified

✅ Image builds successfully (6.5s with cache)
✅ Container starts and becomes healthy
✅ REST API responds: `curl http://localhost:8000/health` → `ok`
✅ Storage initialized with HDF5 backend
✅ WAL (Write-Ahead Log) initialized
✅ All 4 ports exposed and accessible

## Troubleshooting

### Port conflicts
```powershell
# Check what's using ports
docker ps

# Stop conflicting containers
docker stop cidstore
docker rm cidstore
```

### View logs
```powershell
docker logs -f cidstore
```

### Container won't start
```powershell
# Check status
docker ps -a --filter name=cidstore

# Inspect
docker inspect cidstore
```

### Health check fails
Wait 30 seconds (start_period) before checking. If still failing:
```powershell
docker exec cidstore curl http://localhost:8000/health
```

## Next Steps for cidsem Team

1. Copy `Dockerfile.cidstore` to your repo
2. Add the `docker-compose.yml` snippet above
3. Run `docker compose up --build`
4. Connect to cidstore using the examples above
5. Configure your cidsem service to use the connection endpoints

## Alternative: Save/Load Image (No Build)

If the cidsem team doesn't want to build from scratch:

**Export image (you run this):**
```powershell
docker save -o cidstore-py313-slim.tar cidstore:py313-slim
# Share cidstore-py313-slim.tar with cidsem team
```

**Import image (cidsem team runs this):**
```powershell
docker load -i cidstore-py313-slim.tar
docker tag cidstore:py313-slim cidstore:latest
docker run -d --name cidstore -p 8000:8000 -p 5555:5555 -p 5556:5556 -p 5557:5557 cidstore:latest
```

## Notes

- **Python version**: Uses official `python:3.13-slim` base (no custom compile needed)
- **Free-threading**: This image uses standard Python 3.13. If you need GIL-free execution, use a custom-built image (see `docs/Dockerfile.custom-py313`)
- **Storage**: HDF5 files persist in Docker volume `cidstore-data`
- **Registry**: No registry required — build locally or transfer tar file

---

**Ready to hand off!** The cidsem team can copy `Dockerfile.cidstore` and use the docker-compose example to run cidstore as a service.
