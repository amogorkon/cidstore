Handoff: CIDStore prebuilt image (custom CPython 3.13)

This document explains how to build, publish and hand off a CIDStore container image that uses a custom-built CPython 3.13 (optionally with a patch for free-threaded behaviour). The `docs/Dockerfile.custom-py313` in this repo is a multi-stage Dockerfile that compiles CPython and installs CIDStore.

1) Build locally

Run from the cidstore repo root (or copy the Dockerfile.custom-py313 to the cidsem repo and run there):

```powershell
# Build locally with default args
docker build \
  --build-arg CPYTHON_VERSION=3.13.10 \
  --build-arg CIDSTORE_VERSION=main \
  -f docs/Dockerfile.custom-py313 \
  -t myregistry.example.com/myorg/cidstore:py313-custom .
```

To include a patch/tarball that modifies CPython (e.g., a free-threaded patch), supply `CPYTHON_PATCH_URL`:

```powershell
docker build \
  --build-arg CPYTHON_VERSION=3.13.10 \
  --build-arg CIDSTORE_VERSION=main \
  --build-arg CPYTHON_PATCH_URL="https://example.com/patches/py313-nogil.tar.gz" \
  -f docs/Dockerfile.custom-py313 \
  -t myregistry.example.com/myorg/cidstore:py313-custom .
```

2) Push to your registry

```powershell
docker push myregistry.example.com/myorg/cidstore:py313-custom
```

Or use the provided GitHub Actions workflow `publish-cidstore-image.yml` which uses `docs/Dockerfile.custom-py313` to build and push images. Configure these secrets in the repository settings:
- `REGISTRY_HOST` (e.g., ghcr.io or myregistry.example.com)
- `REGISTRY_USERNAME`
- `REGISTRY_PASSWORD`
- `REGISTRY_NAMESPACE` (org or username)

3) How cidsem should consume the image

In the cidsem repo's `docker-compose.yml`:

```yaml
services:
  cidstore:
    image: myregistry.example.com/myorg/cidstore:py313-custom
    environment:
      - PYTHON_JIT=1
      # If the custom Python build supports disabling the GIL, set this to 0
      # - PYTHON_GIL=0
    ports:
      - "8000:8000"
      - "5555:5555"
      - "5556:5556"
      - "5557:5557"
    volumes:
      - cidstore-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

  cidsem:
    build: .
    depends_on:
      cidstore:
        condition: service_healthy
    environment:
      - CIDSTORE_URL=http://cidstore:8000
      - CIDSTORE_ZMQ_REQREP_ENDPOINT=tcp://cidstore:5555
      - CIDSTORE_ZMQ_PUSH_ENDPOINT=tcp://cidstore:5556
      - CIDSTORE_ZMQ_PUB_ENDPOINT=tcp://cidstore:5557

volumes:
  cidstore-data:
```

4) Notes about the GIL and free-threaded Python

- The `PYTHON_GIL` environment variable is only meaningful if the interpreter was built to support runtime disabling of the GIL (which requires a custom patched CPython). If you pass `PYTHON_GIL=0` to a standard CPython build it will likely exit with an error.
- If you require full free-threaded execution, provide a verified patch tarball via `CPYTHON_PATCH_URL` when building the image. Test carefully and document the CPython version + patch used.

5) Testing the image

After building and running the published image:

```powershell
# Run
docker run -d --name cidstore -p 8000:8000 -p 5555:5555 myregistry.example.com/myorg/cidstore:py313-custom

# Check health
curl http://localhost:8000/health

# From the cidsem container
# docker exec -it <cidsem-container> bash
# curl http://cidstore:8000/health
```

6) Troubleshooting

- If startup fails with "Disabling the GIL is not supported by this build", rebuild without setting `PYTHON_GIL=0` or provide the correct patched CPython build.
- If hdf5 or other native libs are missing, ensure the runtime image includes required `libhdf5-dev` or equivalent (the Dockerfile includes `libhdf5-dev`).

7) Hand-off package to the cidsem team

Provide the following artifacts to the cidsem team:
- The image name & tag (e.g., `myregistry.example.com/myorg/cidstore:py313-custom`)
- The `docker-compose.yml` snippet above
- The `docs/Dockerfile.custom-py313` (if they want to build locally)
- The `publish-cidstore-image.yml` CI workflow (if they want to self-host building)

If you'd like, I can now:
- Trigger a local build command and test the image (I cannot push to your registry), or
- Update `docs/Dockerfile.standalone` to point to this custom image or reference the pinned tag.
