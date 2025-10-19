# syntax=docker/dockerfile:1
# Python 3.13 with free-threading and JIT support
# Note: Standard python:3.13-slim includes both features
# Run with: docker run -e PYTHON_GIL=0 -e PYTHON_JIT=1 <image>
FROM python:3.13-slim

WORKDIR /app

# System dependencies for HDF5, NumPy, curl (for health checks), etc.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libhdf5-dev \
        libssl-dev \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Upgrade pip and build tooling before installing package
RUN pip install --upgrade pip setuptools wheel

# Copy source code
# Copy source code and pyproject
COPY src/ ./src/
COPY pyproject.toml ./

# Install CIDStore in editable mode (uses pyproject.toml dependencies)
RUN pip install -e .

# Create data directory for HDF5 storage
RUN mkdir -p /data

# Expose ports:
# 8000: REST API for health checks and control
# 5555: ZMQ for high-performance data operations (test compatibility)
# 5557: ZMQ PUSH/PULL for mutations
# 5558: ZMQ ROUTER/DEALER for reads
# 5559: ZMQ PUB/SUB for notifications
EXPOSE 8000 5555 5557 5558 5559

# Environment variables
# Do NOT set PYTHON_GIL by default. Setting PYTHON_GIL=0 tells the
# interpreter to disable the GIL at startup which will crash for
# CPython builds that do not support free-threading (see error
# "config_read_gil: Disabling the GIL is not supported by this build").
# If you have a custom free-threaded Python build, pass -e PYTHON_GIL=0
# when running the container or set it in a derived image.
ENV PYTHON_JIT=1
ENV CIDSTORE_HDF5_PATH=/data/cidstore.h5
ENV PYTHONUNBUFFERED=1

# Health check using REST API
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -f http://localhost:8000/health || exit 1

# Start script that runs both REST API and ZMQ server
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

CMD ["/docker-entrypoint.sh"]
