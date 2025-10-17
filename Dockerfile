# syntax=docker/dockerfile:1
# Python 3.13 with free-threading and JIT support
# Note: Standard python:3.13-slim includes both features
# Run with: docker run -e PYTHON_GIL=0 -e PYTHON_JIT=1 <image>
FROM python:3.13-slim

WORKDIR /app

# System dependencies for HDF5, NumPy, etc.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libhdf5-dev \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy source code
COPY . .

# Expose ports: 8000 for REST, 5557-5559 for ZMQ
EXPOSE 8000 5557 5558 5559

# Entrypoint: run with free-threading and JIT enabled for optimal performance
# Override with: docker run -e PYTHON_GIL=1 <image> to disable free-threading
ENV PYTHON_GIL=0
ENV PYTHON_JIT=1
CMD ["uvicorn", "src.cidstore.control_api:app", "--host", "0.0.0.0", "--port", "8000"]
