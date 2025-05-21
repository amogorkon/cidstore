# syntax=docker/dockerfile:1
FROM python:3.12-slim

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

# Entrypoint: run both ZMQ and REST (example, adjust as needed)
CMD ["uvicorn", "src.cidstore.control_api:app", "--host", "0.0.0.0", "--port", "8000"]
