#!/bin/bash
set -e

# Start the REST API control plane server, which owns the CIDStore instance
# and spawns ZMQ workers as background tasks. This ensures:
# - Single HDF5 writer (REST API owns the file)
# - Proper control/data plane separation
# - Health checks can monitor both REST and ZMQ workers

echo "Starting CIDStore control plane (REST API + ZMQ workers)..."
uvicorn src.cidstore.control_api:app --host 0.0.0.0 --port 8000

exit $?