#!/bin/bash
set -e

# Start REST API server in background
echo "Starting REST API server on port 8000..."
uvicorn src.cidstore.control_api:app --host 0.0.0.0 --port 8000 &
REST_PID=$!

# Start ZMQ server in background
echo "Starting ZMQ server on ports 5557-5559..."
python -m src.cidstore.async_zmq_server &
ZMQ_PID=$!

# Wait for either process to exit
wait -n

# If one exits, kill the other
kill $REST_PID $ZMQ_PID 2>/dev/null

exit $?