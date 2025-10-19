# Dockerfile for CIDSem mockup test
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy test script
COPY cidsem_test.py .
COPY config.py .

# Run the test
CMD ["python", "cidsem_test.py"]
