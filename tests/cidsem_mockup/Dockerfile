# Dockerfile for CIDSem mockup test
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy test script
COPY cidsem_test.py .
COPY config.py .

# Ensure unbuffered output for immediate logging
ENV PYTHONUNBUFFERED=1

# Run the test
CMD ["python", "-u", "cidsem_test.py"]
