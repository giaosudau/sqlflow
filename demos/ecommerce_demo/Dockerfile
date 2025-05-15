FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy SQLFlow codebase
COPY . .

# Create target directory to ensure it exists and set permissions
RUN mkdir -p /app/target && chmod 777 /app/target

# Set environment variables
ENV PYTHONPATH="/app:${PYTHONPATH}"
ENV PATH="/app/bin:${PATH}"

# Run SQLFlow demo when container starts
CMD ["python", "-m", "sqlflow", "run", "demos/ecommerce_demo/pipelines/simple_test.sf", "--profile", "demos/ecommerce_demo/sqlflow.yml"]
