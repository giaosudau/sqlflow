FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy SQLFlow wheel and install it
COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && \
    echo '#!/bin/bash' > /usr/local/bin/sqlflow && \
    echo 'python -m sqlflow.cli.main "$@"' >> /usr/local/bin/sqlflow && \
    chmod +x /usr/local/bin/sqlflow

# Install additional dependencies for the demo
RUN pip install --no-cache-dir \
    duckdb>=0.9.0 \
    pandas>=2.0.0 \
    pyarrow>=14.0.0 \
    psycopg2-binary>=2.9.0 \
    sqlalchemy>=2.0.0 \
    boto3>=1.28.0 \
    s3fs>=2023.9.0 \
    fsspec==2023.12.1 \
    requests>=2.31.0 \
    pyyaml>=6.0.0 \
    python-dotenv>=1.0.0

# Copy SQLFlow demo files
COPY . .

# Create target directory to ensure it exists and set permissions
RUN mkdir -p /app/target && chmod 777 /app/target

# Set environment variables
ENV PYTHONPATH="/app:${PYTHONPATH}"
ENV PATH="/app/bin:${PATH}"

# Run bash by default to keep container running
# This allows us to execute commands manually
CMD ["bash", "-c", "echo 'SQLFlow container is ready. Run pipelines with: python -m sqlflow.cli.main pipeline run <pipeline_file>' && sleep infinity"]
