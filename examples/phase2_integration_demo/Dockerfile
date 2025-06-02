# SQLFlow Phase 2 Integration Demo Dockerfile
# Optimized for faster builds through:
# 1. Layer caching: Dependencies installed before code copy
# 2. Using pyproject.toml extras instead of manual pip installs
# 3. Minimal duplicate dependency installation
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    postgresql-client \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only pyproject.toml for dependency installation (README.md and LICENSE are optional)
# Since main .dockerignore excludes .md files, we'll create minimal placeholder files
COPY pyproject.toml /sqlflow_source/

# Create minimal placeholder files that pip might expect but are not required for functionality
RUN touch /sqlflow_source/README.md /sqlflow_source/LICENSE

# Create minimal sqlflow package structure for installation
RUN mkdir -p /sqlflow_source/sqlflow && touch /sqlflow_source/sqlflow/__init__.py

# Install SQLFlow dependencies first (leverages Docker layer caching)
# Using postgres and aws extras since they cover most demo dependencies
RUN cd /sqlflow_source && pip install -e ".[postgres,aws,dev]"

# Install additional demo-specific dependencies not in pyproject.toml
# Note: numpy is already included in core dependencies via pandas/pyarrow
# Only minio client is needed for S3 compatibility testing
RUN pip install \
    minio==7.2.*

# Now copy the full SQLFlow source code (this layer will rebuild when code changes)
COPY . /sqlflow_source/

# Reinstall SQLFlow to pick up the actual code (fast since dependencies are cached)
RUN cd /sqlflow_source && pip install -e ".[postgres,aws,dev]" --no-deps

# Create demo directories
RUN mkdir -p /app/pipelines /app/data /app/profiles /app/output /app/target /app/scripts

# Copy demo configuration and scripts (from the demo subdirectory)
COPY examples/phase2_integration_demo/pipelines /app/pipelines/
COPY examples/phase2_integration_demo/data /app/data/
COPY examples/phase2_integration_demo/profiles /app/profiles/
COPY examples/phase2_integration_demo/scripts /app/scripts/

# Set environment variables
ENV PYTHONPATH="/sqlflow_source:$PYTHONPATH"
ENV SQLFLOW_HOME="/app"
ENV SQLFLOW_LOG_LEVEL="INFO"

# Create entrypoint script
RUN echo '#!/bin/bash\n\
echo "🚀 Starting SQLFlow Phase 2 Integration Demo..."\n\
echo "📊 Available services:"\n\
echo "  - PostgreSQL: postgres:5432"\n\
echo "  - MinIO S3: minio:9000"\n\
echo "  - pgAdmin: http://localhost:8080"\n\
echo "  - MinIO Console: http://localhost:9001"\n\
echo ""\n\
echo "🔧 SQLFlow Version:"\n\
sqlflow --version\n\
echo ""\n\
echo "📋 Demo ready! Run ./scripts/run_integration_demo.sh to start testing."\n\
echo ""\n\
# Keep container running\n\
tail -f /dev/null\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Expose port for optional web interface
EXPOSE 8000

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"] 