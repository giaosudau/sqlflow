# SQLFlow Phase 2 Integration Demo Dockerfile
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

# Copy SQLFlow source code (now from root context)
COPY . /sqlflow_source/

# Install SQLFlow in development mode
RUN cd /sqlflow_source && pip install -e ".[dev]"

# Install additional demo dependencies with compatible versions
RUN pip install \
    "numpy<2.0" \
    boto3==1.34.* \
    psycopg2-binary==2.9.* \
    "pandas>=2.0,<2.3" \
    "pyarrow>=14.0,<15.0" \
    minio==7.2.*

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