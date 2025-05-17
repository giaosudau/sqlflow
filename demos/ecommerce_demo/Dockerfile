FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create the directory structure
RUN mkdir -p /app/sqlflow/demos/ecommerce_demo/target \
    && mkdir -p /app/target \
    && chmod -R 777 /app/target \
    && chmod -R 777 /app/sqlflow

# Copy necessary files for the demo
COPY data/ /app/sqlflow/demos/ecommerce_demo/data/
COPY pipelines/ /app/sqlflow/demos/ecommerce_demo/pipelines/
COPY profiles/ /app/sqlflow/demos/ecommerce_demo/profiles/
COPY mock-config/ /app/sqlflow/demos/ecommerce_demo/mock-config/
COPY init-scripts/ /app/sqlflow/demos/ecommerce_demo/init-scripts/

# Install Python dependencies
RUN pip install --no-cache-dir \
    duckdb>=1.2.0 \
    pandas>=2.0.0 \
    pyarrow>=10.0.0 \
    click>=8.0.0 \
    networkx>=3.0 \
    typer==0.9.0 \
    psycopg2-binary>=2.9.0 \
    boto3>=1.26.0 \
    requests>=2.28.0 \
    pyyaml>=6.0 \
    typing-extensions>=4.0.0

# Create a validation script for pipeline syntax checking
RUN echo '#!/usr/bin/env python3\n\
import os\n\
import sys\n\
import re\n\
import glob\n\
\n\
def validate_pipeline(file_path):\n\
    print(f"Validating pipeline: {file_path}")\n\
    try:\n\
        with open(file_path, "r") as f:\n\
            content = f.read()\n\
        \n\
        # Basic checks\n\
        errors = []\n\
        \n\
        # Check for SOURCE statements\n\
        if not re.search(r"SOURCE\\s+\\w+\\s+TYPE", content):\n\
            errors.append("Missing SOURCE statements")\n\
        \n\
        # Check for LOAD statements\n\
        if not re.search(r"LOAD\\s+\\w+\\s+FROM", content):\n\
            errors.append("Missing LOAD statements")\n\
        \n\
        # Check for CREATE TABLE statements\n\
        if not re.search(r"CREATE\\s+TABLE", content):\n\
            errors.append("Missing CREATE TABLE statements")\n\
        \n\
        # Check for EXPORT statements\n\
        if not re.search(r"EXPORT\\s+", content):\n\
            errors.append("Missing EXPORT statements")\n\
        \n\
        if errors:\n\
            print(f"❌ Failed validation with errors:")\n\
            for error in errors:\n\
                print(f"  - {error}")\n\
            return False\n\
        else:\n\
            print(f"✅ Pipeline validated successfully")\n\
            return True\n\
    except Exception as e:\n\
        print(f"❌ Error processing file: {str(e)}")\n\
        return False\n\
\n\
def validate_all_pipelines(directory):\n\
    pipeline_files = glob.glob(os.path.join(directory, "*.sf"))\n\
    \n\
    if not pipeline_files:\n\
        print(f"No .sf files found in {directory}")\n\
        return\n\
    \n\
    valid_count = 0\n\
    total_count = len(pipeline_files)\n\
    \n\
    print(f"Found {total_count} pipeline files to validate")\n\
    print("-" * 60)\n\
    \n\
    for file_path in pipeline_files:\n\
        if validate_pipeline(file_path):\n\
            valid_count += 1\n\
        print("-" * 60)\n\
    \n\
    print(f"Validation summary: {valid_count}/{total_count} pipelines valid")\n\
    \n\
    # List showcase pipelines\n\
    showcase_files = [f for f in pipeline_files if "showcase" in os.path.basename(f)]\n\
    if showcase_files:\n\
        print("\\nShowcase pipelines:")\n\
        for file in showcase_files:\n\
            print(f"- {os.path.basename(file)}")\n\
\n\
if __name__ == "__main__":\n\
    pipeline_dir = "/app/sqlflow/demos/ecommerce_demo/pipelines"\n\
    if len(sys.argv) > 1:\n\
        pipeline_dir = sys.argv[1]\n\
    validate_all_pipelines(pipeline_dir)\n\
' > /app/validate_pipelines.py && chmod +x /app/validate_pipelines.py

# Create a welcome script
RUN echo '#!/bin/bash\n\
echo ""\n\
echo "SQLFlow Demo Pipeline Validator"\n\
echo "=============================="\n\
echo ""\n\
echo "Since we cannot run SQLFlow directly, we will validate the pipeline syntax."\n\
echo "Running pipeline validator..."\n\
echo ""\n\
python /app/validate_pipelines.py\n\
echo ""\n\
echo "For a real SQLFlow environment, you would need to install the sqlflow package."\n\
echo "See the documentation for more details."\n\
' > /app/welcome.sh && chmod +x /app/welcome.sh

# Set environment variables
ENV PYTHONPATH="/app:/app/sqlflow:${PYTHONPATH}"

CMD ["/bin/bash", "-c", "/app/welcome.sh; exit 0"]
