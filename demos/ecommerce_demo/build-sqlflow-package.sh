#!/bin/bash
# Script to build the SQLFlow package from source and place it in the dist directory
# This is separate from the demo setup to allow for easier updates to SQLFlow

set -e  # Exit on error

# Function to print colored text
print_colored() {
  local color=$1
  local text=$2
  
  case $color in
    "blue") echo -e "\033[1;34m$text\033[0m" ;;
    "green") echo -e "\033[1;32m$text\033[0m" ;;
    "red") echo -e "\033[1;31m$text\033[0m" ;;
    "yellow") echo -e "\033[1;33m$text\033[0m" ;;
    *) echo "$text" ;;
  esac
}

print_colored "blue" "📦 Building SQLFlow Package"
print_colored "blue" "=========================="
echo

# Default location for the SQLFlow repository
SQLFLOW_REPO_DIR="/app/sqlflow_package"

# Function to install SQLFlow from local code
install_from_local() {
    echo "Installing SQLFlow package from local code..."
    
    if [ -d "/app/sqlflow" ] && [ -f "/app/sqlflow/pyproject.toml" ]; then
        echo "Found SQLFlow package in /app/sqlflow"
        cd /app/sqlflow
        
        # Install in development mode
        pip install -e .
        
        echo "SQLFlow package installed successfully!"
        echo "Version: $(pip show sqlflow | grep Version)"
        return 0
    fi
    
    echo "Local SQLFlow package not found or not valid."
    return 1
}

# First try to install from local code
if install_from_local; then
    echo "Using SQLFlow package from local directory."
else
    # If the package directory exists but is empty, use it
    if [ -d "$SQLFLOW_REPO_DIR" ] && [ -z "$(ls -A $SQLFLOW_REPO_DIR)" ]; then
        echo "Using empty directory at $SQLFLOW_REPO_DIR"
        
        # Copy the local SQLFlow package to the repository directory
        cp -r /app/sqlflow/* $SQLFLOW_REPO_DIR/
        
        # Install the package
        cd $SQLFLOW_REPO_DIR
        pip install -e .
        
        echo "SQLFlow package installed successfully!"
        echo "Version: $(pip show sqlflow | grep Version)"
    else
        echo "ERROR: Could not install SQLFlow package."
        echo "Please make sure the SQLFlow package is available in /app/sqlflow or mount it as a volume."
        exit 1
    fi
fi

# Test that the SQLFlow CLI is working
echo
echo "Testing SQLFlow CLI..."
sqlflow --version || echo "WARNING: sqlflow command not found or returned an error."

echo
echo "SQLFlow package build completed!"
echo

# Get the absolute path of the repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Ensure the dist directory exists
mkdir -p "${SCRIPT_DIR}/dist"

# Navigate to the repo root
cd "$REPO_ROOT"

# Set up Python venv if it doesn't exist
if [ ! -d ".venv" ]; then
  print_colored "yellow" "Creating virtual environment..."
  python -m venv .venv
fi

# Activate the virtual environment
print_colored "yellow" "Activating virtual environment..."
source .venv/bin/activate

# Install build dependencies
print_colored "yellow" "Installing build dependencies..."
pip install --upgrade pip wheel build

# Build the wheel package
print_colored "yellow" "Building wheel package..."
python -m build --wheel

# Find the newest wheel file
WHEEL_PATH=$(find "${REPO_ROOT}/dist" -name "sqlflow-*.whl" | sort -r | head -n 1)

if [ -z "$WHEEL_PATH" ]; then
  print_colored "red" "❌ Failed to build SQLFlow wheel. Please check for errors."
  exit 1
fi

print_colored "green" "✅ SQLFlow wheel built successfully: $(basename $WHEEL_PATH)"

# Copy the wheel to the demo dist directory
print_colored "yellow" "Copying wheel to demo directory..."
cp "$WHEEL_PATH" "${SCRIPT_DIR}/dist/"
print_colored "green" "✅ Wheel copied successfully"

print_colored "green" "🎉 Build completed! The SQLFlow package is now available in the dist/ directory."
print_colored "yellow" "To use this package, run ./start-demo.sh to rebuild the Docker image with the new package."
echo 