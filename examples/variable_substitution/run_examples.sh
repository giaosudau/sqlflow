#!/bin/bash
"""Run all variable substitution examples."""

set -e

echo "SQLFlow Variable Substitution Examples"
echo "======================================"
echo

# Check if we're in the right directory
if [[ ! -f "basic_substitution.py" ]]; then
    echo "Error: Please run this script from the examples/variable_substitution directory"
    echo "Usage: cd examples/variable_substitution && ./run_examples.sh"
    exit 1
fi

# Make sure we have SQLFlow available
if ! python -c "import sqlflow.core.variables.manager" 2>/dev/null; then
    echo "Error: SQLFlow not available. Please install SQLFlow first:"
    echo "  pip install -e ."
    exit 1
fi

echo "Running basic substitution examples..."
python basic_substitution.py
echo

echo "Running priority resolution examples..."
python priority_resolution.py
echo

echo "All examples completed successfully!"
echo
echo "Next steps:"
echo "1. Try modifying the variables in the examples"
echo "2. Create your own templates"
echo "3. Test with real SQLFlow pipelines" 