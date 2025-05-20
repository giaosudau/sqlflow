#!/bin/bash
# Script to promote SQLFlow from TestPyPI to PyPI (production)
# Usage: ./release_to_pypi.sh <tag_name>

TAG_NAME=$1

if [ -z "$TAG_NAME" ]; then
    echo "Error: Tag name is required"
    echo "Usage: ./release_to_pypi.sh <tag_name>"
    echo "Example: ./release_to_pypi.sh v0.1.5"
    exit 1
fi

# Verify the tag exists
if ! git rev-parse "$TAG_NAME" >/dev/null 2>&1; then
    echo "Error: Tag '$TAG_NAME' does not exist"
    exit 1
fi

echo "🚀 Promoting release $TAG_NAME from TestPyPI to PyPI"

# Create new tag with -prod suffix to trigger PyPI workflow
PROD_TAG="${TAG_NAME}-prod"
git tag "$PROD_TAG" "$TAG_NAME"
git push origin "$PROD_TAG"

if [ $? -ne 0 ]; then
    echo "❌ Failed to create production tag. Check output above for errors."
    exit 1
fi

echo "✅ Production release process initiated!"
echo "The GitHub workflow for PyPI should now be triggered by the tag push."
echo "You can check the status at: https://github.com/$(git remote get-url origin | sed -e 's/.*github.com[:\/]\(.*\)\.git/\1/')/actions"
echo ""
echo "Note: Make sure you have set up the PYPI_USERNAME and PYPI_PASSWORD secrets in your GitHub repository."
