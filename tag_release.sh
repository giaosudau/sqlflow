#!/bin/bash
# Script to tag a new release of SQLFlow
# Usage: ./tag_release.sh [patch|minor|major] [branch]

# Default values
BUMP_TYPE=${1:-patch}
BRANCH=${2:-main}

# Validate bump type
if [[ ! "$BUMP_TYPE" =~ ^(patch|minor|major)$ ]]; then
    echo "Error: Bump type must be one of: patch, minor, major"
    echo "Usage: ./tag_release.sh [patch|minor|major] [branch]"
    exit 1
fi

echo "🏷️ Creating a new tag with $BUMP_TYPE version bump on branch $BRANCH"

# Run the release script to bump version, commit, tag and push
python release.py --bump "$BUMP_TYPE" --branch "$BRANCH"

if [ $? -ne 0 ]; then
    echo "❌ Tag creation failed. Check output above for errors."
    exit 1
fi

echo "✅ Tag created and pushed successfully!"
echo "The GitHub workflow for TestPyPI should now be triggered by the tag push."
echo "You can check the status at: https://github.com/$(git remote get-url origin | sed -e 's/.*github.com[:\/]\(.*\)\.git/\1/')/actions"
echo ""
echo "Note: Make sure you have set up the TEST_PYPI_USERNAME and TEST_PYPI_PASSWORD secrets in your GitHub repository."