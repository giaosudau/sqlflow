# SQLFlow Release Process

This document outlines the release process for SQLFlow, intended for maintainers and contributors who need to create and publish new versions of the package.

## Overview

SQLFlow follows a two-stage release process:

1. **TestPyPI Release**: Every new version is first published to TestPyPI for verification.
2. **PyPI Release**: After successful testing, the release is promoted to PyPI for public use.

This approach ensures that we can verify the package functionality in a test environment before making it available to all users.

## Prerequisites

Before proceeding with a release, ensure you have:

1. Git installed and configured
2. Access to the SQLFlow GitHub repository with push permissions
3. Python 3.11+ installed
4. The following GitHub repository secrets configured:
   - `TEST_PYPI_USERNAME` and `TEST_PYPI_PASSWORD` for TestPyPI
   - `PYPI_USERNAME` and `PYPI_PASSWORD` for PyPI

## Release Process

### Step 1: Create a Release and Publish to TestPyPI

1. Ensure your local repository is up to date:
   ```bash
   git checkout main
   git pull origin main
   ```

2. Run the tag release script with the desired version bump type:
   ```bash
   ./tag_release.sh [patch|minor|major] [branch]
   ```
   - `patch`: For bug fixes (0.1.0 → 0.1.1)
   - `minor`: For new features (0.1.0 → 0.2.0)
   - `major`: For breaking changes (0.1.0 → 1.0.0)
   - `branch`: Optional, defaults to "main"

3. The script will:
   - Bump the version in pyproject.toml
   - Commit and push the changes
   - Create and push a tag (e.g., v0.1.5)
   - Trigger the GitHub workflow to build and publish to TestPyPI

4. Wait for the GitHub Actions workflow to complete and verify the package on TestPyPI:
   - Check that the workflow ran successfully: https://github.com/YOUR_ORG/sqlflow/actions
   - Verify the package on TestPyPI: https://test.pypi.org/project/sqlflow-core/

5. Test the TestPyPI package by installing and running it:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sqlflow-core==X.Y.Z
   ```
   (Replace X.Y.Z with the actual version number)

### Step 2: Promote to PyPI

After verifying the package works as expected on TestPyPI:

1. Run the release to PyPI script with the tag name:
   ```bash
   ./release_to_pypi.sh <tag_name>
   ```
   Example: `./release_to_pypi.sh v0.1.5`

2. The script will:
   - Create a production tag (e.g., v0.1.5-prod)
   - Push the production tag to GitHub
   - Trigger the GitHub Actions workflow to publish to PyPI

3. Wait for the GitHub Actions workflow to complete and verify the package on PyPI:
   - Check that the workflow ran successfully: https://github.com/YOUR_ORG/sqlflow/actions
   - Verify the package on PyPI: https://pypi.org/project/sqlflow-core/

## Release Checklist

Before each release, make sure to:

1. Run all tests and ensure they pass
2. Update the CHANGELOG.md with details of changes
3. Update any documentation that refers to specific versions
4. Check that all required features for this version are complete

## Release Cadence

As an early-stage open-source project (pre-1.0), SQLFlow follows these release guidelines:

- **Patch releases** (0.x.Y): As needed for bug fixes, can be released frequently
- **Minor releases** (0.X.0): Every 2-4 weeks for new features
- **Major release** (1.0.0): When the API is considered stable and feature-complete

## Versioning

SQLFlow follows [Semantic Versioning](https://semver.org/):

- **Patch version**: Incremented for backwards-compatible bug fixes
- **Minor version**: Incremented for new, backwards-compatible functionality
- **Major version**: Incremented for incompatible API changes

## Creating a GitHub Release

After successfully publishing to PyPI, create a GitHub release:

1. Go to the Releases page on GitHub
2. Click "Draft a new release"
3. Select the production tag (e.g., v0.1.5-prod)
4. Title the release "SQLFlow v0.1.5"
5. Include release notes (see template in release_management/GITHUB_RELEASE_TEMPLATE.md)
6. Publish the release
