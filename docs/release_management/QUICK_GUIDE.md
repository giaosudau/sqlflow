# SQLFlow Release Quick Guide

## TestPyPI Release (Stage 1)

```bash
# To release a new version to TestPyPI
./tag_release.sh [patch|minor|major]

# Examples:
./tag_release.sh patch  # Bumps 0.1.0 to 0.1.1
./tag_release.sh minor  # Bumps 0.1.0 to 0.2.0
./tag_release.sh major  # Bumps 0.1.0 to 1.0.0
```

This will:
1. Bump version in pyproject.toml
2. Create a git tag (e.g., v0.1.5)
3. Trigger GitHub Actions to publish to TestPyPI

## Verify TestPyPI Release

```bash
# Install from TestPyPI to verify
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sqlflow==X.Y.Z
```

Test the package to ensure it works as expected.

## PyPI Release (Stage 2)

```bash
# To promote a version from TestPyPI to PyPI
./release_to_pypi.sh <tag_name>

# Example:
./release_to_pypi.sh v0.1.5
```

This will:
1. Create a production tag (e.g., v0.1.5-prod)
2. Trigger GitHub Actions to publish to PyPI

## Post-Release

After successful PyPI release:
1. Create a GitHub Release with the prod tag
2. Update documentation as needed
3. Announce the release to users

For detailed instructions, see [Release Process](./RELEASE_PROCESS.md).
