"""
Release automation script for Python projects.

Features:
- Automatically bumps the version in pyproject.toml (patch, minor, major), starting at 0.1.0 if not found.
- Commits and pushes the version change.
- Tags the latest commit on the 'dev' branch by default (or a specified branch).
- Pushes the tag to origin.

Version bump types (Semantic Versioning):
- patch: Increments the last digit (X.Y.Z → X.Y.(Z+1)). Use for bug fixes and small changes that do not affect the API.
- minor: Increments the middle digit (X.Y.Z → X.(Y+1).0). Use for adding new features in a backwards-compatible manner.
- major: Increments the first digit (X.Y.Z → (X+1).0.0). Use for incompatible API changes or major new releases.

Usage:
    python release.py --bump patch         # Bump patch version on 'dev' branch
    python release.py --bump minor         # Bump minor version on 'dev' branch
    python release.py --bump major         # Bump major version on 'dev' branch
    python release.py --bump patch --branch feature-branch  # Tag latest commit on 'feature-branch'

Requirements:
- Must be run from the project root (where pyproject.toml is located).
- Requires git to be installed and configured.

"""

import argparse
import logging
import re
import subprocess
import sys
from pathlib import Path

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("release")


def get_current_version(pyproject_path: Path) -> str:
    content = pyproject_path.read_text(encoding="utf-8")
    match = re.search(r'(?m)^version\s*=\s*"([^"]+)"', content)
    if match:
        return match.group(1)
    return "0.1.0"


def bump_version(version: str, bump_type: str) -> str:
    major, minor, patch = map(int, version.split("."))
    if bump_type == "patch":
        patch += 1
    elif bump_type == "minor":
        minor += 1
        patch = 0
    elif bump_type == "major":
        major += 1
        minor = 0
        patch = 0
    else:
        raise ValueError(f"Unknown bump type: {bump_type}")
    return f"{major}.{minor}.{patch}"


def update_pyproject_version(pyproject_path: Path, new_version: str) -> None:
    content = pyproject_path.read_text(encoding="utf-8")
    if re.search(r'(?m)^version\s*=\s*"([^"]+)"', content):
        new_content = re.sub(
            r'(?m)^(version\s*=\s*")([^"]+)(")', f"\\1{new_version}\\3", content
        )
    else:
        # Add version under [project] if not present
        new_content = re.sub(
            r"(?m)^\[project\]", f'[project]\nversion = "{new_version}"', content
        )
    pyproject_path.write_text(new_content, encoding="utf-8")
    logger.info(f"Updated version to {new_version} in {pyproject_path}")


def run_cmd(cmd: list[str], check: bool = True) -> None:
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=check)


def get_latest_commit(branch: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", branch], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def get_current_branch() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Release script for versioning and tagging."
    )
    parser.add_argument(
        "--bump",
        choices=["patch", "minor", "major"],
        required=True,
        help="Version part to bump",
    )
    parser.add_argument(
        "--branch", help="Branch to tag latest commit from (default: dev)"
    )
    args = parser.parse_args()

    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        logger.error("pyproject.toml not found.")
        sys.exit(1)

    # Get and bump version
    current_version = get_current_version(pyproject_path)
    new_version = bump_version(current_version, args.bump)
    update_pyproject_version(pyproject_path, new_version)

    # Git add, commit, push
    run_cmd(["git", "add", str(pyproject_path)])
    run_cmd(["git", "commit", "-m", f"Bump version to {new_version}"])
    run_cmd(["git", "push"])

    # Tag the latest commit on the specified or default branch (dev)
    branch = args.branch or "dev"
    commit_hash = get_latest_commit(branch)
    tag_name = f"v{new_version}"
    run_cmd(["git", "tag", tag_name, commit_hash])
    run_cmd(["git", "push", "origin", tag_name])
    logger.info(f"Release {tag_name} created and pushed on branch {branch}.")


if __name__ == "__main__":
    main()
