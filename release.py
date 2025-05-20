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


def update_pyproject_version(
    pyproject_path: Path, new_version: str, dry_run: bool = False
) -> None:
    content = pyproject_path.read_text(encoding="utf-8")
    if re.search(r'(?m)^version\s*=\s*"([^"]+)"', content):
        new_content = re.sub(
            r'(?m)^(version\s*=\s*")([^"]+)(")', f"\\g<1>{new_version}\\g<3>", content
        )
    else:
        # Add version under [project] if not present
        new_content = re.sub(
            r"(?m)^\[project\]", f'[project]\nversion = "{new_version}"', content
        )

    if not dry_run:
        pyproject_path.write_text(new_content, encoding="utf-8")
        logger.info(f"Updated version to {new_version} in {pyproject_path}")
    else:
        logger.info(
            f"[DRY RUN] Would update version to {new_version} in {pyproject_path}"
        )


def update_init_version(new_version: str, dry_run: bool = False) -> Path | None:
    """Update the __version__ in the package's __init__.py file."""
    init_path = Path("sqlflow/__init__.py")
    if not init_path.exists():
        logger.warning(f"Could not find {init_path}, skipping version update.")
        return None

    content = init_path.read_text(encoding="utf-8")
    new_content = re.sub(
        r'(__version__\s*=\s*")[^"]+(")', f"\\g<1>{new_version}\\g<2>", content
    )

    if not dry_run:
        init_path.write_text(new_content, encoding="utf-8")
        logger.info(f"Updated version to {new_version} in {init_path}")
    else:
        logger.info(f"[DRY RUN] Would update version to {new_version} in {init_path}")

    return init_path


def run_cmd(cmd: list[str], check: bool = True, dry_run: bool = False) -> None:
    logger.info(f"Running: {' '.join(cmd)}")
    if not dry_run:
        subprocess.run(cmd, check=check)
    else:
        logger.info(f"[DRY RUN] Would run: {' '.join(cmd)}")


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
        "--branch",
        default="main",
        help="Branch to tag latest commit from (default: main)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the version bump without making any changes",
    )
    args = parser.parse_args()

    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        logger.error("pyproject.toml not found.")
        sys.exit(1)

    # Get and bump version
    current_version = get_current_version(pyproject_path)
    new_version = bump_version(current_version, args.bump)
    update_pyproject_version(pyproject_path, new_version, args.dry_run)

    # Update version in __init__.py
    init_path = update_init_version(new_version, args.dry_run)

    # Git add, commit, push
    files_to_add = [str(pyproject_path)]
    if init_path:
        files_to_add.append(str(init_path))

    for file_path in files_to_add:
        run_cmd(["git", "add", file_path], dry_run=args.dry_run)
    run_cmd(
        ["git", "commit", "-m", f"Bump version to {new_version}"], dry_run=args.dry_run
    )

    # Ensure we're on the correct branch before pushing
    current_branch = get_current_branch()
    if current_branch != args.branch and not args.dry_run:
        logger.warning(f"Currently on branch '{current_branch}', not '{args.branch}'")
        response = input("Continue anyway? [y/N]: ")
        if response.lower() != "y":
            logger.info("Aborting release process.")
            sys.exit(1)
    elif current_branch != args.branch and args.dry_run:
        logger.warning(
            f"[DRY RUN] Currently on branch '{current_branch}', not '{args.branch}'"
        )
        logger.info("[DRY RUN] Would ask for confirmation before continuing")

    run_cmd(["git", "push", "origin", current_branch], dry_run=args.dry_run)

    # Tag the latest commit
    tag_name = f"v{new_version}"
    run_cmd(["git", "tag", tag_name], dry_run=args.dry_run)
    run_cmd(["git", "push", "origin", tag_name], dry_run=args.dry_run)

    if args.dry_run:
        logger.info(
            f"[DRY RUN] Would create release {tag_name} on branch {current_branch}."
        )
    else:
        logger.info(
            f"Release {tag_name} created and pushed on branch {current_branch}."
        )


if __name__ == "__main__":
    main()
