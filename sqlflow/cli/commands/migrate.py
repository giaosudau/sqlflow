"""Migration tooling CLI commands for SQLFlow.

Implements Task 5.2: Migration Tooling from the profile enhancement plan.
Provides commands to convert inline syntax to profile-based and extract profiles.
"""

import re
from pathlib import Path
from typing import Dict, List

import typer
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from sqlflow.logging import get_logger

logger = get_logger(__name__)
console = Console()

# Create the migrate command group
migrate_app = typer.Typer(
    name="migrate",
    help="Migrate SQLFlow pipelines to use profile-based configuration",
)


@migrate_app.command("to-profiles")
def migrate_to_profiles(
    pipeline_file: str = typer.Argument(
        ..., help="Path to the SQLFlow pipeline file to migrate"
    ),
    profile_dir: str = typer.Option(
        "profiles", "--profile-dir", help="Directory to store generated profiles"
    ),
    profile_name: str = typer.Option(
        None, "--profile-name", help="Name for the generated profile (default: auto)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview changes without writing files"
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing profile files"
    ),
) -> None:
    """Convert a pipeline from inline syntax to profile-based configuration.

    Analyzes the pipeline file for SOURCE and LOAD statements with inline
    connector configurations and extracts them into a profile file.
    """
    try:
        pipeline_path = Path(pipeline_file)
        if not pipeline_path.exists():
            console.print(f"❌ Pipeline file not found: {pipeline_file}")
            raise typer.Exit(code=1)

        console.print(f"🔍 Analyzing pipeline: {pipeline_file}")

        # Parse the pipeline and extract connector configurations
        with open(pipeline_path, "r") as f:
            pipeline_content = f.read()

        extracted_configs = _extract_connector_configs(pipeline_content)

        if not extracted_configs:
            console.print("✅ No inline connector configurations found")
            console.print("   Pipeline already uses profile-based configuration")
            return

        # Generate profile name if not provided
        if not profile_name:
            profile_name = _generate_profile_name(pipeline_path)

        # Create profile configuration
        profile_config = _create_profile_config(extracted_configs)

        # Generate migrated pipeline content
        migrated_content = _migrate_pipeline_content(
            pipeline_content, extracted_configs, profile_name
        )

        if dry_run:
            _show_migration_preview(
                pipeline_file, profile_name, profile_config, migrated_content
            )
        else:
            _write_migration_files(
                pipeline_path,
                profile_dir,
                profile_name,
                profile_config,
                migrated_content,
                force,
            )

    except Exception as e:
        logger.error(f"Error migrating pipeline: {e}")
        console.print(f"❌ Error: {e}")
        raise typer.Exit(code=1)


@migrate_app.command("extract-profiles")
def extract_profiles(
    pipeline_dir: str = typer.Argument(
        ..., help="Directory containing SQLFlow pipeline files"
    ),
    output_file: str = typer.Option(
        "profiles/extracted.yml", "--output", help="Output profile file path"
    ),
    merge_similar: bool = typer.Option(
        True, "--merge-similar", help="Merge similar connector configurations"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview extracted profiles without writing"
    ),
) -> None:
    """Extract profiles from multiple pipeline files.

    Scans all SQLFlow files in a directory and extracts connector
    configurations into a unified profile file.
    """
    try:
        pipeline_path = Path(pipeline_dir)
        if not pipeline_path.exists():
            console.print(f"❌ Pipeline directory not found: {pipeline_dir}")
            raise typer.Exit(code=1)

        console.print(f"🔍 Scanning directory: {pipeline_dir}")

        # Find all SQLFlow files
        pipeline_files = _find_pipeline_files(pipeline_path)

        if not pipeline_files:
            console.print("❌ No SQLFlow pipeline files found")
            raise typer.Exit(code=1)

        console.print(f"📁 Found {len(pipeline_files)} pipeline files")

        # Extract configurations from all files
        all_configs = {}
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("Extracting configurations...", total=None)

            for pipeline_file in pipeline_files:
                with open(pipeline_file, "r") as f:
                    content = f.read()

                configs = _extract_connector_configs(content)
                if configs:
                    relative_path = pipeline_file.relative_to(pipeline_path)
                    all_configs[str(relative_path)] = configs

        if not all_configs:
            console.print("✅ No inline connector configurations found")
            return

        # Merge similar configurations if requested
        if merge_similar:
            merged_configs = _merge_similar_configs(all_configs)
        else:
            merged_configs = _flatten_configs(all_configs)

        # Create unified profile
        unified_profile = _create_unified_profile(merged_configs)

        if dry_run:
            _show_extraction_preview(all_configs, unified_profile)
        else:
            _write_extracted_profile(output_file, unified_profile)

    except Exception as e:
        logger.error(f"Error extracting profiles: {e}")
        console.print(f"❌ Error: {e}")
        raise typer.Exit(code=1)


def _extract_connector_configs(content: str) -> Dict[str, Dict]:
    """Extract connector configurations from pipeline content."""
    configs = {}

    # Pattern for SOURCE statements with inline configuration
    source_pattern = r"SOURCE\s+(\w+)\s+TYPE\s+(\w+)\s+PARAMS\s*\{([^}]+)\}"
    for match in re.finditer(source_pattern, content, re.IGNORECASE | re.MULTILINE):
        source_name, connector_type, params_str = match.groups()
        params = _parse_params_string(params_str)

        configs[source_name] = {
            "type": "source",
            "connector_type": connector_type.lower(),
            "params": params,
            "original_match": match.group(0),
        }

    # Pattern for LOAD statements with inline configuration
    load_pattern = r"LOAD\s+(\w+)\s+TO\s+(\w+)\s+TYPE\s+(\w+)\s+PARAMS\s*\{([^}]+)\}"
    for match in re.finditer(load_pattern, content, re.IGNORECASE | re.MULTILINE):
        table_name, dest_name, connector_type, params_str = match.groups()
        params = _parse_params_string(params_str)

        configs[dest_name] = {
            "type": "destination",
            "connector_type": connector_type.lower(),
            "params": params,
            "original_match": match.group(0),
        }

    return configs


def _parse_params_string(params_str: str) -> Dict[str, str]:
    """Parse parameter string into dictionary."""
    params = {}
    # Simple key=value parsing (can be enhanced for complex cases)
    for line in params_str.strip().split("\n"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            params[key] = value

    return params


def _generate_profile_name(pipeline_path: Path) -> str:
    """Generate a profile name based on the pipeline file."""
    base_name = pipeline_path.stem
    # Remove common suffixes
    for suffix in ["_pipeline", "_flow", "_etl"]:
        if base_name.endswith(suffix):
            base_name = base_name[: -len(suffix)]
            break

    return base_name


def _create_profile_config(configs: Dict[str, Dict]) -> Dict:
    """Create profile configuration from extracted configs."""
    profile = {"version": "1.0", "connectors": {}}

    for name, config in configs.items():
        connector_config = {
            "type": config["connector_type"],
            "params": config["params"],
        }
        profile["connectors"][name] = connector_config

    return profile


def _migrate_pipeline_content(
    content: str, configs: Dict[str, Dict], profile_name: str
) -> str:
    """Migrate pipeline content to use profile-based syntax."""
    migrated_content = content

    for name, config in configs.items():
        original = config["original_match"]

        if config["type"] == "source":
            # Replace SOURCE with profile-based syntax
            replacement = f'SOURCE {name} FROM "{name}"'
        else:  # destination
            # Extract table name from original LOAD statement
            load_match = re.search(r"LOAD\s+(\w+)\s+TO\s+\w+", original, re.IGNORECASE)
            table_name = load_match.group(1) if load_match else "table"
            replacement = f'LOAD {table_name} TO "{name}"'

        migrated_content = migrated_content.replace(original, replacement)

    # Add profile reference at the top
    profile_header = (
        f"-- Profile: {profile_name}\n-- Generated by SQLFlow migration tool\n\n"
    )
    migrated_content = profile_header + migrated_content

    return migrated_content


def _find_pipeline_files(directory: Path) -> List[Path]:
    """Find all SQLFlow pipeline files in directory."""
    pipeline_files = []
    for pattern in ["*.sql", "*.sqlflow", "*.sf"]:
        pipeline_files.extend(directory.rglob(pattern))

    return sorted(pipeline_files)


def _merge_similar_configs(all_configs: Dict[str, Dict]) -> Dict[str, Dict]:
    """Merge similar connector configurations."""
    # Group configurations by type and parameters
    config_groups = {}
    merged_configs = {}

    for file_path, configs in all_configs.items():
        for name, config in configs.items():
            # Create a signature for the configuration
            signature = _create_config_signature(config)

            if signature not in config_groups:
                config_groups[signature] = []

            config_groups[signature].append((file_path, name, config))

    # Create merged configurations
    for i, (signature, group) in enumerate(config_groups.items()):
        if len(group) == 1:
            # Single configuration, use original name
            file_path, name, config = group[0]
            merged_name = name
        else:
            # Multiple similar configurations, create merged name
            connector_type = group[0][2]["connector_type"]
            merged_name = f"{connector_type}_{i + 1}"

        merged_configs[merged_name] = group[0][2]  # Use first config as template

    return merged_configs


def _create_config_signature(config: Dict) -> str:
    """Create a signature for configuration comparison."""
    # Create signature based on connector type and key parameters
    signature_parts = [config["connector_type"]]

    # Add key parameters (excluding sensitive ones)
    params = config.get("params", {})
    for key in sorted(params.keys()):
        if not any(
            sensitive in key.lower()
            for sensitive in ["password", "token", "secret", "key"]
        ):
            signature_parts.append(f"{key}={params[key]}")

    return "|".join(signature_parts)


def _flatten_configs(all_configs: Dict[str, Dict]) -> Dict[str, Dict]:
    """Flatten all configurations without merging."""
    flattened = {}
    for file_path, configs in all_configs.items():
        for name, config in configs.items():
            # Create unique name by prefixing with file name
            file_prefix = Path(file_path).stem
            unique_name = f"{file_prefix}_{name}"
            flattened[unique_name] = config

    return flattened


def _create_unified_profile(configs: Dict[str, Dict]) -> Dict:
    """Create unified profile from merged configurations."""
    profile = {
        "version": "1.0",
        "connectors": {},
        "metadata": {
            "generated_by": "SQLFlow migration tool",
            "extracted_connectors": len(configs),
        },
    }

    for name, config in configs.items():
        connector_config = {
            "type": config["connector_type"],
            "params": config["params"],
        }
        profile["connectors"][name] = connector_config

    return profile


def _show_migration_preview(
    pipeline_file: str,
    profile_name: str,
    profile_config: Dict,
    migrated_content: str,
) -> None:
    """Show preview of migration changes."""
    console.print("\n📋 Migration Preview")
    console.print("=" * 50)

    console.print(f"\n📁 Pipeline: {pipeline_file}")
    console.print(f"📄 Profile: {profile_name}.yml")

    console.print(f"\n🔌 Extracted Connectors ({len(profile_config['connectors'])}):")
    for name, config in profile_config["connectors"].items():
        console.print(f"  • {name} ({config['type']})")

    console.print("\n📄 Generated Profile:")
    console.print("```yaml")
    console.print(yaml.dump(profile_config, default_flow_style=False))
    console.print("```")

    console.print("\n📝 Migrated Pipeline (first 10 lines):")
    console.print("```sql")
    lines = migrated_content.split("\n")[:10]
    for line in lines:
        console.print(line)
    if len(migrated_content.split("\n")) > 10:
        console.print("...")
    console.print("```")

    console.print("\n💡 Run without --dry-run to apply changes")


def _show_extraction_preview(all_configs: Dict, unified_profile: Dict) -> None:
    """Show preview of profile extraction."""
    console.print("\n📋 Extraction Preview")
    console.print("=" * 50)

    console.print(f"\n📁 Files Analyzed: {len(all_configs)}")
    for file_path in all_configs.keys():
        config_count = len(all_configs[file_path])
        console.print(f"  • {file_path}: {config_count} configurations")

    console.print(f"\n🔌 Total Connectors: {len(unified_profile['connectors'])}")
    for name, config in unified_profile["connectors"].items():
        console.print(f"  • {name} ({config['type']})")

    console.print("\n📄 Generated Profile:")
    console.print("```yaml")
    console.print(yaml.dump(unified_profile, default_flow_style=False))
    console.print("```")

    console.print("\n💡 Run without --dry-run to write profile file")


def _write_migration_files(
    pipeline_path: Path,
    profile_dir: str,
    profile_name: str,
    profile_config: Dict,
    migrated_content: str,
    force: bool,
) -> None:
    """Write migration files to disk."""
    # Create profile directory
    profile_path = Path(profile_dir)
    profile_path.mkdir(parents=True, exist_ok=True)

    # Write profile file
    profile_file = profile_path / f"{profile_name}.yml"
    if profile_file.exists() and not force:
        console.print(f"❌ Profile file exists: {profile_file}")
        console.print("   Use --force to overwrite")
        raise typer.Exit(code=1)

    with open(profile_file, "w") as f:
        yaml.dump(profile_config, f, default_flow_style=False)

    # Create backup of original pipeline
    backup_file = pipeline_path.with_suffix(pipeline_path.suffix + ".backup")
    if not backup_file.exists():
        import shutil

        shutil.copy2(pipeline_path, backup_file)
        console.print(f"📄 Created backup: {backup_file}")

    # Write migrated pipeline
    with open(pipeline_path, "w") as f:
        f.write(migrated_content)

    console.print("✅ Migration completed!")
    console.print(f"   📄 Profile: {profile_file}")
    console.print(f"   📝 Pipeline: {pipeline_path}")
    console.print(f"   💾 Backup: {backup_file}")


def _write_extracted_profile(output_file: str, profile_config: Dict) -> None:
    """Write extracted profile to file."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        yaml.dump(profile_config, f, default_flow_style=False)

    console.print("✅ Profile extracted!")
    console.print(f"   📄 Output: {output_path}")
    console.print(f"   🔌 Connectors: {len(profile_config['connectors'])}")
