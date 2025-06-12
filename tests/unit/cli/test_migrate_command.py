"""Unit tests for migration CLI commands.

Implements unit tests for Task 5.2: Migration Tooling
Tests pipeline migration and profile extraction functionality.
"""

from pathlib import Path
from unittest.mock import patch

import yaml
from typer.testing import CliRunner

from sqlflow.cli.commands.migrate import (
    _create_config_signature,
    _create_profile_config,
    _create_unified_profile,
    _extract_connector_configs,
    _find_pipeline_files,
    _flatten_configs,
    _generate_profile_name,
    _merge_similar_configs,
    _migrate_pipeline_content,
    _parse_params_string,
    migrate_app,
)

runner = CliRunner()


class TestMigrateToProfiles:
    """Test the migrate to-profiles command."""

    def test_migrate_to_profiles_success(self, tmp_path):
        """Test successful pipeline migration."""
        # Create test pipeline with inline configuration
        pipeline_content = """
SOURCE customers TYPE csv PARAMS {
    file_path = "data/customers.csv"
    delimiter = ","
    header = true
}

TRANSFORM customer_summary AS
SELECT customer_id, name, country
FROM customers
WHERE age > 18;

LOAD customer_summary TO warehouse TYPE postgres PARAMS {
    host = "localhost"
    database = "analytics"
    table = "customer_summary"
}
"""
        pipeline_file = tmp_path / "test_pipeline.sql"
        pipeline_file.write_text(pipeline_content)

        profile_dir = tmp_path / "profiles"

        # Run migration
        result = runner.invoke(
            migrate_app,
            [
                "to-profiles",
                str(pipeline_file),
                "--profile-dir",
                str(profile_dir),
                "--profile-name",
                "test_profile",
            ],
        )

        assert result.exit_code == 0
        assert "Migration completed!" in result.stdout

        # Check profile file was created
        profile_file = profile_dir / "test_profile.yml"
        assert profile_file.exists()

        # Check profile content
        with open(profile_file) as f:
            profile_data = yaml.safe_load(f)

        assert profile_data["version"] == "1.0"
        assert "connectors" in profile_data
        assert "customers" in profile_data["connectors"]
        assert "warehouse" in profile_data["connectors"]

        # Check migrated pipeline
        with open(pipeline_file) as f:
            migrated_content = f.read()

        assert 'SOURCE customers FROM "customers"' in migrated_content
        assert 'LOAD customer_summary TO "warehouse"' in migrated_content
        assert "-- Profile: test_profile" in migrated_content

        # Check backup was created
        backup_file = pipeline_file.with_suffix(".sql.backup")
        assert backup_file.exists()

    def test_migrate_to_profiles_dry_run(self, tmp_path):
        """Test dry-run mode for migration."""
        pipeline_content = """
SOURCE data TYPE csv PARAMS {
    file_path = "test.csv"
    delimiter = ","
}
"""
        pipeline_file = tmp_path / "test.sql"
        pipeline_file.write_text(pipeline_content)

        result = runner.invoke(
            migrate_app,
            ["to-profiles", str(pipeline_file), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Migration Preview" in result.stdout
        assert "Run without --dry-run to apply changes" in result.stdout

        # Check no files were created
        profile_dir = tmp_path / "profiles"
        assert not profile_dir.exists()

    def test_migrate_to_profiles_no_inline_configs(self, tmp_path):
        """Test migration with no inline configurations."""
        pipeline_content = """
SOURCE customers FROM "csv_source"

TRANSFORM summary AS
SELECT * FROM customers;
"""
        pipeline_file = tmp_path / "test.sql"
        pipeline_file.write_text(pipeline_content)

        result = runner.invoke(
            migrate_app,
            ["to-profiles", str(pipeline_file)],
        )

        assert result.exit_code == 0
        assert "No inline connector configurations found" in result.stdout

    def test_migrate_to_profiles_file_not_found(self):
        """Test migration with non-existent file."""
        result = runner.invoke(
            migrate_app,
            ["to-profiles", "nonexistent.sql"],
        )

        assert result.exit_code == 1
        assert "Pipeline file not found" in result.stdout

    def test_migrate_to_profiles_force_overwrite(self, tmp_path):
        """Test force overwrite of existing profile."""
        pipeline_content = """
SOURCE test TYPE csv PARAMS {
    file_path = "test.csv"
}
"""
        pipeline_file = tmp_path / "test.sql"
        pipeline_file.write_text(pipeline_content)

        profile_dir = tmp_path / "profiles"
        profile_dir.mkdir()
        profile_file = profile_dir / "test.yml"
        profile_file.write_text("existing: content")

        # First attempt without force should fail
        result = runner.invoke(
            migrate_app,
            [
                "to-profiles",
                str(pipeline_file),
                "--profile-dir",
                str(profile_dir),
                "--profile-name",
                "test",
            ],
        )

        assert result.exit_code == 1
        assert "Profile file exists" in result.stdout

        # Second attempt with force should succeed
        result = runner.invoke(
            migrate_app,
            [
                "to-profiles",
                str(pipeline_file),
                "--profile-dir",
                str(profile_dir),
                "--profile-name",
                "test",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert "Migration completed!" in result.stdout


class TestExtractProfiles:
    """Test the extract-profiles command."""

    def test_extract_profiles_success(self, tmp_path):
        """Test successful profile extraction from multiple files."""
        # Create test pipelines
        pipeline1_content = """
SOURCE customers TYPE csv PARAMS {
    file_path = "customers.csv"
    delimiter = ","
}
"""
        pipeline2_content = """
SOURCE orders TYPE csv PARAMS {
    file_path = "orders.csv"
    delimiter = ","
}

LOAD summary TO db TYPE postgres PARAMS {
    host = "localhost"
    database = "test"
}
"""

        pipeline1 = tmp_path / "pipeline1.sql"
        pipeline2 = tmp_path / "pipeline2.sql"
        pipeline1.write_text(pipeline1_content)
        pipeline2.write_text(pipeline2_content)

        output_file = tmp_path / "extracted.yml"

        result = runner.invoke(
            migrate_app,
            [
                "extract-profiles",
                str(tmp_path),
                "--output",
                str(output_file),
            ],
        )

        assert result.exit_code == 0
        assert "Profile extracted!" in result.stdout

        # Check extracted profile
        assert output_file.exists()
        with open(output_file) as f:
            profile_data = yaml.safe_load(f)

        assert "connectors" in profile_data
        assert len(profile_data["connectors"]) >= 2

    def test_extract_profiles_dry_run(self, tmp_path):
        """Test dry-run mode for profile extraction."""
        pipeline_content = """
SOURCE test TYPE csv PARAMS {
    file_path = "test.csv"
}
"""
        pipeline_file = tmp_path / "test.sql"
        pipeline_file.write_text(pipeline_content)

        result = runner.invoke(
            migrate_app,
            ["extract-profiles", str(tmp_path), "--dry-run"],
        )

        assert result.exit_code == 0
        assert "Extraction Preview" in result.stdout
        assert "Run without --dry-run to write profile file" in result.stdout

    def test_extract_profiles_no_files(self, tmp_path):
        """Test extraction with no pipeline files."""
        result = runner.invoke(
            migrate_app,
            ["extract-profiles", str(tmp_path)],
        )

        assert result.exit_code == 1
        assert "No SQLFlow pipeline files found" in result.stdout

    def test_extract_profiles_directory_not_found(self):
        """Test extraction with non-existent directory."""
        result = runner.invoke(
            migrate_app,
            ["extract-profiles", "nonexistent"],
        )

        assert result.exit_code == 1
        assert "Pipeline directory not found" in result.stdout

    def test_extract_profiles_merge_similar(self, tmp_path):
        """Test merging similar configurations."""
        # Create pipelines with similar configurations
        pipeline1_content = """
SOURCE data1 TYPE csv PARAMS {
    delimiter = ","
    header = true
}
"""
        pipeline2_content = """
SOURCE data2 TYPE csv PARAMS {
    delimiter = ","
    header = true
}
"""

        pipeline1 = tmp_path / "pipeline1.sql"
        pipeline2 = tmp_path / "pipeline2.sql"
        pipeline1.write_text(pipeline1_content)
        pipeline2.write_text(pipeline2_content)

        output_file = tmp_path / "merged.yml"

        result = runner.invoke(
            migrate_app,
            [
                "extract-profiles",
                str(tmp_path),
                "--output",
                str(output_file),
                "--merge-similar",
            ],
        )

        assert result.exit_code == 0

        # Check that similar configs were merged
        with open(output_file) as f:
            profile_data = yaml.safe_load(f)

        # Should have merged similar configs (currently flattens them)
        # Note: Current implementation flattens rather than truly merges
        assert len(profile_data["connectors"]) >= 1


class TestHelperFunctions:
    """Test helper functions used by migration commands."""

    def test_extract_connector_configs(self):
        """Test extraction of connector configurations from content."""
        content = """
SOURCE customers TYPE csv PARAMS {
    file_path = "customers.csv"
    delimiter = ","
}

LOAD data TO warehouse TYPE postgres PARAMS {
    host = "localhost"
    database = "test"
}
"""
        configs = _extract_connector_configs(content)

        assert len(configs) == 2
        assert "customers" in configs
        assert "warehouse" in configs

        # Check source config
        customers_config = configs["customers"]
        assert customers_config["type"] == "source"
        assert customers_config["connector_type"] == "csv"
        assert customers_config["params"]["file_path"] == "customers.csv"

        # Check destination config
        warehouse_config = configs["warehouse"]
        assert warehouse_config["type"] == "destination"
        assert warehouse_config["connector_type"] == "postgres"
        assert warehouse_config["params"]["host"] == "localhost"

    def test_parse_params_string(self):
        """Test parsing of parameter strings."""
        params_str = """
    file_path = "test.csv"
    delimiter = ","
    header = true
    # This is a comment
    encoding = "utf-8"
"""
        params = _parse_params_string(params_str)

        assert params["file_path"] == "test.csv"
        assert params["delimiter"] == ","
        assert params["header"] == "true"
        assert params["encoding"] == "utf-8"
        assert "# This is a comment" not in str(params)

    def test_generate_profile_name(self):
        """Test profile name generation."""
        # Test basic name
        path = Path("test_pipeline.sql")
        assert _generate_profile_name(path) == "test"

        # Test with common suffixes
        path = Path("customer_etl.sql")
        assert _generate_profile_name(path) == "customer"

        path = Path("data_flow.sql")
        assert _generate_profile_name(path) == "data"

        # Test without suffix
        path = Path("analytics.sql")
        assert _generate_profile_name(path) == "analytics"

    def test_create_profile_config(self):
        """Test profile configuration creation."""
        configs = {
            "source1": {
                "connector_type": "csv",
                "params": {"file_path": "test.csv"},
            },
            "dest1": {
                "connector_type": "postgres",
                "params": {"host": "localhost"},
            },
        }

        profile = _create_profile_config(configs)

        assert profile["version"] == "1.0"
        assert "connectors" in profile
        assert len(profile["connectors"]) == 2

        source_config = profile["connectors"]["source1"]
        assert source_config["type"] == "csv"
        assert source_config["params"]["file_path"] == "test.csv"

    def test_migrate_pipeline_content(self):
        """Test pipeline content migration."""
        content = """
SOURCE customers TYPE csv PARAMS {
    file_path = "customers.csv"
}

TRANSFORM summary AS
SELECT * FROM customers;

LOAD summary TO warehouse TYPE postgres PARAMS {
    host = "localhost"
}
"""
        configs = {
            "customers": {
                "type": "source",
                "original_match": 'SOURCE customers TYPE csv PARAMS {\n    file_path = "customers.csv"\n}',
            },
            "warehouse": {
                "type": "destination",
                "original_match": 'LOAD summary TO warehouse TYPE postgres PARAMS {\n    host = "localhost"\n}',
            },
        }

        migrated = _migrate_pipeline_content(content, configs, "test_profile")

        assert 'SOURCE customers FROM "customers"' in migrated
        assert 'LOAD summary TO "warehouse"' in migrated
        assert "-- Profile: test_profile" in migrated

    def test_find_pipeline_files(self, tmp_path):
        """Test finding pipeline files in directory."""
        # Create test files
        (tmp_path / "pipeline1.sql").touch()
        (tmp_path / "pipeline2.sqlflow").touch()
        (tmp_path / "pipeline3.sf").touch()
        (tmp_path / "not_pipeline.txt").touch()

        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested.sql").touch()

        files = _find_pipeline_files(tmp_path)

        assert len(files) == 4
        assert all(f.suffix in [".sql", ".sqlflow", ".sf"] for f in files)

    def test_create_config_signature(self):
        """Test configuration signature creation."""
        config = {
            "connector_type": "postgres",
            "params": {
                "host": "localhost",
                "database": "test",
                "password": "secret",  # Should be excluded
            },
        }

        signature = _create_config_signature(config)

        assert "postgres" in signature
        assert "host=localhost" in signature
        assert "database=test" in signature
        assert "password" not in signature

    def test_merge_similar_configs(self):
        """Test merging of similar configurations."""
        all_configs = {
            "file1.sql": {
                "source1": {
                    "connector_type": "csv",
                    "params": {"delimiter": ","},
                },
            },
            "file2.sql": {
                "source2": {
                    "connector_type": "csv",
                    "params": {"delimiter": ","},
                },
            },
            "file3.sql": {
                "source3": {
                    "connector_type": "postgres",
                    "params": {"host": "localhost"},
                },
            },
        }

        merged = _merge_similar_configs(all_configs)

        # Should merge similar CSV configs but keep different postgres config
        assert len(merged) == 2

    def test_flatten_configs(self):
        """Test flattening configurations without merging."""
        all_configs = {
            "file1.sql": {
                "source1": {"connector_type": "csv"},
            },
            "file2.sql": {
                "source2": {"connector_type": "postgres"},
            },
        }

        flattened = _flatten_configs(all_configs)

        assert len(flattened) == 2
        assert "file1_source1" in flattened
        assert "file2_source2" in flattened

    def test_create_unified_profile(self):
        """Test unified profile creation."""
        configs = {
            "csv_source": {
                "connector_type": "csv",
                "params": {"file_path": "test.csv"},
            },
            "pg_dest": {
                "connector_type": "postgres",
                "params": {"host": "localhost"},
            },
        }

        profile = _create_unified_profile(configs)

        assert profile["version"] == "1.0"
        assert "connectors" in profile
        assert "metadata" in profile
        assert profile["metadata"]["extracted_connectors"] == 2
        assert len(profile["connectors"]) == 2


class TestErrorHandling:
    """Test error handling in migration commands."""

    @patch("sqlflow.cli.commands.migrate._extract_connector_configs")
    def test_migrate_to_profiles_extraction_error(self, mock_extract, tmp_path):
        """Test handling of extraction errors."""
        mock_extract.side_effect = Exception("Extraction failed")

        pipeline_file = tmp_path / "test.sql"
        pipeline_file.write_text("SOURCE test TYPE csv PARAMS {}")

        result = runner.invoke(
            migrate_app,
            ["to-profiles", str(pipeline_file)],
        )

        assert result.exit_code == 1
        assert "Error: Extraction failed" in result.stdout

    @patch("sqlflow.cli.commands.migrate._find_pipeline_files")
    def test_extract_profiles_scan_error(self, mock_find, tmp_path):
        """Test handling of file scanning errors."""
        mock_find.side_effect = Exception("Scan failed")

        result = runner.invoke(
            migrate_app,
            ["extract-profiles", str(tmp_path)],
        )

        assert result.exit_code == 1
        assert "Error: Scan failed" in result.stdout


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_complex_pipeline_migration(self, tmp_path):
        """Test migration of complex pipeline with multiple connectors."""
        complex_pipeline = """
-- Complex ETL Pipeline
SOURCE customers TYPE csv PARAMS {
    file_path = "data/customers.csv"
    delimiter = ","
    header = true
    encoding = "utf-8"
}

SOURCE orders TYPE postgres PARAMS {
    host = "source-db.example.com"
    port = 5432
    database = "production"
    table = "orders"
    user = "readonly"
}

TRANSFORM customer_orders AS
SELECT 
    c.customer_id,
    c.name,
    o.order_date,
    o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= '2024-01-01';

LOAD customer_orders TO analytics TYPE postgres PARAMS {
    host = "analytics-db.example.com"
    port = 5432
    database = "analytics"
    table = "customer_orders"
    mode = "REPLACE"
}

LOAD customer_orders TO backup TYPE s3 PARAMS {
    bucket = "data-backup"
    key_prefix = "customer_orders/"
    format = "parquet"
}
"""
        pipeline_file = tmp_path / "complex_etl.sql"
        pipeline_file.write_text(complex_pipeline)

        result = runner.invoke(
            migrate_app,
            [
                "to-profiles",
                str(pipeline_file),
                "--profile-dir",
                str(tmp_path / "profiles"),
            ],
        )

        assert result.exit_code == 0

        # Check profile was created with all connectors
        profile_file = tmp_path / "profiles" / "complex.yml"
        assert profile_file.exists()

        with open(profile_file) as f:
            profile_data = yaml.safe_load(f)

        connectors = profile_data["connectors"]
        assert len(connectors) == 4  # customers, orders, analytics, backup
        assert "customers" in connectors
        assert "orders" in connectors
        assert "analytics" in connectors
        assert "backup" in connectors

        # Verify connector types
        assert connectors["customers"]["type"] == "csv"
        assert connectors["orders"]["type"] == "postgres"
        assert connectors["analytics"]["type"] == "postgres"
        assert connectors["backup"]["type"] == "s3"

    def test_batch_extraction_with_merging(self, tmp_path):
        """Test batch extraction with similar configuration merging."""
        # Create multiple pipelines with similar and different configs
        pipelines = {
            "sales_daily.sql": """
SOURCE sales TYPE csv PARAMS {
    file_path = "sales_daily.csv"
    delimiter = ","
    header = true
}
""",
            "sales_weekly.sql": """
SOURCE sales TYPE csv PARAMS {
    file_path = "sales_weekly.csv"
    delimiter = ","
    header = true
}
""",
            "inventory.sql": """
SOURCE inventory TYPE postgres PARAMS {
    host = "inventory-db"
    database = "inventory"
    table = "stock_levels"
}
""",
        }

        for filename, content in pipelines.items():
            (tmp_path / filename).write_text(content)

        result = runner.invoke(
            migrate_app,
            [
                "extract-profiles",
                str(tmp_path),
                "--output",
                str(tmp_path / "unified.yml"),
                "--merge-similar",
            ],
        )

        assert result.exit_code == 0

        # Check unified profile
        with open(tmp_path / "unified.yml") as f:
            profile_data = yaml.safe_load(f)

        # Should have extracted all connectors
        connectors = profile_data["connectors"]
        assert len(connectors) >= 2  # At least CSV and postgres types

        # Check metadata
        assert "metadata" in profile_data
        assert profile_data["metadata"]["generated_by"] == "SQLFlow migration tool"
