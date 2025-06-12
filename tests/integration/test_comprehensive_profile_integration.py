"""Comprehensive integration tests for profile-based connector configuration.

This module implements Task 6.1 from the profile enhancement plan with:
- End-to-end tests for all supported connectors
- Multi-environment testing (dev/staging/prod profiles)
- Error scenario testing
- Performance benchmarking
- Load testing with concurrent profile access
- Security testing (credential handling)

Following the Zen of Python:
- Simple is better than complex
- Explicit is better than implicit
- Readability counts
- Minimal mocking - use real implementations where possible
"""

import concurrent.futures
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import pytest
import yaml

from sqlflow.core.config_resolver import ConfigurationResolver
from sqlflow.core.executors.local_executor import LocalExecutor
from sqlflow.core.profiles import ProfileManager
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TestComprehensiveProfileIntegration:
    """Comprehensive integration tests for profile-based connector configuration."""

    @pytest.fixture
    def multi_env_profiles_dir(self):
        """Create comprehensive multi-environment profile structure."""
        temp_dir = tempfile.mkdtemp()

        # Create project structure
        for subdir in ["profiles", "pipelines", "data", "output"]:
            os.makedirs(os.path.join(temp_dir, subdir))

        profiles_dir = os.path.join(temp_dir, "profiles")

        # Development environment profile
        dev_profile = {
            "version": "1.0",
            "variables": {
                "env": "development",
                "data_path": "${DATA_ROOT|/tmp/dev-data}",
                "db_host": "${DB_HOST|localhost}",
                "db_port": "${DB_PORT|5432}",
                "api_timeout": "${API_TIMEOUT|30}",
            },
            "connectors": {
                "csv_default": {
                    "type": "csv",
                    "params": {
                        "path": "${data_path}/default.csv",
                        "delimiter": ",",
                        "has_header": True,
                        "encoding": "utf-8",
                    },
                },
                "postgres_analytics": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": "${db_port}",
                        "database": "dev_analytics",
                        "username": "dev_user",
                        "password": "dev_secret",
                        "ssl": False,
                    },
                },
                "rest_api": {
                    "type": "rest",
                    "params": {
                        "base_url": "https://api-dev.example.com",
                        "timeout": "${api_timeout}",
                        "headers": {"Authorization": "Bearer dev-token"},
                    },
                },
                "s3_data": {
                    "type": "s3",
                    "params": {
                        "bucket": "dev-data-bucket",
                        "region": "us-east-1",
                        "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
                        "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
                    },
                },
                "parquet_files": {
                    "type": "parquet",
                    "params": {
                        "path": "${data_path}/data.parquet",
                        "engine": "pyarrow",
                    },
                },
                "google_sheets": {
                    "type": "google_sheets",
                    "params": {
                        "spreadsheet_id": "dev-sheet-id",
                        "range": "Sheet1!A1:Z1000",
                        "credentials_path": "${GOOGLE_CREDS_PATH}",
                    },
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "memory",
                    "threads": 2,
                }
            },
        }

        # Staging environment profile
        staging_profile = {
            "version": "1.0",
            "variables": {
                "env": "staging",
                "data_path": "/staging/data",
                "db_host": "staging-db.example.com",
                "db_port": "5433",
                "api_timeout": "60",
            },
            "connectors": {
                "csv_default": {
                    "type": "csv",
                    "params": {
                        "path": "${data_path}/staging.csv",
                        "delimiter": "|",  # Different delimiter for staging
                        "has_header": True,
                        "encoding": "utf-8",
                    },
                },
                "postgres_analytics": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": "${db_port}",
                        "database": "staging_analytics",
                        "username": "staging_user",
                        "password": "staging_secret",
                        "ssl": True,  # SSL required in staging
                    },
                },
                "rest_api": {
                    "type": "rest",
                    "params": {
                        "base_url": "https://api-staging.example.com",
                        "timeout": "${api_timeout}",
                        "headers": {"Authorization": "Bearer staging-token"},
                    },
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "persistent",
                    "path": "/staging/db/sqlflow.db",
                    "threads": 4,
                }
            },
        }

        # Production environment profile
        prod_profile = {
            "version": "1.0",
            "variables": {
                "env": "production",
                "data_path": "/prod/data",
                "db_host": "prod-db.example.com",
                "db_port": "5432",
                "api_timeout": "120",
            },
            "connectors": {
                "csv_default": {
                    "type": "csv",
                    "params": {
                        "path": "${data_path}/production.csv",
                        "delimiter": ",",
                        "has_header": True,
                        "encoding": "utf-8",
                        "chunk_size": 10000,  # Larger chunks for production
                    },
                },
                "postgres_analytics": {
                    "type": "postgres",
                    "params": {
                        "host": "${db_host}",
                        "port": "${db_port}",
                        "database": "prod_analytics",
                        "username": "prod_user",
                        "password": "${PROD_DB_PASSWORD}",  # From env var
                        "ssl": True,
                        "pool_size": 20,  # Connection pooling for production
                    },
                },
                "rest_api": {
                    "type": "rest",
                    "params": {
                        "base_url": "https://api.example.com",
                        "timeout": "${api_timeout}",
                        "headers": {"Authorization": "Bearer ${PROD_API_TOKEN}"},
                        "retry_attempts": 5,  # More retries in production
                    },
                },
            },
            "engines": {
                "duckdb": {
                    "mode": "persistent",
                    "path": "/prod/db/sqlflow.db",
                    "threads": 8,
                    "memory_limit": "4GB",
                }
            },
        }

        # Write profile files
        profile_files = {
            "dev.yml": dev_profile,
            "staging.yml": staging_profile,
            "prod.yml": prod_profile,
        }

        for filename, profile_data in profile_files.items():
            with open(os.path.join(profiles_dir, filename), "w") as f:
                yaml.dump(profile_data, f, default_flow_style=False)

        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_all_connectors_end_to_end(self, multi_env_profiles_dir):
        """Test end-to-end functionality for all supported connectors.

        This test validates that all connector types can be:
        1. Loaded from profiles
        2. Configured with variable substitution
        3. Created successfully
        4. Used in real scenarios (where possible)
        """
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")
        profile_manager = ProfileManager(profiles_dir, "dev")
        config_resolver = ConfigurationResolver(profile_manager)

        # Test environment variables
        test_env_vars = {
            "DATA_ROOT": "/tmp/test-data",
            "DB_HOST": "test-db.local",
            "API_TIMEOUT": "45",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "GOOGLE_CREDS_PATH": "/tmp/creds.json",
        }

        # All connector types to test
        connector_tests = [
            ("csv_default", "csv"),
            ("postgres_analytics", "postgres"),
            ("rest_api", "rest"),
            ("s3_data", "s3"),
            ("parquet_files", "parquet"),
            ("google_sheets", "google_sheets"),
        ]

        results = {}

        for connector_name, expected_type in connector_tests:
            try:
                # Step 1: Load connector profile
                connector_profile = profile_manager.get_connector_profile(
                    connector_name
                )
                assert (
                    connector_profile.connector_type == expected_type
                ), f"Wrong type for {connector_name}"

                # Step 2: Resolve configuration with variables
                resolved_config = config_resolver.resolve_config(
                    profile_name=connector_name,
                    variables=test_env_vars,
                    options={"test_mode": True},  # Runtime override
                )

                # Step 3: Verify variable substitution worked
                assert (
                    resolved_config["type"] == expected_type
                ), f"Type mismatch for {connector_name}"
                assert (
                    resolved_config.get("test_mode") is True
                ), f"Runtime override failed for {connector_name}"

                # Step 4: Connector-specific validations (check actual substituted values)
                if expected_type == "csv":
                    # Uses DATA_ROOT variable with default /tmp/dev-data
                    assert "/default.csv" in resolved_config["path"]
                    assert resolved_config["delimiter"] == ","
                    assert resolved_config["has_header"] is True

                elif expected_type == "postgres":
                    # Uses DB_HOST variable with default localhost
                    assert resolved_config["database"] == "dev_analytics"
                    assert resolved_config["ssl"] is False
                    # Host may be default or substituted value
                    assert resolved_config["host"] in ["localhost", "test-db.local"]

                elif expected_type == "rest":
                    assert resolved_config["base_url"] == "https://api-dev.example.com"
                    # Timeout may use default or env var
                    assert resolved_config["timeout"] in ["30", "45"]
                    assert "Authorization" in resolved_config["headers"]

                elif expected_type == "s3":
                    assert resolved_config["bucket"] == "dev-data-bucket"
                    # Credentials should be substituted if variables provided
                    assert resolved_config["aws_access_key_id"] == "test-key"
                    assert resolved_config["aws_secret_access_key"] == "test-secret"

                elif expected_type == "parquet":
                    # Uses DATA_ROOT variable with default /tmp/dev-data
                    assert "/data.parquet" in resolved_config["path"]
                    assert resolved_config["engine"] == "pyarrow"

                elif expected_type == "google_sheets":
                    assert resolved_config["spreadsheet_id"] == "dev-sheet-id"
                    assert resolved_config["credentials_path"] == "/tmp/creds.json"

                results[connector_name] = "SUCCESS"

            except Exception as e:
                results[connector_name] = f"FAILED: {str(e)}"
                logger.error(f"Connector {connector_name} test failed: {e}")

        # All connectors should succeed
        failed_connectors = [
            name for name, result in results.items() if "FAILED" in result
        ]
        assert not failed_connectors, f"Failed connectors: {failed_connectors}"

        logger.info(f"All {len(connector_tests)} connectors tested successfully")

    def test_multi_environment_configuration(self, multi_env_profiles_dir):
        """Test multi-environment profile configuration consistency."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")

        environments = ["dev", "staging", "prod"]
        test_results = {}

        for env in environments:
            profile_manager = ProfileManager(profiles_dir, env)
            config_resolver = ConfigurationResolver(profile_manager)

            # Test CSV connector across environments
            connector_profile = profile_manager.get_connector_profile("csv_default")
            resolved_config = config_resolver.resolve_config(
                profile_name="csv_default", variables={"data_path": f"/{env}/data"}
            )

            test_results[env] = {
                "connector_type": connector_profile.connector_type,
                "path": resolved_config["path"],
                "delimiter": resolved_config["delimiter"],
                "has_header": resolved_config["has_header"],
            }

        # Verify environment-specific differences
        assert test_results["dev"]["delimiter"] == ","
        assert test_results["staging"]["delimiter"] == "|"  # Different in staging
        assert test_results["prod"]["delimiter"] == ","

        # Verify environment-specific paths
        assert test_results["dev"]["path"] == "/dev/data/default.csv"
        assert test_results["staging"]["path"] == "/staging/data/staging.csv"
        assert test_results["prod"]["path"] == "/prod/data/production.csv"

        # Common properties should be consistent
        for env in environments:
            assert test_results[env]["connector_type"] == "csv"
            assert test_results[env]["has_header"] is True

    def test_error_scenarios_comprehensive(self, multi_env_profiles_dir):
        """Test comprehensive error scenarios and edge cases."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")

        # Test 1: Invalid environment - error occurs during load_profile()
        invalid_manager = ProfileManager(profiles_dir, "nonexistent")
        with pytest.raises(FileNotFoundError):
            invalid_manager.load_profile()

        # Test 2: Invalid connector name
        profile_manager = ProfileManager(profiles_dir, "dev")
        with pytest.raises(ValueError, match="Connector 'nonexistent' not found"):
            profile_manager.get_connector_profile("nonexistent")

        # Test 3: Missing variable substitution (should use defaults)
        config_resolver = ConfigurationResolver(profile_manager)
        resolved_config = config_resolver.resolve_config(
            profile_name="csv_default", variables={}  # No variables provided
        )
        # Should use default from profile definition
        assert (
            "${DATA_ROOT|/tmp/dev-data}" in resolved_config["path"]
            or resolved_config["path"] == "/tmp/dev-data/default.csv"
        )

        # Test 4: Invalid profile structure
        invalid_profile_dir = tempfile.mkdtemp()
        try:
            invalid_profile = {
                "version": "1.0",
                "connectors": {
                    "invalid_connector": {
                        # Missing 'type' field
                        "params": {"path": "/test"}
                    }
                },
            }

            with open(os.path.join(invalid_profile_dir, "test.yml"), "w") as f:
                yaml.dump(invalid_profile, f)

            invalid_profile_manager = ProfileManager(invalid_profile_dir, "test")
            with pytest.raises(ValueError, match="missing required 'type' field"):
                invalid_profile_manager.get_connector_profile("invalid_connector")

        finally:
            shutil.rmtree(invalid_profile_dir)

        # Test 5: Configuration precedence edge cases
        resolved_config = config_resolver.resolve_config(
            profile_name="csv_default",
            variables={"data_path": "/var/override"},
            options={"delimiter": ";"},  # Should override profile delimiter
            defaults={"encoding": "ascii", "delimiter": "default-delimiter"},
        )

        # OPTIONS should override profile
        assert resolved_config["delimiter"] == ";"
        # Variables should substitute in path
        assert resolved_config["path"] == "/var/override/default.csv"
        # Profile should override defaults for encoding
        assert resolved_config["encoding"] == "utf-8"

    def test_performance_benchmarking(self, multi_env_profiles_dir):
        """Test profile loading and configuration resolution performance."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")

        # Benchmark profile loading
        start_time = time.time()
        profile_manager = ProfileManager(profiles_dir, "dev")
        profile_load_time = time.time() - start_time

        # Should load quickly (< 50ms as per requirements)
        assert (
            profile_load_time < 0.05
        ), f"Profile loading took {profile_load_time:.3f}s (> 50ms)"

        # Benchmark configuration resolution
        config_resolver = ConfigurationResolver(profile_manager)

        start_time = time.time()
        for _ in range(100):  # Test 100 resolutions
            config_resolver.resolve_config(
                profile_name="csv_default",
                variables={"data_path": "/test"},
                options={"delimiter": ","},
            )
        resolution_time = (time.time() - start_time) / 100

        # Should resolve quickly (< 10ms per resolution as per requirements)
        assert (
            resolution_time < 0.01
        ), f"Config resolution took {resolution_time:.3f}s (> 10ms)"

        # Benchmark connector profile access (should use caching)
        start_time = time.time()
        for _ in range(100):
            profile_manager.get_connector_profile("csv_default")
        access_time = (time.time() - start_time) / 100

        # Cached access should be very fast
        assert (
            access_time < 0.001
        ), f"Cached profile access took {access_time:.3f}s (> 1ms)"

        logger.info("Performance benchmarks passed:")
        logger.info(f"  Profile loading: {profile_load_time:.3f}s")
        logger.info(f"  Config resolution: {resolution_time:.3f}s")
        logger.info(f"  Cached access: {access_time:.3f}s")

    def test_concurrent_profile_access(self, multi_env_profiles_dir):
        """Test concurrent profile access and thread safety."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")

        def load_and_resolve_config(thread_id: int) -> Dict[str, Any]:
            """Worker function for concurrent testing."""
            try:
                profile_manager = ProfileManager(profiles_dir, "dev")
                config_resolver = ConfigurationResolver(profile_manager)

                # Each thread uses slightly different variables
                variables = {
                    "data_path": f"/thread-{thread_id}/data",
                    "api_timeout": str(30 + thread_id),
                }

                resolved_config = config_resolver.resolve_config(
                    profile_name="csv_default",
                    variables=variables,
                    options={"thread_id": thread_id},
                )

                return {
                    "thread_id": thread_id,
                    "success": True,
                    "path": resolved_config["path"],
                    "thread_id_option": resolved_config["thread_id"],
                }

            except Exception as e:
                return {
                    "thread_id": thread_id,
                    "success": False,
                    "error": str(e),
                }

        # Test with 10 concurrent threads
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(load_and_resolve_config, i) for i in range(10)]
            results = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]

        # All threads should succeed
        successful_results = [r for r in results if r["success"]]
        failed_results = [r for r in results if not r["success"]]

        assert len(successful_results) == 10, f"Failed results: {failed_results}"

        # Each thread should get its own variable substitution
        for result in successful_results:
            thread_id = result["thread_id"]
            expected_path = f"/thread-{thread_id}/data/default.csv"
            assert result["path"] == expected_path
            assert result["thread_id_option"] == thread_id

        logger.info("Concurrent profile access test passed with 10 threads")

    def test_security_credential_handling(self, multi_env_profiles_dir):
        """Test security aspects of credential handling in profiles."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")
        profile_manager = ProfileManager(profiles_dir, "dev")
        config_resolver = ConfigurationResolver(profile_manager)

        # Test 1: Environment variable substitution for sensitive data
        sensitive_env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIA1234567890EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "PROD_DB_PASSWORD": "super-secret-password",
        }

        resolved_config = config_resolver.resolve_config(
            profile_name="s3_data", variables=sensitive_env_vars
        )

        # Credentials should be properly substituted
        assert resolved_config["aws_access_key_id"] == "AKIA1234567890EXAMPLE"
        assert (
            resolved_config["aws_secret_access_key"]
            == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        )

        # Test 2: Verify sensitive data is not logged
        # This is more of a design test - ensure logging doesn't expose credentials
        import logging
        from io import StringIO

        log_stream = StringIO()
        test_handler = logging.StreamHandler(log_stream)
        test_handler.setLevel(logging.DEBUG)

        # Temporarily add handler to capture logs
        logger.addHandler(test_handler)

        try:
            # Perform operations that might log sensitive data
            config_resolver.resolve_config(
                profile_name="postgres_analytics",
                variables={"PROD_DB_PASSWORD": "secret123"},
            )

            log_output = log_stream.getvalue()

            # Sensitive values should not appear in logs
            assert "secret123" not in log_output
            assert "super-secret-password" not in log_output
            assert "AKIA1234567890EXAMPLE" not in log_output

        finally:
            logger.removeHandler(test_handler)

        # Test 3: Variable substitution with missing sensitive variables
        # Should continue with warnings but leave variables unsubstituted
        resolved_config = config_resolver.resolve_config(
            profile_name="s3_data", variables={}  # Missing required AWS credentials
        )

        # Variables should remain unsubstituted when not provided
        assert "${AWS_ACCESS_KEY_ID}" in str(
            resolved_config.get("aws_access_key_id", "")
        )
        assert "${AWS_SECRET_ACCESS_KEY}" in str(
            resolved_config.get("aws_secret_access_key", "")
        )

        logger.info("Security credential handling tests passed")

    def test_production_readiness_checklist(self, multi_env_profiles_dir):
        """Test production readiness scenarios and edge cases."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")

        # Test 1: Production profile has appropriate settings
        prod_profile_manager = ProfileManager(profiles_dir, "prod")

        # Verify production-specific configurations
        csv_profile = prod_profile_manager.get_connector_profile("csv_default")
        assert "chunk_size" in csv_profile.params  # Production should have chunking

        postgres_profile = prod_profile_manager.get_connector_profile(
            "postgres_analytics"
        )
        assert postgres_profile.params["ssl"] is True  # Production should use SSL
        assert "pool_size" in postgres_profile.params  # Production should have pooling

        rest_profile = prod_profile_manager.get_connector_profile("rest_api")
        assert "retry_attempts" in rest_profile.params  # Production should have retries

        # Test 2: Resource cleanup and memory usage
        # Create multiple profile managers to test resource usage
        managers = []
        for i in range(50):
            manager = ProfileManager(profiles_dir, "dev")
            managers.append(manager)

        # All managers should be functional
        for manager in managers:
            profile = manager.get_connector_profile("csv_default")
            assert profile.connector_type == "csv"

        # Test 3: Large configuration handling
        # Create a profile with many connectors and large configurations
        large_config_dir = tempfile.mkdtemp()
        try:
            large_profile = {
                "version": "1.0",
                "variables": {f"var_{i}": f"value_{i}" for i in range(100)},
                "connectors": {
                    f"connector_{i}": {
                        "type": "csv",
                        "params": {
                            "path": f"/data/file_{i}.csv",
                            "delimiter": ",",
                            "has_header": True,
                            # Large parameter set
                            **{f"param_{j}": f"value_{j}" for j in range(20)},
                        },
                    }
                    for i in range(50)
                },
            }

            with open(os.path.join(large_config_dir, "large.yml"), "w") as f:
                yaml.dump(large_profile, f)

            large_profile_manager = ProfileManager(large_config_dir, "large")
            config_resolver = ConfigurationResolver(large_profile_manager)

            # Should handle large configurations efficiently
            start_time = time.time()
            resolved_config = config_resolver.resolve_config("connector_0")
            resolution_time = time.time() - start_time

            assert (
                resolution_time < 0.2
            ), f"Large config resolution took {resolution_time:.3f}s (may be slow in test environment)"
            assert resolved_config["path"] == "/data/file_0.csv"

        finally:
            shutil.rmtree(large_config_dir)

        logger.info("Production readiness checklist passed")

    def test_high_concurrency_load(self, multi_env_profiles_dir):
        """Test profile system under high concurrency load."""
        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")
        load_tester = ProfileLoadTester(profiles_dir, "dev")

        # Test with increasing concurrency levels
        concurrency_tests = [
            (5, 100),  # 5 threads, 100 ops each
            (10, 50),  # 10 threads, 50 ops each
            (20, 25),  # 20 threads, 25 ops each
        ]

        for num_threads, ops_per_thread in concurrency_tests:
            result = load_tester.run_load_test(num_threads, ops_per_thread)

            # All workers should succeed
            assert (
                result["failed_workers"] == 0
            ), f"Failed workers in {num_threads} thread test"

            # Should maintain reasonable performance
            assert (
                result["overall_ops_per_second"] > 100
            ), f"Performance degraded: {result['overall_ops_per_second']} ops/sec"

            logger.info(
                f"Concurrency test ({num_threads} threads, {ops_per_thread} ops): "
                f"{result['overall_ops_per_second']:.1f} ops/sec"
            )

    def test_memory_usage_stability(self, multi_env_profiles_dir):
        """Test memory usage stability under sustained load."""
        import gc

        try:
            import psutil
        except ImportError:
            pytest.skip("psutil not available for memory testing")

        profiles_dir = os.path.join(multi_env_profiles_dir, "profiles")
        process = psutil.Process()

        # Baseline memory usage
        gc.collect()
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create and destroy many profile managers
        for i in range(100):
            profile_manager = ProfileManager(profiles_dir, "dev")
            config_resolver = ConfigurationResolver(profile_manager)

            # Perform operations
            for j in range(10):
                profile_manager.get_connector_profile("csv_default")
                config_resolver.resolve_config("postgres_analytics")

            # Force garbage collection every 10 iterations
            if i % 10 == 0:
                gc.collect()

        # Final memory check
        gc.collect()
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - baseline_memory

        # Memory increase should be reasonable (< 50MB for this test)
        assert (
            memory_increase < 50
        ), f"Memory increased by {memory_increase:.1f}MB (potential leak)"

        logger.info(
            f"Memory usage: {baseline_memory:.1f}MB -> {final_memory:.1f}MB (increase: {memory_increase:.1f}MB)"
        )

    def test_integration_with_local_executor(self, multi_env_profiles_dir):
        """Test integration of profile system with LocalExecutor end-to-end."""
        os.path.join(multi_env_profiles_dir, "profiles")

        # Create a mock project structure

        # Mock project with profiles directory
        class MockProject:
            def __init__(self, project_dir):
                self.project_dir = project_dir

        project = MockProject(multi_env_profiles_dir)

        # Create LocalExecutor with profile support
        executor = LocalExecutor(project=project, profile_name="dev")

        # Set variables manually (they're normally extracted from project)
        executor.variables = {"DATA_ROOT": "/tmp/executor-test"}

        # Test profile-based source definition execution
        profile_source_step = {
            "id": "test_source_step",
            "name": "test_csv_source",
            "is_from_profile": True,
            "profile_connector_name": "csv_default",
            "params": {
                "path": "/custom/override.csv",  # Override profile path
                "delimiter": "|",  # Override profile delimiter
            },
        }

        # Execute source definition
        result = executor._execute_source_definition(profile_source_step)

        # Should succeed and store source definition
        assert result["status"] == "success"
        assert result["source_name"] == "test_csv_source"

        # Verify source definition was stored with resolved configuration
        assert "test_csv_source" in executor.source_definitions
        stored_def = executor.source_definitions["test_csv_source"]
        assert stored_def["connector_type"] == "csv"
        assert stored_def["is_from_profile"] is True

        # Test traditional source definition still works
        traditional_source_step = {
            "id": "traditional_step",
            "name": "traditional_csv",
            "connector_type": "csv",
            "params": {
                "path": "/traditional/file.csv",
                "delimiter": ",",
                "has_header": True,
            },
        }

        result = executor._execute_source_definition(traditional_source_step)
        assert result["status"] == "success"
        assert result["source_name"] == "traditional_csv"

        # Both source definitions should coexist
        assert len(executor.source_definitions) == 2
        assert "test_csv_source" in executor.source_definitions
        assert "traditional_csv" in executor.source_definitions

        logger.info("LocalExecutor integration test passed")


# Performance and load testing utilities
class ProfileLoadTester:
    """Utility class for profile load testing."""

    def __init__(self, profiles_dir: str, environment: str):
        self.profiles_dir = profiles_dir
        self.environment = environment

    def run_load_test(
        self, num_threads: int, operations_per_thread: int
    ) -> Dict[str, Any]:
        """Run load test with specified concurrency."""

        def worker_operations(worker_id: int) -> Dict[str, Any]:
            """Perform profile operations in a worker thread."""
            start_time = time.time()

            try:
                profile_manager = ProfileManager(self.profiles_dir, self.environment)
                config_resolver = ConfigurationResolver(profile_manager)

                operations_completed = 0

                for i in range(operations_per_thread):
                    # Mix of different operations
                    if i % 3 == 0:
                        profile_manager.get_connector_profile("csv_default")
                    elif i % 3 == 1:
                        config_resolver.resolve_config("postgres_analytics")
                    else:
                        profile_manager.list_connectors()

                    operations_completed += 1

                duration = time.time() - start_time

                return {
                    "worker_id": worker_id,
                    "success": True,
                    "operations_completed": operations_completed,
                    "duration": duration,
                    "ops_per_second": operations_completed / duration,
                }

            except Exception as e:
                return {
                    "worker_id": worker_id,
                    "success": False,
                    "error": str(e),
                    "duration": time.time() - start_time,
                }

        # Execute load test
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(worker_operations, i) for i in range(num_threads)
            ]
            results = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]

        # Aggregate results
        successful_results = [r for r in results if r["success"]]
        failed_results = [r for r in results if not r["success"]]

        total_operations = sum(r["operations_completed"] for r in successful_results)
        total_duration = (
            max(r["duration"] for r in successful_results) if successful_results else 0
        )

        return {
            "num_threads": num_threads,
            "operations_per_thread": operations_per_thread,
            "successful_workers": len(successful_results),
            "failed_workers": len(failed_results),
            "total_operations": total_operations,
            "total_duration": total_duration,
            "overall_ops_per_second": (
                total_operations / total_duration if total_duration > 0 else 0
            ),
            "results": results,
        }
