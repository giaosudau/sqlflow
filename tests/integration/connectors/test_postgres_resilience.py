"""Integration tests for PostgreSQL connector resilience patterns."""

import os
import threading
import time

import psycopg2
import pytest

from sqlflow.connectors.postgres_connector import PostgresConnector

# Mark all tests in this module as requiring external services
pytestmark = pytest.mark.external_services


@pytest.fixture(scope="module")
def docker_postgres_config():
    """Real PostgreSQL configuration from docker-compose.yml."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "postgres",
        "username": "postgres",
        "password": "postgres",
        "schema": "public",
    }


@pytest.fixture(scope="module")
def setup_postgres_test_environment(docker_postgres_config):
    """Set up PostgreSQL test environment."""
    # Skip if services are not available
    if not os.getenv("INTEGRATION_TESTS", "").lower() in ["true", "1"]:
        pytest.skip("Integration tests disabled. Set INTEGRATION_TESTS=true to enable.")

    try:
        # Test PostgreSQL connection
        conn = psycopg2.connect(
            host=docker_postgres_config["host"],
            port=docker_postgres_config["port"],
            database=docker_postgres_config["database"],
            user=docker_postgres_config["username"],
            password=docker_postgres_config["password"],
        )

        # Create test table and data
        cursor = conn.cursor()

        # Drop table if exists and create fresh
        cursor.execute("DROP TABLE IF EXISTS resilience_test_table")
        cursor.execute(
            """
            CREATE TABLE resilience_test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert test data
        test_data = [
            ("test_record_1", 100),
            ("test_record_2", 200),
            ("test_record_3", 300),
            ("test_record_4", 400),
            ("test_record_5", 500),
        ]

        cursor.executemany(
            "INSERT INTO resilience_test_table (name, value) VALUES (%s, %s)", test_data
        )

        # Create additional test tables for discovery testing
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50),
                email VARCHAR(100)
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                amount DECIMAL(10,2),
                status VARCHAR(20)
            )
        """
        )

        conn.commit()
        conn.close()

        yield

        # Cleanup
        try:
            conn = psycopg2.connect(
                host=docker_postgres_config["host"],
                port=docker_postgres_config["port"],
                database=docker_postgres_config["database"],
                user=docker_postgres_config["username"],
                password=docker_postgres_config["password"],
            )
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS resilience_test_table")
            cursor.execute("DROP TABLE IF EXISTS orders")
            cursor.execute("DROP TABLE IF EXISTS users")
            conn.commit()
            conn.close()
        except Exception:
            pass  # Cleanup errors are not critical

    except Exception as e:
        pytest.skip(f"PostgreSQL service not available: {e}")


class TestPostgresConnectorResilience:
    """Test PostgreSQL connector resilience patterns with real PostgreSQL service."""

    def test_real_connection_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test real connection to PostgreSQL with resilience patterns."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Verify resilience manager is set up
        assert hasattr(connector, "resilience_manager")
        assert connector.resilience_manager is not None

        # Test connection with real service
        result = connector.test_connection()
        assert result.success is True, f"Connection failed: {result.message}"
        assert "PostgreSQL" in result.message or "postgres" in result.message

    def test_real_discovery_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test discovery operations with real PostgreSQL service."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Test discovery
        discovered = connector.discover()
        assert len(discovered) > 0, "Should discover tables"

        # Should find our test tables
        table_names = [table.lower() for table in discovered]
        assert "resilience_test_table" in table_names
        assert "users" in table_names
        assert "orders" in table_names

    def test_real_data_reading_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test data reading with resilience patterns on real data."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Read real data from test table
        chunks = list(connector.read("resilience_test_table"))
        assert len(chunks) > 0, "Should read data chunks"

        # Verify data content
        df = chunks[0].pandas_df
        assert len(df) == 5, "Should have 5 test rows"
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns
        assert "created_at" in df.columns

        # Verify actual data values
        assert df["name"].tolist() == [
            "test_record_1",
            "test_record_2",
            "test_record_3",
            "test_record_4",
            "test_record_5",
        ]
        assert df["value"].tolist() == [100, 200, 300, 400, 500]

    def test_connection_retry_with_invalid_config(
        self, setup_postgres_test_environment
    ):
        """Test connection retry behavior with invalid configuration."""
        connector = PostgresConnector()

        # Test with invalid port
        invalid_config = {
            "host": "localhost",
            "port": 9999,  # Invalid port
            "database": "postgres",
            "username": "postgres",
            "password": "postgres",
        }

        connector.configure(invalid_config)

        # Should fail gracefully with resilience patterns after retries
        with pytest.raises(psycopg2.OperationalError) as exc_info:
            connector.test_connection()

        # Verify the error message indicates connection failure
        error_msg = str(exc_info.value).lower()
        assert "connection" in error_msg and (
            "refused" in error_msg or "failed" in error_msg
        )

    def test_authentication_failure_resilience(self, setup_postgres_test_environment):
        """Test resilience with authentication failures."""
        connector = PostgresConnector()

        # Test with invalid credentials
        invalid_config = {
            "host": "localhost",
            "port": 5432,
            "database": "postgres",
            "username": "invalid_user",
            "password": "invalid_password",
        }

        connector.configure(invalid_config)

        # Should fail gracefully (authentication errors are non-retryable)
        result = connector.test_connection()
        assert result.success is False
        assert (
            "authentication" in result.message.lower()
            or "password" in result.message.lower()
        )

    def test_database_not_found_resilience(self, setup_postgres_test_environment):
        """Test resilience with non-existent database."""
        connector = PostgresConnector()

        # Test with non-existent database
        invalid_config = {
            "host": "localhost",
            "port": 5432,
            "database": "nonexistent_database",
            "username": "postgres",
            "password": "postgres",
        }

        connector.configure(invalid_config)

        # Should fail gracefully (database not found errors are non-retryable)
        result = connector.test_connection()
        assert result.success is False
        assert (
            "database" in result.message.lower()
            or "does not exist" in result.message.lower()
        )

    def test_concurrent_connections_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test concurrent connections with resilience patterns."""
        results = {}

        def test_concurrent_connection(thread_id):
            try:
                connector = PostgresConnector()
                connector.configure(docker_postgres_config)

                # Test connection
                result = connector.test_connection()
                results[f"{thread_id}_connection"] = result.success

                # Test discovery
                discovered = connector.discover()
                results[f"{thread_id}_discovery"] = len(discovered) > 0

                # Test reading
                chunks = list(connector.read("resilience_test_table"))
                results[f"{thread_id}_read"] = (
                    len(chunks) > 0 and len(chunks[0].pandas_df) == 5
                )

            except Exception as e:
                results[f"{thread_id}_error"] = str(e)

        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=test_concurrent_connection, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify all succeeded
        for i in range(5):
            assert (
                results.get(f"{i}_connection") is True
            ), f"Thread {i} connection failed"
            assert results.get(f"{i}_discovery") is True, f"Thread {i} discovery failed"
            assert results.get(f"{i}_read") is True, f"Thread {i} read failed"

    def test_schema_operations_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test schema operations with resilience patterns."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Test get_schema for existing table
        schema = connector.get_schema("resilience_test_table")
        assert schema is not None

        # Verify schema contains expected columns
        field_names = [field.name for field in schema.arrow_schema]
        assert "id" in field_names
        assert "name" in field_names
        assert "value" in field_names
        assert "created_at" in field_names

    def test_query_resilience_patterns(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test query execution with resilience patterns."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Test reading with filters
        chunks = list(
            connector.read("resilience_test_table", filters={"value": ">= 300"})
        )

        assert len(chunks) > 0
        df = chunks[0].pandas_df

        # Should only get records with value >= 300
        assert len(df) == 3  # records 3, 4, 5
        assert all(value >= 300 for value in df["value"])

    def test_connection_pooling_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test connection pooling behavior with resilience."""
        # Create multiple connectors with same config
        connectors = []
        for i in range(3):
            connector = PostgresConnector()
            connector.configure(docker_postgres_config)
            connectors.append(connector)

        # Test that all can connect
        for i, connector in enumerate(connectors):
            result = connector.test_connection()
            assert result.success is True, f"Connector {i} failed to connect"

        # Test that all can read data simultaneously
        for i, connector in enumerate(connectors):
            chunks = list(connector.read("resilience_test_table"))
            assert len(chunks) > 0, f"Connector {i} failed to read data"
            assert len(chunks[0].pandas_df) == 5, f"Connector {i} got wrong data count"

    def test_large_result_set_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test resilience with larger result sets."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Create a temporary table with more data
        try:
            conn = psycopg2.connect(
                host=docker_postgres_config["host"],
                port=docker_postgres_config["port"],
                database=docker_postgres_config["database"],
                user=docker_postgres_config["username"],
                password=docker_postgres_config["password"],
            )
            cursor = conn.cursor()

            # Create table with more data
            cursor.execute("DROP TABLE IF EXISTS large_test_table")
            cursor.execute(
                """
                CREATE TABLE large_test_table (
                    id SERIAL PRIMARY KEY,
                    data VARCHAR(100),
                    number INTEGER
                )
            """
            )

            # Insert 1000 rows
            for i in range(1000):
                cursor.execute(
                    "INSERT INTO large_test_table (data, number) VALUES (%s, %s)",
                    (f"data_{i}", i),
                )

            conn.commit()
            conn.close()

            # Test reading large dataset with resilience
            chunks = list(connector.read("large_test_table"))
            total_rows = sum(len(chunk.pandas_df) for chunk in chunks)
            assert total_rows == 1000, f"Expected 1000 rows, got {total_rows}"

        finally:
            # Cleanup
            try:
                conn = psycopg2.connect(
                    host=docker_postgres_config["host"],
                    port=docker_postgres_config["port"],
                    database=docker_postgres_config["database"],
                    user=docker_postgres_config["username"],
                    password=docker_postgres_config["password"],
                )
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS large_test_table")
                conn.commit()
                conn.close()
            except Exception:
                pass

    def test_transaction_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test transaction handling with resilience patterns."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Verify initial data count
        chunks = list(connector.read("resilience_test_table"))
        initial_count = len(chunks[0].pandas_df)
        assert initial_count == 5

        # Test that connector handles read-only operations gracefully
        # (PostgreSQL connector typically uses read-only connections)
        for i in range(3):
            chunks = list(connector.read("resilience_test_table"))
            assert len(chunks[0].pandas_df) == initial_count

    def test_connection_timeout_resilience(self, setup_postgres_test_environment):
        """Test connection timeout behavior with resilience."""
        connector = PostgresConnector()

        # Test with unreachable host (should timeout)
        timeout_config = {
            "host": "192.0.2.1",  # RFC 5737 test address (should be unreachable)
            "port": 5432,
            "database": "postgres",
            "username": "postgres",
            "password": "postgres",
            "connect_timeout": 1,  # Short timeout
        }

        connector.configure(timeout_config)

        # Should fail with timeout after retries
        start_time = time.time()
        with pytest.raises(psycopg2.OperationalError) as exc_info:
            connector.test_connection()
        end_time = time.time()

        # Verify the error message indicates timeout
        error_msg = str(exc_info.value).lower()
        assert "timeout" in error_msg
        # Should timeout relatively quickly (with some tolerance for retries and system variance)
        assert (
            end_time - start_time < 15
        ), "Connection should timeout within reasonable time including retries"

    def test_sql_injection_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test resilience against SQL injection attempts."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Test with malicious table name
        malicious_table = "resilience_test_table'; DROP TABLE users; --"

        # Should handle gracefully (not execute injection)
        try:
            chunks = list(connector.read(malicious_table))
            # If it doesn't raise an exception, it should return empty results
            assert len(chunks) == 0 or len(chunks[0].pandas_df) == 0
        except Exception as e:
            # Should fail safely with an appropriate error
            assert "syntax error" in str(e).lower() or "relation" in str(e).lower()

        # Verify original tables still exist
        discovered = connector.discover()
        table_names = [table.lower() for table in discovered]
        assert "users" in table_names, "users table should still exist"

    def test_memory_management_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test memory management with resilience patterns."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Read data multiple times to test memory management
        for iteration in range(10):
            chunks = list(connector.read("resilience_test_table"))
            assert len(chunks) > 0
            assert len(chunks[0].pandas_df) == 5

            # Verify data is still correct after multiple iterations
            df = chunks[0].pandas_df
            assert df["value"].tolist() == [100, 200, 300, 400, 500]

    def test_connection_recovery_after_restart(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test connection recovery behavior (simulated restart scenario)."""
        connector = PostgresConnector()
        connector.configure(docker_postgres_config)

        # Initial connection should work
        result = connector.test_connection()
        assert result.success is True

        # Read initial data
        chunks = list(connector.read("resilience_test_table"))
        assert len(chunks) > 0

        # Simulate connection recovery by creating new connector instance
        # (In real scenarios, this would be after service restart)
        new_connector = PostgresConnector()
        new_connector.configure(docker_postgres_config)

        # Should be able to connect and read data again
        result = new_connector.test_connection()
        assert result.success is True

        chunks = list(new_connector.read("resilience_test_table"))
        assert len(chunks) > 0
        assert len(chunks[0].pandas_df) == 5


if __name__ == "__main__":
    pytest.main([__file__])
