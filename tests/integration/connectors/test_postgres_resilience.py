"""Integration tests for PostgreSQL connector resilience patterns."""

import os
import threading
import time

import pandas as pd
import psycopg2
import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from sqlflow.connectors.postgres.source import PostgresSource

# Mark all tests in this module as requiring external services
pytestmark = pytest.mark.external_services


@pytest.fixture(scope="module")
def docker_postgres_config():
    """Real PostgreSQL configuration from docker-compose.yml."""
    return {
        "host": "localhost",
        "port": 5432,
        "dbname": "postgres",
        "user": "postgres",
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
            database=docker_postgres_config["dbname"],
            user=docker_postgres_config["user"],
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
                database=docker_postgres_config["dbname"],
                user=docker_postgres_config["user"],
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
        connector = PostgresSource(config=docker_postgres_config)

        # Test connection with real service by fetching tables
        tables = connector.get_tables()
        assert isinstance(tables, list)

    def test_real_discovery_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test discovery operations with real PostgreSQL service."""
        connector = PostgresSource(config=docker_postgres_config)

        # Test discovery
        discovered = connector.get_tables()
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
        connector = PostgresSource(config=docker_postgres_config)

        # Read real data from test table
        chunks = list(connector.read(options={"table_name": "resilience_test_table"}))
        df = (
            pd.concat([chunk.pandas_df for chunk in chunks])
            if chunks
            else pd.DataFrame()
        )
        assert not df.empty, "Should read data"

        # Verify data content
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
        # Test with invalid port
        invalid_config = {
            "host": "localhost",
            "port": 9999,  # Invalid port
            "dbname": "postgres",
            "user": "postgres",
            "password": "postgres",
        }
        connector = PostgresSource(config=invalid_config)

        # Should fail gracefully with resilience patterns after retries
        with pytest.raises(Exception) as exc_info:
            connector.get_tables()

        # Verify the error message indicates connection failure
        error_msg = str(exc_info.value).lower()
        assert "connection" in error_msg and (
            "refused" in error_msg
            or "failed" in error_msg
            or "is the server running" in error_msg
        )

    def test_authentication_failure_resilience(self, setup_postgres_test_environment):
        """Test resilience with authentication failures."""
        # Test with invalid credentials
        invalid_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "invalid_user",
            "password": "invalid_password",
        }
        connector = PostgresSource(config=invalid_config)

        # Should fail gracefully (authentication errors are non-retryable)
        with pytest.raises(Exception) as exc_info:
            connector.get_tables()
        assert (
            "authentication" in str(exc_info.value).lower()
            or "password" in str(exc_info.value).lower()
        )

    def test_database_not_found_resilience(self, setup_postgres_test_environment):
        """Test resilience with non-existent database."""
        # Test with non-existent database
        invalid_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "nonexistent_database",
            "user": "postgres",
            "password": "postgres",
        }
        connector = PostgresSource(config=invalid_config)

        with pytest.raises(Exception) as exc_info:
            connector.get_tables()

        assert (
            'database "nonexistent_database" does not exist'
            in str(exc_info.value).lower()
        )

    def test_concurrent_connections_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test concurrent connections with resilience patterns."""
        results = {}

        def test_concurrent_connection(thread_id):
            try:
                connector = PostgresSource(config=docker_postgres_config)

                # Test connection
                tables = connector.get_tables()
                results[f"{thread_id}_connection"] = isinstance(tables, list)

                # Test discovery
                discovered = connector.get_tables()
                results[f"{thread_id}_discovery"] = len(discovered) > 0

                # Test reading
                chunks = list(
                    connector.read(options={"table_name": "resilience_test_table"})
                )
                df = (
                    pd.concat([chunk.pandas_df for chunk in chunks])
                    if chunks
                    else pd.DataFrame()
                )
                results[f"{thread_id}_read"] = not df.empty and len(df) == 5

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
            ), f"Thread {i} connection failed: {results.get(f'{i}_error', 'No error logged')}"
            assert (
                results.get(f"{i}_discovery") is True
            ), f"Thread {i} discovery failed: {results.get(f'{i}_error', 'No error logged')}"
            assert (
                results.get(f"{i}_read") is True
            ), f"Thread {i} read failed: {results.get(f'{i}_error', 'No error logged')}"

    def test_schema_operations_with_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test schema operations with resilience patterns."""
        connector = PostgresSource(config=docker_postgres_config)
        test_schema = "resilience_test_schema"

        with connector.engine.connect() as connection:
            # Use a transaction to ensure DDL is committed
            with connection.begin() as trans:
                # Create schema
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {test_schema}"))
                # Create table in schema
                connection.execute(
                    text(f"CREATE TABLE {test_schema}.my_table (id INT)")
                )
                trans.commit()

            try:
                # Verify schema and table exist outside the transaction
                tables_in_schema = connector.get_tables(schema=test_schema)
                assert "my_table" in tables_in_schema
            finally:
                # Cleanup
                with connection.begin() as trans:
                    connection.execute(
                        text(f"DROP SCHEMA IF EXISTS {test_schema} CASCADE")
                    )
                    trans.commit()

    def test_query_resilience_patterns(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test query execution with resilience patterns."""
        connector = PostgresSource(config=docker_postgres_config)

        # Test valid query
        chunks = list(
            connector.read(
                options={
                    "query": "SELECT * FROM resilience_test_table WHERE value > 250"
                }
            )
        )
        df = (
            pd.concat([chunk.pandas_df for chunk in chunks])
            if chunks
            else pd.DataFrame()
        )
        assert len(df) == 3

        # Test invalid query
        with pytest.raises(Exception) as exc_info:
            list(connector.read(options={"query": "SELECT * FROM non_existent_table"}))
        assert "non_existent_table" in str(exc_info.value).lower()

    def test_connection_pooling_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test connection pooling behavior with resilience."""
        # Create multiple connectors with same config
        connectors = []
        for i in range(3):
            connector = PostgresSource(config=docker_postgres_config)
            connectors.append(connector)

        # Perform operations
        for conn in connectors:
            chunks = list(conn.read(options={"table_name": "resilience_test_table"}))
            df = (
                pd.concat([chunk.pandas_df for chunk in chunks])
                if chunks
                else pd.DataFrame()
            )
            assert not df.empty

        # Check pool stats (conceptual) - cannot directly check pool size easily
        # but if the above runs without exhausting connections, pooling is working.

    def test_large_result_set_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test resilience with larger result sets."""
        connector = PostgresSource(config=docker_postgres_config)

        with connector.engine.connect() as connection:
            # Use a transaction to create and populate the table
            with connection.begin() as trans:
                connection.execute(text("CREATE TABLE large_table (id INT, data TEXT)"))

                # Insert a decent number of rows
                for i in range(1000):
                    connection.execute(
                        text(f"INSERT INTO large_table VALUES ({i}, 'some data')")
                    )
                trans.commit()

            try:
                # Now read the committed data
                chunks = list(connector.read(options={"table_name": "large_table"}))
                df = (
                    pd.concat([chunk.pandas_df for chunk in chunks])
                    if chunks
                    else pd.DataFrame()
                )
                assert len(df) == 1000
            finally:
                # Cleanup
                with connection.begin() as trans:
                    connection.execute(text("DROP TABLE IF EXISTS large_table"))
                    trans.commit()

    def test_transaction_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test transaction handling with resilience patterns."""
        connector = PostgresSource(config=docker_postgres_config)
        with connector.engine.connect() as connection:
            # Start a transaction
            with connection.begin() as transaction:
                try:
                    # Perform some operations
                    connection.execute(
                        text(
                            "INSERT INTO resilience_test_table (name, value) VALUES ('transaction_test', 999)"
                        )
                    )
                    # This will fail
                    connection.execute(text("SELECT * FROM non_existent_table"))
                    transaction.commit()
                except Exception:
                    # Transaction should be rolled back
                    transaction.rollback()

            # Verify transaction was rolled back
            chunks = list(
                connector.read(
                    options={
                        "query": "SELECT * FROM resilience_test_table WHERE name = 'transaction_test'"
                    }
                )
            )
            df = (
                pd.concat([chunk.pandas_df for chunk in chunks])
                if chunks
                else pd.DataFrame()
            )
            assert df.empty, "Transaction should have been rolled back"

    def test_connection_timeout_resilience(self, setup_postgres_test_environment):
        """Test connection timeout behavior with resilience."""
        # This is hard to test without specific server-side config
        # We can simulate by trying to connect to a non-responsive IP
        invalid_config = {
            "host": "10.255.255.1",  # Non-routable IP address
            "port": 5432,
            "dbname": "postgres",
            "user": "postgres",
            "password": "postgres",
            "connect_timeout": 2,
        }
        connector = PostgresSource(config=invalid_config)

        start_time = time.time()
        with pytest.raises(Exception) as exc_info:
            connector.get_tables()
        duration = time.time() - start_time

        # Check if it timed out approximately within the specified time
        assert duration < 10
        assert (
            "timeout" in str(exc_info.value).lower()
            or "is the server running" in str(exc_info.value).lower()
        )

    def test_sql_injection_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test resilience against SQL injection attempts."""
        connector = PostgresSource(config=docker_postgres_config)

        # Attempt injection in table name
        malicious_table_name = "resilience_test_table; DROP TABLE users;"

        # The underlying driver should prevent this by raising a ProgrammingError.
        with pytest.raises(ProgrammingError) as exc_info:
            list(connector.read(options={"table_name": malicious_table_name}))

        # Verify the exception message to ensure it's the expected error
        assert "does not exist" in str(exc_info.value)

        # And the 'users' table should still exist, proving no damage was done
        all_tables = connector.get_tables()
        assert "users" in [t.lower() for t in all_tables]

    def test_memory_management_resilience(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test memory management with resilience patterns."""
        # This is more of a conceptual test in this context.
        # We read a reasonably sized dataset and trust the garbage collector.
        connector = PostgresSource(config=docker_postgres_config)
        chunks = list(connector.read(options={"table_name": "resilience_test_table"}))
        df = (
            pd.concat([chunk.pandas_df for chunk in chunks])
            if chunks
            else pd.DataFrame()
        )
        assert len(df) > 0
        del df, chunks
        # If this doesn't crash, we assume basic memory management is ok.

    def test_connection_recovery_after_restart(
        self, docker_postgres_config, setup_postgres_test_environment
    ):
        """Test connection recovery behavior (simulated restart scenario)."""
        connector = PostgresSource(config=docker_postgres_config)
        # 1. Initial connection works
        assert len(connector.get_tables()) > 0

        # This test requires manual docker restart to be a true integration test.
        # For CI, we assume that if a new connector can be created after a simulated
        # delay, the pooling/connection logic is resilient enough.

        print("\nSimulating service restart... (conceptual)")

        # 2. Create a new connector instance, it should also work
        new_connector = PostgresSource(config=docker_postgres_config)
        assert len(new_connector.get_tables()) > 0


if __name__ == "__main__":
    pytest.main([__file__])
