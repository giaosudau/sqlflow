import json
import unittest

import requests
import requests_mock

from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.rest.source import RestSource


class TestRestSource(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.base_url = "https://api.example.com"
        self.test_data = [
            {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "created_at": "2023-01-01T10:00:00Z",
            },
            {
                "id": 2,
                "name": "Bob",
                "email": "bob@example.com",
                "created_at": "2023-01-02T11:00:00Z",
            },
            {
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "created_at": "2023-01-03T12:00:00Z",
            },
        ]

    def test_configure_basic(self):
        """Test basic configuration."""
        connector = RestSource()
        config = {"url": f"{self.base_url}/data"}

        connector.configure(config)

        self.assertEqual(connector.url, f"{self.base_url}/data")
        self.assertEqual(connector.method, "GET")
        self.assertEqual(connector.timeout, 30)

    def test_configure_advanced(self):
        """Test configuration with advanced options."""
        connector = RestSource()
        config = {
            "url": f"{self.base_url}/data",
            "method": "POST",
            "headers": {"Custom-Header": "value"},
            "params": {"limit": 100},
            "timeout": 60,
            "max_retries": 5,
            "data_path": "results.data",
        }

        connector.configure(config)

        self.assertEqual(connector.method, "POST")
        self.assertEqual(connector.headers["Custom-Header"], "value")
        self.assertEqual(connector.params["limit"], 100)
        self.assertEqual(connector.timeout, 60)
        self.assertEqual(connector.max_retries, 5)
        self.assertEqual(connector.data_path, "results.data")

    def test_configure_missing_url(self):
        """Test that error is raised when URL is missing."""
        connector = RestSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({})

        self.assertIn("url", str(context.exception))

    def test_configure_invalid_url(self):
        """Test that error is raised for invalid URL."""
        connector = RestSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({"url": "not-a-valid-url"})

        self.assertIn("Invalid URL format", str(context.exception))

    def test_configure_invalid_method(self):
        """Test that error is raised for unsupported HTTP method."""
        connector = RestSource()

        with self.assertRaises(ValueError) as context:
            connector.configure({"url": f"{self.base_url}/data", "method": "PATCH"})

        self.assertIn("Unsupported HTTP method", str(context.exception))

    def test_test_connection_success(self):
        """Test successful connection test."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            result = connector.test_connection()

            self.assertTrue(result.success)
            self.assertIn("Successfully connected", result.message)
            self.assertIn("found 3 items", result.message)

    def test_test_connection_not_configured(self):
        """Test connection test when connector is not configured."""
        connector = RestSource()

        result = connector.test_connection()

        self.assertFalse(result.success)
        self.assertIn("not configured", result.message)

    def test_test_connection_http_error(self):
        """Test connection test with HTTP error."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", status_code=404)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            result = connector.test_connection()

            self.assertFalse(result.success)
            self.assertIn("Connection test failed", result.message)

    def test_test_connection_invalid_json(self):
        """Test connection test with invalid JSON response."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", text="not json")

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            result = connector.test_connection()

            self.assertFalse(result.success)
            self.assertIn("Invalid JSON response", result.message)

    def test_discover(self):
        """Test endpoint discovery."""
        connector = RestSource()
        connector.configure({"url": f"{self.base_url}/api/users"})

        objects = connector.discover()

        self.assertEqual(objects, ["users"])

    def test_get_schema(self):
        """Test schema inference."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            schema = connector.get_schema()

            self.assertEqual(len(schema.arrow_schema), 4)
            column_names = [field.name for field in schema.arrow_schema]
            self.assertIn("id", column_names)
            self.assertIn("name", column_names)
            self.assertIn("email", column_names)
            self.assertIn("created_at", column_names)

    def test_get_schema_with_data_path(self):
        """Test schema inference with data path."""
        nested_response = {"results": {"users": self.test_data}}

        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=nested_response)

            connector = RestSource()
            connector.configure(
                {"url": f"{self.base_url}/data", "data_path": "results.users"}
            )

            schema = connector.get_schema()

            self.assertEqual(len(schema.arrow_schema), 4)

    def test_read_simple(self):
        """Test simple data reading."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            chunks = list(connector.read())

            self.assertEqual(len(chunks), 1)
            self.assertIsInstance(chunks[0], DataChunk)
            self.assertEqual(len(chunks[0].pandas_df), 3)

    def test_read_with_column_selection(self):
        """Test reading with column selection."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            chunks = list(connector.read(columns=["id", "name"]))

            self.assertEqual(len(chunks), 1)
            df = chunks[0].pandas_df
            self.assertEqual(list(df.columns), ["id", "name"])

    def test_read_with_data_path(self):
        """Test reading with data path extraction."""
        nested_response = {"status": "success", "data": self.test_data}

        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=nested_response)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data", "data_path": "data"})

            chunks = list(connector.read())

            self.assertEqual(len(chunks), 1)
            self.assertEqual(len(chunks[0].pandas_df), 3)

    def test_read_with_pagination(self):
        """Test reading with pagination."""
        page1_data = self.test_data[:2]
        page2_data = self.test_data[2:]

        with requests_mock.Mocker() as m:
            m.get(
                f"{self.base_url}/data",
                [
                    {"json": page1_data, "status_code": 200},
                    {"json": page2_data, "status_code": 200},
                ],
            )

            connector = RestSource()
            connector.configure(
                {
                    "url": f"{self.base_url}/data",
                    "pagination": {
                        "page_param": "page",
                        "size_param": "limit",
                        "page_size": 2,
                    },
                }
            )

            chunks = list(connector.read())

            self.assertEqual(len(chunks), 2)
            self.assertEqual(len(chunks[0].pandas_df), 2)
            self.assertEqual(len(chunks[1].pandas_df), 1)

    def test_authentication_basic(self):
        """Test basic authentication."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure(
                {
                    "url": f"{self.base_url}/data",
                    "auth": {"type": "basic", "username": "user", "password": "pass"},
                }
            )

            list(connector.read())

            # Verify authorization header was set
            self.assertEqual(
                m.last_request.headers["Authorization"], "Basic dXNlcjpwYXNz"
            )

    def test_authentication_bearer(self):
        """Test bearer token authentication."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure(
                {
                    "url": f"{self.base_url}/data",
                    "auth": {"type": "bearer", "token": "my-token"},
                }
            )

            list(connector.read())

            self.assertEqual(m.last_request.headers["Authorization"], "Bearer my-token")

    def test_authentication_api_key(self):
        """Test API key authentication."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure(
                {
                    "url": f"{self.base_url}/data",
                    "auth": {
                        "type": "api_key",
                        "key_name": "X-API-Key",
                        "key_value": "secret-key",
                    },
                }
            )

            list(connector.read())

            self.assertEqual(m.last_request.headers["X-API-Key"], "secret-key")

    def test_get_cursor_value(self):
        """Test cursor value extraction."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", json=self.test_data)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            cursor_value = connector.get_cursor_value("data", "created_at")

            # Should return the first (most recent) created_at value
            self.assertEqual(cursor_value, "2023-01-01T10:00:00Z")

    def test_error_handling_connection_error(self):
        """Test handling of connection errors."""
        with requests_mock.Mocker() as m:
            m.get(f"{self.base_url}/data", exc=requests.exceptions.ConnectionError)

            connector = RestSource()
            connector.configure({"url": f"{self.base_url}/data"})

            result = connector.test_connection()

            self.assertFalse(result.success)
            self.assertIn("Connection failed", result.message)

    def test_post_method(self):
        """Test POST method requests."""
        with requests_mock.Mocker() as m:
            m.post(f"{self.base_url}/search", json=self.test_data)

            connector = RestSource()
            connector.configure(
                {
                    "url": f"{self.base_url}/search",
                    "method": "POST",
                    "params": {"query": "test"},
                }
            )

            chunks = list(connector.read())

            self.assertEqual(len(chunks), 1)
            # Verify the request was POST with JSON body
            self.assertEqual(m.last_request.method, "POST")
            self.assertEqual(json.loads(m.last_request.text), {"query": "test"})


if __name__ == "__main__":
    unittest.main()
