import unittest

import pandas as pd
import requests_mock

from sqlflow.connectors.rest.source import RestApiSource


class TestRestApiSource(unittest.TestCase):
    def test_read_success(self):
        """Test successful read from a REST API."""
        url = "http://test.com/api/data"
        with requests_mock.Mocker() as m:
            m.get(url, json=[{"a": 1, "b": 2}])
            connector = RestApiSource(config={"url": url})
            data_frame = connector.read()
            self.assertIsInstance(data_frame, pd.DataFrame)
            self.assertEqual(len(data_frame), 1)

    def test_read_with_basic_auth(self):
        """Test read with basic authentication."""
        url = "http://test.com/api/data"
        with requests_mock.Mocker() as m:
            m.get(url, json=[{"a": 1, "b": 2}])
            config = {
                "url": url,
                "auth": {"type": "basic", "username": "user", "password": "pass"},
            }
            connector = RestApiSource(config=config)
            connector.read()
            self.assertEqual(
                m.last_request.headers["Authorization"], "Basic dXNlcjpwYXNz"
            )

    def test_read_with_bearer_token(self):
        """Test read with bearer token authentication."""
        url = "http://test.com/api/data"
        with requests_mock.Mocker() as m:
            m.get(url, json=[{"a": 1, "b": 2}])
            config = {"url": url, "auth": {"type": "bearer", "token": "my-token"}}
            connector = RestApiSource(config=config)
            connector.read()
            self.assertEqual(m.last_request.headers["Authorization"], "Bearer my-token")

    def test_missing_url_config(self):
        """Test that an error is raised if 'url' is not in config."""
        with self.assertRaises(ValueError):
            RestApiSource(config={})


if __name__ == "__main__":
    unittest.main()
