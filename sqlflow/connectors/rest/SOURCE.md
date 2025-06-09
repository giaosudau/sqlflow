# REST API Source Connector

Pulls data from a REST API endpoint. This connector is flexible and can be used to integrate with a wide variety of web services that return JSON.

## ✅ Features
- **HTTP Methods**: Supports `GET` and `POST` requests.
- **Authentication**: Includes built-in support for Basic, Bearer Token, Digest, and API Key authentication.
- **Pagination**: Handles a basic page-number-based pagination strategy.
- **JSON Processing**: Automatically parses JSON responses and can flatten nested structures.
- **Resilience**: Built-in support for connection timeouts and retries with exponential backoff.

## 📋 Configuration

| Parameter | Type | Description | Required | Example |
|---|---|---|:---:|---|
| `type` | `string` | Must be `"rest"`. | ✅ | `"rest"` |
| `url` | `string` | The full URL of the API endpoint to call. | ✅ | `"https://api.example.com/v1/data"` |
| `method` | `string` | The HTTP method to use (`GET` or `POST`). Defaults to `GET`. | | `"POST"` |
| `headers` | `dict` | A dictionary of HTTP headers to send with the request. | | `{"Accept": "application/json"}`|
| `params` | `dict` | For `GET`, a dictionary of query parameters. For `POST`, this is the JSON body. | | |
| `auth` | `dict` | An object defining the authentication method. See below. | | |
| `pagination`| `dict` | An object defining the pagination strategy. See below. | | |
| `data_path` | `string`| A dot-separated path to extract records from a nested JSON response. | | `"results.data"`|
| `flatten_response`|`boolean`| Whether to flatten the nested JSON structure of records. | `true` (default)| `false`|
| `timeout` | `integer` | Connection timeout in seconds. | `30` (default) | `60` |
| `max_retries`| `integer` | Number of times to retry a failed request. | `3` (default) | `5` |

### Authentication
**Basic Auth:**
```yaml
auth:
  type: "basic"
  username: "myuser"
  password: "${API_PASSWORD}"
```
**Bearer Token:**
```yaml
auth:
  type: "bearer"
  token: "${API_TOKEN}"
```
**API Key (in header):**
```yaml
auth:
  type: "api_key"
  key_name: "X-Api-Key"
  key_value: "${API_KEY}"
```
**Digest Auth:**
```yaml
auth:
  type: "digest"
  username: "myuser"
  password: "${API_PASSWORD}"
```

### Pagination
The generic REST connector supports a simple page number strategy. More complex strategies (like following `Link` headers) are implemented in specialized connectors like the `Shopify` source.

**Page Number Strategy:**
Sends an incrementing page number and a page size in the query parameters.
```yaml
pagination:
  page_param: "page"      # The name of the query parameter for the page number.
  size_param: "per_page"  # The name of the query parameter for the page size.
  page_size: 100          # The number of records to request per page.
```

### Data Extraction (`data_path`)
The `data_path` parameter extracts a list of records from a nested JSON response. It uses a simple dot-notation syntax, not full JSONPath.

For a response like `{"data": {"items": [...]}}`, the `data_path` would be `"data.items"`.

## 💡 Example
This example fetches paginated user data from an API that requires an API key.
```sql
SOURCE generic_api_users TYPE REST PARAMS {
    "url": "https://api.test.com/users",
    "method": "GET",
    "auth": {
        "type": "api_key",
        "key_name": "X-Custom-Auth-Token",
        "key_value": "${API_TOKEN}"
    },
    "pagination": {
        "page_param": "p",
        "size_param": "limit",
        "page_size": 50
    },
    "data_path": "users"
};
```

---
**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ❌ Not Supported 