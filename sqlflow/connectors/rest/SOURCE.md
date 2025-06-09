# REST API Source

The REST API Source connects to HTTP endpoints to retrieve data. For a general overview, see the [main README](./README.md).

## Configuration

The source is configured in your `profiles.yml` file.

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | `string` | Must be `"rest"`. |
| `url` | `string` | The full URL of the API endpoint to request. |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `method` | `string` | `"GET"` | The HTTP method to use (e.g., `"GET"`, `"POST"`). |
| `params` | `dict` | `None` | A dictionary of query parameters to send with the request. |
| `headers`| `dict` | `None` | A dictionary of custom HTTP headers to send. |
| `auth` | `dict` | `None` | Authentication configuration object. See [Authentication](#authentication). |
| `pagination`| `dict`| `None` | Pagination configuration object. See [Pagination](#pagination). |
| `data_path`| `string`| `None` | Path to the list of records in a nested JSON response. See [Data Extraction](#data-extraction). |
| `flatten_response`|`boolean`|`true`| Whether to flatten the structure of nested JSON objects. |
| `timeout`| `integer`| `30` | Request timeout in seconds. |
| `max_retries`| `integer`| `3` | Maximum number of retries on failed requests. |
| `retry_delay`|`float`|`1.0`| Delay in seconds between retries (uses exponential backoff). |

### Example Profile Configuration
```yaml
# profiles/dev.yml
sources:
  my_api:
    type: "rest"
    url: "https://api.example.com/data"
    params:
      status: "active"
      limit: 1000
    headers:
      User-Agent: "SQLFlow/1.0"
    auth:
      type: "bearer"
      token: "your-jwt-token-here"
```

## Authentication
The connector supports multiple authentication methods via the `auth` object.

#### Basic Authentication
- `type`: `"basic"`
- `username`: The username.
- `password`: The password.

#### Bearer Token
- `type`: `"bearer"`
- `token`: The bearer token.

#### API Key
- `type`: `"api_key"`
- `key_name`: The name of the header or query parameter for the key.
- `key_value`: The API key value.
- `add_to` (optional, default `"header"`): Where to add the key (`"header"` or `"query"`).

#### Digest Authentication
- `type`: `"digest"`
- `username`: The username.
- `password`: The password.

## Pagination
The connector can automatically handle paginated APIs.

#### Page-based Pagination
Used for APIs that paginate using a page number and page size.
- `page_param`: The name of the page number parameter (e.g., `"page"`).
- `size_param`: The name of the page size parameter (e.g., `"per_page"`).
- `page_size`: The number of records to request per page.

#### Cursor-based Pagination
Used for APIs that return a cursor or token for the next page of results.
- `cursor_param`: The name of the cursor parameter (e.g., `"after"` or `"next_token"`).
- `cursor_path`: The JSON path to find the *next* cursor value in the response body.
- `size_param`: The name of the page size parameter.
- `page_size`: The number of records per page.

## Data Extraction
Use `data_path` to specify the location of the list of records if it's nested within the JSON response.

```yaml
# For response: {"results": {"users": [...]}}
data_path: "results.users"
```

Use `flatten_response` to control whether nested JSON objects within your records are flattened into columns with `_` separators.

## 📈 Incremental Loading
This connector supports incremental loading to process only new data since the last run.

### Configuration
- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: The field in your API response records used to determine new data (e.g., `updated_at`, `id`).
- `incremental_param` (optional): The name of the URL query parameter to which the last cursor value should be sent (e.g., `since` or `updated_after`).

```yaml
# profiles/dev.yml
sources:
  api_events:
    type: rest
    url: "https://api.example.com/events"
    sync_mode: "incremental"
    cursor_field: "timestamp"
    incremental_param: "since"
```

### Behavior
1. SQLFlow retrieves the last saved watermark for the `cursor_field`.
2. The connector adds a query parameter (e.g., `?since=<watermark>`) to the API request.
3. The API is expected to return only records newer than the watermark.
4. After a successful run, SQLFlow updates the watermark with the new maximum value.

**Note**: This is most efficient when the API supports a timestamp-based filter. If `incremental_param` is not provided, the connector fetches all data and filters it locally, which is less efficient.

## 🛠️ Common API Examples

### GitHub API
```yaml
github_repos:
  type: rest
  url: "https://api.github.com/user/repos"
  auth:
    type: "bearer"
    token: "ghp_your_token"
  params:
    type: "owner"
    sort: "updated"
  pagination:
    page_param: "page"
    size_param: "per_page"
    page_size: 100
```

### Shopify API Orders
```yaml
shopify_orders:
  type: rest
  url: "https://your-shop.myshopify.com/admin/api/2023-01/orders.json"
  auth:
    type: "api_key"
    key_name: "X-Shopify-Access-Token"
    key_value": "your_access_token"
  data_path: "orders"
  sync_mode: "incremental"
  cursor_field: "id"
  incremental_param: "since_id"
``` 