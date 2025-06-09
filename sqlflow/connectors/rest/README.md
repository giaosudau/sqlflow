# REST API Connector

The REST API connector provides a powerful and flexible way to connect to virtually any HTTP-based API, extract data, and use it as a source in your SQLFlow pipelines.

It is designed to handle a wide variety of APIs, from simple public endpoints to complex, authenticated, and paginated enterprise APIs.

## Overview

This connector allows you to:
- Connect to any REST/HTTP API endpoint using GET or POST methods.
- Handle various authentication schemes (Basic, Bearer Token, API Key).
- Automatically handle paginated responses to retrieve complete datasets.
- Extract data from nested JSON responses using a simple path syntax.
- Configure retries, timeouts, and custom headers for robust integration.

This connector provides **source (read) capabilities only**.

## 📖 Documentation

For detailed information on all configuration options, authentication methods, and advanced features, please see the full source documentation.

**[REST API Source Documentation →](./SOURCE.md)**

## Configuration

### Basic Usage

```sql
-- Simple GET request
SOURCE api_data TYPE REST PARAMS {
  "url": "https://api.example.com/data"
};

-- With query parameters
SOURCE filtered_data TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "params": {
    "status": "active",
    "limit": 1000
  }
};
```

### Authentication

#### Basic Authentication
```sql
SOURCE secure_api TYPE REST PARAMS {
  "url": "https://api.example.com/protected",
  "auth": {
    "type": "basic",
    "username": "myuser",
    "password": "mypass"
  }
};
```

#### Bearer Token
```sql
SOURCE api_with_token TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "auth": {
    "type": "bearer",
    "token": "your-jwt-token-here"
  }
};
```

#### API Key
```sql
SOURCE api_key_auth TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "auth": {
    "type": "api_key",
    "key_name": "X-API-Key",
    "key_value": "your-api-key"
  }
};
```

#### Digest Authentication
```sql
SOURCE digest_auth TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "auth": {
    "type": "digest",
    "username": "user",
    "password": "pass"
  }
};
```

### Pagination

#### Page-based pagination
```sql
SOURCE paginated_api TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "pagination": {
    "page_param": "page",
    "size_param": "per_page",
    "page_size": 100
  }
};
```

#### Cursor-based pagination
```sql
SOURCE cursor_api TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "pagination": {
    "cursor_param": "after",
    "size_param": "limit",
    "page_size": 50
  }
};
```

### Data Extraction

#### Extract nested data
```sql
-- For response: {"results": {"users": [...]}}
SOURCE nested_data TYPE REST PARAMS {
  "url": "https://api.example.com/users",
  "data_path": "results.users"
};
```

#### Control response flattening
```sql
SOURCE structured_data TYPE REST PARAMS {
  "url": "https://api.example.com/complex",
  "flatten_response": false,  -- Keep nested structures
  "data_path": "data"
};
```

### Advanced Configuration

#### Custom headers and timeouts
```sql
SOURCE advanced_api TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "headers": {
    "User-Agent": "MyApp/1.0",
    "Accept": "application/json",
    "Custom-Header": "value"
  },
  "timeout": 60,
  "max_retries": 5,
  "retry_delay": 2.0
};
```

#### POST requests with data
```sql
SOURCE post_api TYPE REST PARAMS {
  "url": "https://api.example.com/search",
  "method": "POST",
  "params": {
    "query": "search term",
    "filters": ["active", "verified"]
  }
};
```

## Common API Patterns

### GitHub API
```sql
SOURCE github_repos TYPE REST PARAMS {
  "url": "https://api.github.com/user/repos",
  "auth": {
    "type": "bearer",
    "token": "ghp_your_token"
  },
  "params": {
    "type": "owner",
    "sort": "updated"
  },
  "pagination": {
    "page_param": "page",
    "size_param": "per_page",
    "page_size": 100
  }
};
```

### Twitter API v2
```sql
SOURCE twitter_tweets TYPE REST PARAMS {
  "url": "https://api.twitter.com/2/tweets/search/recent",
  "auth": {
    "type": "bearer",
    "token": "your_bearer_token"
  },
  "params": {
    "query": "SQLFlow OR data pipeline",
    "max_results": 100
  },
  "data_path": "data"
};
```

### Shopify API
```sql
SOURCE shopify_orders TYPE REST PARAMS {
  "url": "https://your-shop.myshopify.com/admin/api/2023-01/orders.json",
  "auth": {
    "type": "api_key",
    "key_name": "X-Shopify-Access-Token",
    "key_value": "your_access_token"
  },
  "data_path": "orders",
  "pagination": {
    "cursor_param": "since_id",
    "size_param": "limit",
    "page_size": 250
  }
};
```

### Slack API
```sql
SOURCE slack_messages TYPE REST PARAMS {
  "url": "https://slack.com/api/conversations.history",
  "auth": {
    "type": "bearer",
    "token": "xoxb-your-bot-token"
  },
  "params": {
    "channel": "C1234567890",
    "limit": 200
  },
  "data_path": "messages"
};
```

## 📈 Incremental Loading

This connector supports incremental loading, allowing you to process only new data since the last pipeline run. It's designed to be efficient by attempting to fetch only new records from the API.

### Configuration

To enable incremental loading, you need to specify the `sync_mode` and `cursor_field` in your source configuration.

- `sync_mode`: Set to `"incremental"`.
- `cursor_field`: The field in your API response that will be used to determine new records (e.g., `updated_at`, `id`).

```yaml
# profiles/dev.yml
sources:
  api_events:
    type: rest
    url: "https://api.example.com/events"
    sync_mode: "incremental"
    cursor_field: "timestamp"
    # Optional: If your API supports sorting, you can provide the parameter name
    params:
      sort_by: "timestamp" 
      order: "asc"
```

### Behavior

When a pipeline runs in incremental mode:
1.  SQLFlow retrieves the last saved maximum value (watermark) for the `cursor_field`.
2.  The connector adds a parameter to the API request to filter for records where the `cursor_field` is greater than the watermark. It typically uses the `cursor_field` name as the parameter name (e.g., `&timestamp=...`).
3.  For APIs that support sorting, the connector will attempt to order results to more efficiently find the latest records.
4.  After a successful pipeline run, SQLFlow updates the watermark with the new maximum value from the processed data.

**Note**: The effectiveness of incremental loading depends on the API's capabilities (i.e., whether it supports filtering by the cursor field).

## Error Handling

The connector implements a retry mechanism with exponential backoff for transient network errors and common HTTP status codes (5xx).

---

**Version**: 1.0 • **Status**: ✅ Production Ready • **Incremental**: ✅ Supported

## Response Format Support

- **JSON arrays**: `[{"id": 1}, {"id": 2}]`
- **JSON objects**: `{"data": [...]}`  
- **Nested structures**: Use `data_path` to extract
- **Scalar values**: Wrapped in `{"value": ...}`

## Performance Tips

1. **Use pagination** for large datasets to avoid timeouts
2. **Set appropriate timeouts** based on API response times
3. **Limit column selection** when possible
4. **Use incremental loading** for frequently updated data
5. **Configure retry settings** based on API reliability

## Troubleshooting

### Common Issues

**Connection timeout**:
- Increase `timeout` parameter
- Check network connectivity
- Verify API endpoint is accessible

**Authentication failed**:
- Verify credentials are correct
- Check token expiration
- Ensure proper auth type is specified

**Empty response**:
- Check `data_path` configuration
- Verify API returns expected format
- Test endpoint manually with curl

**Schema inference fails**:
- Ensure response contains data
- Check for empty arrays/objects
- Verify JSON structure

### Debug Configuration

```sql
-- Test connection with minimal data
SOURCE debug_api TYPE REST PARAMS {
  "url": "https://api.example.com/data",
  "params": {"limit": 1},  -- Minimal response
  "timeout": 10,           -- Quick timeout
  "max_retries": 1         -- Fast failure
};
```

## Security Considerations

- Store credentials in environment variables
- Use secure authentication methods (Bearer tokens over Basic auth)
- Implement proper token rotation
- Monitor API usage and rate limits
- Use HTTPS endpoints only
- Validate SSL certificates (default behavior)

## API Rate Limiting

Most APIs have rate limits. The connector includes:
- Automatic retry with exponential backoff
- Configurable retry delays
- Request timeout settings

Monitor your API usage and adjust `retry_delay` and `max_retries` accordingly.

## Examples

See `examples/rest_demo/` for complete working examples including:
- Public API integration (JSONPlaceholder)
- Authentication patterns
- Pagination handling
- Error scenarios
- Performance optimization 