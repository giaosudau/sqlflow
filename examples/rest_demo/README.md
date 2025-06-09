# REST API Connector Demo

This demo showcases the SQLFlow REST API connector capabilities using the public JSONPlaceholder API.

## Overview

The REST API connector enables SQLFlow to consume data from HTTP/REST endpoints with features including:

- **Multiple authentication methods** (Basic, Digest, Bearer, API Key)
- **Pagination support** for large datasets
- **Flexible data extraction** with JSONPath-like syntax
- **Schema inference** from JSON responses
- **Retry logic** with exponential backoff

## Demo Structure

```
rest_demo/
├── pipelines/              # SQL pipeline definitions
│   ├── fetch_users.sf     # Basic user data loading
│   └── analyze_posts.sf   # Advanced posts analysis
├── profiles/              # Environment configurations
│   └── dev.yml           # Development profile
├── output/               # Generated results
├── run_demo.sh          # Main demo script
└── README.md           # This documentation
```

## What This Demo Shows

### 1. Basic REST API Integration (`fetch_users.sf`)
- **Simple GET requests** to public APIs
- **JSON data extraction** using DuckDB's JSON functions
- **Data transformation** and cleaning
- **Summary statistics** generation

### 2. Advanced Data Analysis (`analyze_posts.sf`)
- **Multiple API endpoints** in one pipeline
- **Data enrichment** by joining API responses
- **Content analysis** with text metrics
- **Statistical aggregations** and categorization

## Running the Demo

### Prerequisites
- SQLFlow installed and configured
- Internet connection (uses public JSONPlaceholder API)
- No authentication required (public API)

### Quick Start

```bash
# Navigate to the demo directory
cd examples/rest_demo

# Run the complete demo
./run_demo.sh
```

### Manual Execution

```bash
# Run individual pipelines
sqlflow pipeline run fetch_users --profile dev
sqlflow pipeline run analyze_posts --profile dev

# Query results
sqlflow debug query "SELECT * FROM user_summary" --profile dev
sqlflow debug query "SELECT * FROM author_stats LIMIT 5" --profile dev
```

## Expected Results

### User Summary
- Total users count
- Unique cities and companies
- Users with websites/phones

### Author Statistics
- Posts per author
- Average content metrics
- Word count analysis

### Content Insights
- Post categorization by length
- Content distribution analysis
- Author diversity metrics

## API Endpoints Used

This demo uses the free JSONPlaceholder API:

- **Users**: `https://jsonplaceholder.typicode.com/users`
- **Posts**: `https://jsonplaceholder.typicode.com/posts`

No API key or authentication required.

## Key Features Demonstrated

### 1. Schema Inference
The connector automatically infers schema from JSON responses:
```sql
SOURCE users TYPE REST PARAMS {
    "url": "https://jsonplaceholder.typicode.com/users"
};
```

### 2. Query Parameters
Add parameters to API requests:
```sql
SOURCE posts TYPE REST PARAMS {
    "url": "https://jsonplaceholder.typicode.com/posts",
    "params": {
        "_limit": 50
    }
};
```

### 3. JSON Data Extraction
Use DuckDB's JSON functions to extract nested data:
```sql
JSON_EXTRACT_STRING(address, '$.city') as city,
JSON_EXTRACT_STRING(company, '$.name') as company_name
```

### 4. Data Enrichment
Join data from multiple API endpoints:
```sql
FROM posts p
LEFT JOIN users u ON p.userId = u.id
```

## Extending the Demo

### Add Authentication
```sql
SOURCE secure_api TYPE REST PARAMS {
    "url": "https://api.example.com/data",
    "auth": {
        "type": "bearer",
        "token": "your-token-here"
    }
};
```

### Add Pagination
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

### Extract Nested Data
```sql
SOURCE nested_api TYPE REST PARAMS {
    "url": "https://api.example.com/data",
    "data_path": "results.items"
};
```

## Troubleshooting

### Connection Issues
- Verify internet connectivity
- Check if JSONPlaceholder API is accessible
- Review firewall/proxy settings

### Pipeline Errors
- Check SQL syntax in pipeline files
- Verify JSON extraction paths
- Review DuckDB error messages

### Performance Tips
- Use `_limit` parameter for large datasets
- Consider pagination for production APIs
- Monitor API rate limits

## Real-World Applications

This demo pattern can be adapted for:

- **Social Media APIs** (Twitter, Facebook, LinkedIn)
- **E-commerce APIs** (Shopify, WooCommerce, Stripe)
- **CRM APIs** (Salesforce, HubSpot, Pipedrive)
- **Analytics APIs** (Google Analytics, Mixpanel)
- **Financial APIs** (Alpha Vantage, Yahoo Finance)

## Security Considerations

- Store API credentials in environment variables
- Use HTTPS endpoints only
- Implement proper token rotation
- Monitor API usage and costs
- Validate SSL certificates

## Performance Optimization

- Use appropriate timeouts
- Configure retry settings
- Implement pagination for large datasets
- Cache frequently accessed data
- Monitor API rate limits 