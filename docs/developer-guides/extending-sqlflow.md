# Extending SQLFlow: Building Connectors and UDFs

## Why Extend SQLFlow?

Remember why SQLFlow exists - to democratize data pipeline development. When you extend SQLFlow, you're contributing to that mission. You're making it easier for analysts, engineers, and business people to connect to new data sources and add custom logic without complex infrastructure.

The founder's vision was simple: **SQL should be enough**. Your extensions help realize that vision by bringing more data sources and analytical capabilities into the SQL-centric world.

## 🔌 Building Custom Connectors

### The Reality of Data Sources

Every organization has unique data sources. That internal CRM system, the legacy database that "we'll migrate someday," the SaaS tool that only has a REST API. SQLFlow's connector system was designed by someone who lived this reality at App Annie.

### Understanding the Connector Architecture

SQLFlow uses a unified connector interface that makes building new connectors straightforward:

```python
# Source: sqlflow/connectors/base.py:246
from sqlflow.connectors.base import Connector, register_connector
from sqlflow.connectors.data_chunk import DataChunk

@register_connector("MYCRM")  # This registers your connector automatically
class MyCRMConnector(Connector):
    def configure(self, params):
        # Handle connection parameters like API keys, endpoints
        pass
    
    def test_connection(self):
        # Test if you can actually connect
        pass
    
    def read(self, object_name, **kwargs):
        # Read data and yield DataChunk objects
        pass
```

### Real Example: Building a CRM Connector

Let's build a connector for a hypothetical CRM system. This shows the actual process:

```python
# mycrm_connector.py
from typing import Any, Dict, Iterator, List, Optional
import requests
import pandas as pd

from sqlflow.connectors.base import (
    Connector, 
    ConnectionTestResult, 
    Schema,
    ConnectorState,
    ParameterValidator
)
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.connectors.registry import register_connector
from sqlflow.core.errors import ConnectorError

class CRMParameterValidator(ParameterValidator):
    """Validates CRM-specific parameters."""
    
    def _get_required_params(self) -> List[str]:
        return ["api_url", "api_key"]
    
    def _get_optional_params(self) -> Dict[str, Any]:
        base_params = super()._get_optional_params()
        return {
            **base_params,
            "timeout": 30,
            "page_size": 1000
        }

@register_connector("MYCRM")
class MyCRMConnector(Connector):
    """Connector for My CRM System."""
    
    def __init__(self):
        super().__init__()
        self.api_url: Optional[str] = None
        self.api_key: Optional[str] = None
        self.timeout: int = 30
        self.page_size: int = 1000
        self._parameter_validator = CRMParameterValidator("MYCRM")
    
    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with API credentials."""
        try:
            # Use the standardized parameter validation
            validated = self.validate_params(params)
            
            self.api_url = validated["api_url"]
            self.api_key = validated["api_key"] 
            self.timeout = validated["timeout"]
            self.page_size = validated["page_size"]
            
            self.state = ConnectorState.CONFIGURED
            
        except Exception as e:
            self.state = ConnectorState.ERROR
            raise ConnectorError(self.name or "MYCRM", f"Configuration failed: {e}")
    
    def test_connection(self) -> ConnectionTestResult:
        """Test API connectivity."""
        try:
            response = requests.get(
                f"{self.api_url}/health",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                self.state = ConnectorState.READY
                return ConnectionTestResult(True, "Connection successful")
            else:
                return ConnectionTestResult(False, f"API returned {response.status_code}")
                
        except Exception as e:
            return ConnectionTestResult(False, f"Connection failed: {e}")
    
    def discover(self) -> List[str]:
        """Discover available objects (tables) in the CRM."""
        try:
            response = requests.get(
                f"{self.api_url}/objects",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Assume API returns {"objects": ["customers", "deals", "contacts"]}
            return response.json()["objects"]
            
        except Exception as e:
            raise ConnectorError(self.name or "MYCRM", f"Discovery failed: {e}")
    
    def get_schema(self, object_name: str) -> Schema:
        """Get schema for a CRM object."""
        try:
            response = requests.get(
                f"{self.api_url}/objects/{object_name}/schema",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Convert API schema to SQLFlow Schema
            api_schema = response.json()
            # This would map CRM field types to Arrow types
            return Schema.from_dict(api_schema["fields"])
            
        except Exception as e:
            raise ConnectorError(self.name or "MYCRM", f"Schema retrieval failed: {e}")
    
    def read(
        self, 
        object_name: str, 
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000
    ) -> Iterator[DataChunk]:
        """Read data from CRM in chunks."""
        try:
            page = 0
            
            while True:
                # Build API request
                params = {
                    "page": page,
                    "page_size": min(batch_size, self.page_size)
                }
                
                if columns:
                    params["fields"] = ",".join(columns)
                
                if filters:
                    params.update(self._build_filters(filters))
                
                response = requests.get(
                    f"{self.api_url}/objects/{object_name}/data",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                data = response.json()
                
                if not data["records"]:
                    break  # No more data
                
                # Convert to DataFrame and yield
                df = pd.DataFrame(data["records"])
                yield DataChunk.from_pandas(df, schema=None)
                
                # Check if we got less than page_size (last page)
                if len(data["records"]) < params["page_size"]:
                    break
                    
                page += 1
                
        except Exception as e:
            raise ConnectorError(self.name or "MYCRM", f"Read operation failed: {e}")
    
    def _build_filters(self, filters: Dict[str, Any]) -> Dict[str, str]:
        """Convert SQLFlow filters to CRM API format."""
        api_filters = {}
        
        for field, condition in filters.items():
            if isinstance(condition, dict):
                # Handle operators like {">=": "2023-01-01"}
                for op, value in condition.items():
                    api_filters[f"{field}_{op}"] = str(value)
            else:
                # Simple equality
                api_filters[field] = str(condition)
        
        return api_filters
```

### Using Your Connector

Once built, your connector works like any built-in connector:

```sql
-- In your SQLFlow pipeline
SOURCE crm_customers TYPE MYCRM PARAMS {
    "api_url": "https://api.mycrm.com/v1",
    "api_key": "${CRM_API_KEY}",
    "timeout": 60
};

LOAD customers FROM crm_customers;

-- Now you can use CRM data in SQL
CREATE TABLE active_customers AS
SELECT customer_id, name, email, deal_value
FROM customers 
WHERE status = 'active' 
  AND deal_value > 1000;
```

### Adding Incremental Loading

Make your connector even more powerful with incremental loading:

```python
def supports_incremental(self) -> bool:
    """CRM supports incremental loading via updated_at field."""
    return True

def read_incremental(
    self,
    object_name: str,
    cursor_field: str,
    cursor_value: Optional[Any] = None,
    columns: Optional[List[str]] = None,
    batch_size: int = 10000,
) -> Iterator[DataChunk]:
    """Read only records updated since cursor_value."""
    
    # Add cursor-based filtering to API request
    filters = {}
    if cursor_value:
        filters[cursor_field] = {">=": cursor_value}
    
    # Use regular read() method with added filters
    return self.read(object_name, columns, filters, batch_size)
```

Now your connector supports automatic watermark management:

```sql
SOURCE crm_customers TYPE MYCRM PARAMS {
    "api_url": "https://api.mycrm.com/v1",
    "api_key": "${CRM_API_KEY}",
    "sync_mode": "incremental",
    "cursor_field": "updated_at",
    "primary_key": "customer_id"
};
```

## 🔧 Building Python UDFs

### Why UDFs Matter

Sometimes SQL isn't enough. You need custom business logic, complex calculations, or specialized algorithms. Python UDFs let you bring Python's power into SQL while keeping everything in one place.

### Scalar UDFs: One Row, One Result

Most UDFs are scalar - they take values from one row and return a single result:

```python
# Source: examples/udf_examples/python_udfs/data_transforms.py
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def calculate_tax(price: float, tax_rate: float = 0.1) -> float:
    """Calculate tax on a price."""
    return price * tax_rate

@python_scalar_udf  
def is_valid_email(email: str) -> bool:
    """Check if email format is valid."""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

@python_scalar_udf
def clean_phone_number(phone: str) -> str:
    """Clean phone number format."""
    import re
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Format as (XXX) XXX-XXXX if US number
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone  # Return original if not 10 digits
```

Use them in SQL like built-in functions:

```sql
-- Load your UDF file
PYTHON_UDFS FROM "python_udfs/";

CREATE TABLE processed_customers AS
SELECT 
    customer_id,
    name,
    clean_phone_number(phone) as phone_formatted,
    is_valid_email(email) as email_valid,
    amount,
    calculate_tax(amount, 0.08) as tax_amount,
    amount + calculate_tax(amount, 0.08) as total_amount
FROM customers;
```

### Table UDFs: Transform Entire Datasets

For complex transformations that need to see all the data:

```python
from sqlflow.udfs import python_table_udf
import pandas as pd

@python_table_udf(
    output_schema={
        "customer_id": "INTEGER", 
        "name": "VARCHAR",
        "email": "VARCHAR",
        "segment": "VARCHAR",
        "score": "DOUBLE"
    }
)
def segment_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Segment customers based on purchase behavior."""
    
    # Calculate customer metrics
    df['total_purchases'] = df.groupby('customer_id')['amount'].transform('sum')
    df['purchase_frequency'] = df.groupby('customer_id')['customer_id'].transform('count')
    df['avg_purchase'] = df['total_purchases'] / df['purchase_frequency']
    
    # Create segments using business logic
    def assign_segment(row):
        if row['total_purchases'] > 10000:
            return 'VIP'
        elif row['total_purchases'] > 5000:
            return 'Premium'  
        elif row['purchase_frequency'] > 10:
            return 'Frequent'
        else:
            return 'Standard'
    
    df['segment'] = df.apply(assign_segment, axis=1)
    
    # Calculate customer score
    df['score'] = (
        df['total_purchases'] * 0.5 + 
        df['purchase_frequency'] * 0.3 + 
        df['avg_purchase'] * 0.2
    )
    
    # Return only the columns we want
    return df[['customer_id', 'name', 'email', 'segment', 'score']].drop_duplicates()

@python_table_udf
def detect_anomalies(df: pd.DataFrame, column_name: str = "amount") -> pd.DataFrame:
    """Detect anomalies using statistical methods."""
    
    # Calculate z-scores
    mean_val = df[column_name].mean()
    std_val = df[column_name].std()
    df['z_score'] = (df[column_name] - mean_val) / std_val
    
    # Mark outliers (z-score > 3)
    df['is_anomaly'] = abs(df['z_score']) > 3
    
    # Add percentile ranks
    df['percentile'] = df[column_name].rank(pct=True)
    
    return df
```

Using table UDFs:

```sql
-- Transform entire customer dataset  
CREATE TABLE customer_segments AS
SELECT * FROM segment_customers(customers);

-- Detect anomalies in sales data
CREATE TABLE sales_with_anomalies AS  
SELECT * FROM detect_anomalies(sales, 'amount');
```

### UDF Best Practices

Based on real-world usage, here are the patterns that work:

#### 1. Handle Missing Data Gracefully
```python
@python_scalar_udf
def safe_divide(numerator: float, denominator: float) -> Optional[float]:
    """Divide with safe handling of edge cases."""
    if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
        return None
    return numerator / denominator
```

#### 2. Use Type Hints for Better Integration
```python
from typing import Optional

@python_scalar_udf
def calculate_discount(
    price: float, 
    customer_tier: str, 
    default_rate: float = 0.05
) -> Optional[float]:
    """Calculate discount based on customer tier."""
    
    tier_discounts = {
        'VIP': 0.20,
        'Premium': 0.15, 
        'Standard': 0.10
    }
    
    rate = tier_discounts.get(customer_tier, default_rate)
    return price * rate if price and price > 0 else None
```

#### 3. Document Your UDFs
```python
@python_scalar_udf
def calculate_ltv(
    avg_order_value: float,
    purchase_frequency: float, 
    customer_lifespan: float
) -> float:
    """Calculate Customer Lifetime Value.
    
    LTV = Average Order Value × Purchase Frequency × Customer Lifespan
    
    Args:
        avg_order_value: Average value per order in dollars
        purchase_frequency: Number of purchases per year  
        customer_lifespan: Expected relationship duration in years
        
    Returns:
        Customer lifetime value in dollars
        
    Example:
        LTV for customer with $100 AOV, 4 purchases/year, 3 year lifespan = $1,200
    """
    return avg_order_value * purchase_frequency * customer_lifespan
```

## 🔗 Integration Patterns

### Making Connectors Production-Ready

Real production systems need reliability. SQLFlow provides resilience patterns:

```python
from sqlflow.connectors.resilience import resilient_operation, DB_RESILIENCE_CONFIG

class MyCRMConnector(Connector):
    def __init__(self):
        super().__init__()
        # Configure resilience patterns
        self.configure_resilience(DB_RESILIENCE_CONFIG)
    
    @resilient_operation()  # Auto-retry with exponential backoff
    def read(self, object_name: str, **kwargs) -> Iterator[DataChunk]:
        # Your read logic here - failures will be automatically retried
        pass
    
    def check_health(self) -> Dict[str, Any]:
        """Health monitoring for production deployment."""
        return {
            "status": "healthy" if self.is_connected else "unhealthy",
            "response_time_ms": self._last_response_time,
            "errors_last_hour": self._error_count,
            "api_rate_limit_remaining": self._rate_limit_remaining
        }
```

### Packaging and Distribution

Make your connector reusable:

```python
# setup.py or pyproject.toml
[project]
name = "sqlflow-crm-connector"
version = "1.0.0"
dependencies = ["sqlflow-core", "requests"]

[project.entry-points."sqlflow.connectors"]
MYCRM = "mycrm_connector:MyCRMConnector"
```

Install and use:

```bash
pip install sqlflow-crm-connector

# Now available in any SQLFlow project
sqlflow pipeline run my_crm_pipeline.sf
```

### Testing Your Extensions

SQLFlow provides testing utilities:

```python
# test_mycrm_connector.py
import pytest
from sqlflow.connectors.testing import ConnectorTestBase

class TestMyCRMConnector(ConnectorTestBase):
    def setup_connector(self):
        return MyCRMConnector()
    
    def get_test_config(self):
        return {
            "api_url": "https://api-test.mycrm.com/v1",
            "api_key": "test_key_123"
        }
    
    def test_connection(self):
        # Inherited test for connection functionality
        super().test_connection()
    
    def test_incremental_loading(self):
        # Test incremental loading works correctly
        pass
```

## 🚀 Contributing to SQLFlow Core

### The Bigger Picture

When you contribute to SQLFlow core, you're helping realize the founder's vision of democratizing data pipeline development. Every improvement makes it easier for someone to solve real business problems with data.

### Areas That Need Help

Based on actual usage and feedback:

1. **New Connector Types**: REST APIs, NoSQL databases, SaaS platforms
2. **Performance Optimizations**: Parallel processing, memory management
3. **UDF Enhancements**: Better type inference, streaming UDFs
4. **Developer Experience**: Better error messages, debugging tools

### Development Workflow

```bash
# Set up development environment
git clone https://github.com/sqlflow/sqlflow.git
cd sqlflow
pip install -e ".[dev]"

# Run tests to ensure everything works
pytest tests/unit tests/integration

# Make your changes, then test
pytest tests/unit/connectors/test_your_connector.py

# Run the full test suite
pytest
```

### Code Standards

SQLFlow values **simplicity and clarity** over cleverness:

```python
# Good: Clear and explicit
def read_customer_data(api_client, customer_id):
    """Read customer data from CRM API."""
    response = api_client.get(f"/customers/{customer_id}")
    return response.json()

# Avoid: Too clever or abbreviated  
def rd_cust(cli, cid):
    return cli.get(f"/c/{cid}").json()
```

### Documentation Standards

Every contribution needs human-friendly documentation:

```python
def calculate_customer_score(
    purchase_history: pd.DataFrame,
    weights: Dict[str, float]
) -> pd.DataFrame:
    """Calculate customer engagement scores.
    
    This scoring system helps identify customers who are likely to churn
    or become high-value customers. Used by the marketing team for
    targeted campaigns.
    
    Args:
        purchase_history: DataFrame with columns [customer_id, amount, date]
        weights: Scoring weights like {"recency": 0.4, "frequency": 0.3, "monetary": 0.3}
        
    Returns:
        DataFrame with customer_id and calculated score (0-100)
        
    Example:
        >>> history = pd.DataFrame({
        ...     'customer_id': [1, 1, 2],
        ...     'amount': [100, 50, 200], 
        ...     'date': ['2023-01-01', '2023-01-15', '2023-01-10']
        ... })
        >>> weights = {"recency": 0.5, "frequency": 0.3, "monetary": 0.2}
        >>> scores = calculate_customer_score(history, weights)
    """
```

## 🎯 Real-World Examples

### Example 1: Shopify Analytics Connector

```sql
-- Real example from production usage
SOURCE shopify_orders TYPE SHOPIFY PARAMS {
    "shop_domain": "${SHOPIFY_DOMAIN}",
    "access_token": "${SHOPIFY_TOKEN}",
    "sync_mode": "incremental",
    "cursor_field": "updated_at"
};

LOAD orders FROM shopify_orders;

-- Custom UDF for e-commerce metrics
CREATE TABLE daily_metrics AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_orders,
    SUM(total_price) as revenue,
    calculate_ltv(AVG(total_price), 2.5, 1.8) as avg_customer_ltv,
    detect_seasonal_trends(total_price, created_at) as seasonal_factor
FROM orders
GROUP BY DATE(created_at);
```

### Example 2: Real-Time Data Quality Pipeline

```sql
-- Load from multiple sources
SOURCE users TYPE POSTGRES PARAMS { ... };
SOURCE events TYPE S3 PARAMS { ... };

LOAD user_data FROM users;
LOAD event_data FROM events;

-- Apply data quality UDFs
CREATE TABLE clean_users AS
SELECT 
    user_id,
    validate_email(email) as email_valid,
    clean_phone_number(phone) as phone_clean,
    detect_duplicate_user(name, email, phone) as is_duplicate
FROM user_data;

-- Business intelligence with custom metrics
CREATE TABLE user_insights AS
SELECT * FROM analyze_user_behavior(
    (SELECT * FROM clean_users WHERE NOT is_duplicate),
    (SELECT * FROM event_data WHERE event_type = 'purchase')
);
```

## 🎉 Your Extension in Production

### Monitoring and Observability

```python
# Your connector provides metrics automatically
class MyCRMConnector(Connector):
    def get_performance_metrics(self) -> Dict[str, Any]:
        return {
            "requests_per_second": self._requests_per_second,
            "avg_response_time_ms": self._avg_response_time, 
            "success_rate": self._success_rate,
            "records_processed": self._total_records
        }
```

### Error Handling That Helps Users

```python
@python_scalar_udf
def calculate_roi(investment: float, return_value: float) -> Optional[float]:
    """Calculate return on investment."""
    try:
        if investment <= 0:
            logger.warning(f"Invalid investment amount: {investment}")
            return None
            
        roi = (return_value - investment) / investment * 100
        return roi
        
    except Exception as e:
        logger.error(f"ROI calculation failed: {e}")
        return None
```

### Success Metrics

When your extension is successful, you'll see:

- **Faster development**: Teams ship analytics faster
- **Better data quality**: Automated validation and cleaning  
- **Happier users**: Analysts can focus on insights, not infrastructure
- **Growing adoption**: More teams using your connector/UDF

Remember, you're not just building code - you're making data accessible to people who can use it to make better decisions. That's the real impact of extending SQLFlow.

---

**Ready to build?** Start with a simple connector or UDF that solves a real problem you have. The SQLFlow community is here to help, and every contribution makes the platform better for everyone. 