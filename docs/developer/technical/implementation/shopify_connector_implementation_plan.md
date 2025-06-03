# SQLFlow Shopify Connector Implementation Plan

**Task:** 3.1 - Shopify Connector  
**Priority:** 🔥 Critical  
**Estimated Effort:** 8 days  
**Status:** Ready to Start  
**Document Version:** 1.0  
**Date:** January 2025  

## Brainstorming Panel Review & Approval

Following the SQLFlow Brainstorming Protocol from `.cursor/rules/010-sqlflow-brainstorming.mdc`:

### Panel Composition & Roles

**Principal Product Manager (PPM) - Lead Decision Maker**
- **Responsibility**: Lead feature development and user experience decisions for Shopify connector
- **Decision Authority**: SME user requirements, feature priorities, business value validation
- **Key Focus**: Ensure connector serves real SME e-commerce analytics needs with exceptional UX

**Principal Data Engineer (PDE) - Technical Lead**  
- **Responsibility**: Lead technical discussions on data engineering aspects
- **Decision Authority**: Data pipeline integrations, processing strategies, performance requirements
- **Key Focus**: Shopify API integration, incremental loading patterns, data model optimization

**Principal Software Architect (PSA) - Architecture Lead**
- **Responsibility**: Lead architectural design discussions  
- **Decision Authority**: System components, interfaces, connector architecture
- **Key Focus**: Integration with Phase 2 infrastructure, resilience patterns, scalability design

**Junior Data Analyst (JDA) - SME User Representative**
- **Responsibility**: Represent end-users with basic SQL and Python knowledge
- **Key Focus**: Usability feedback, learning curve assessment, documentation clarity

**Data Engineer (DE) - Advanced User Representative**  
- **Responsibility**: Represent advanced users familiar with complex data infrastructure
- **Key Focus**: Integration capabilities, performance requirements, enterprise feature needs

### Panel Decision Summary

**PPM Decision**: Prioritize SME e-commerce analytics with flattened data models over full API coverage
**PDE Decision**: Implement incremental loading with 7-day lookback window for reliability  
**PSA Decision**: Leverage Phase 2 resilience infrastructure with Shopify-specific configurations
**JDA Feedback**: Simple 3-parameter setup for 80% of use cases, clear error messages essential
**DE Feedback**: Industry-standard parameter compatibility for migration from Airbyte/Fivetran

---

## Executive Summary

Implement production-ready Shopify connector for e-commerce analytics leveraging the completed Phase 2 infrastructure. Focus on SME requirements with industry-standard parameters, automatic resilience patterns, and exceptional user experience.

### Strategic Alignment

**Based on SQLFlow_Connector_Strategy_Technical_Design.md:**
- **Quality Over Quantity**: Single exceptionally well-implemented connector vs. feature quantity
- **SME-First Design**: Reliability and "just works" experience for Small-Medium Enterprises  
- **Industry Standards**: Airbyte/Fivetran parameter compatibility for easy migration
- **Phase 2 Foundation**: Leverage completed incremental loading, resilience patterns, and connector standardization

**From sqlflow_connector_implementation_tasks.md Task 3.1:**
- **Foundation Available**: Phase 2 infrastructure provides production-ready base
- **Expert Team Assembled**: Data engineers + product managers + security engineers
- **SME Requirements Defined**: Clear focus on business value over API completeness
- **Testing Strategy**: Real Shopify testing with development stores

---

## Technical Architecture

### Integration with Phase 2 Infrastructure

**Leveraging Completed Components:**
- ✅ **Incremental Loading**: Automatic watermark-based filtering with cursor fields
- ✅ **Resilience Patterns**: Retry, circuit breaker, rate limiting pre-built
- ✅ **Industry Standards**: Parameter validation framework ready
- ✅ **DuckDB State**: Watermark persistence and state management operational

**Implementation Pattern:**
```python
# Leverage existing standardized connector interface
class ShopifyConnector(Connector):
    def __init__(self):
        super().__init__()
        # Automatic resilience configuration from Phase 2
        self.configure_resilience(API_RESILIENCE_CONFIG)
        
    def get_parameter_schema(self) -> Dict[str, Any]:
        return SHOPIFY_PARAMETER_SCHEMA  # Industry-standard validation
```

### Shopify-Specific Architecture

**Core Components:**
1. **ShopifyAuthenticator**: Secure token management with domain validation
2. **ShopifyAPIClient**: Rate-limited API client with retry patterns  
3. **ShopifyDataMapper**: E-commerce data model transformation
4. **ShopifyIncrementalReader**: Cursor-based incremental loading

**Data Flow:**
```
Shopify API → Authentication → Rate Limiting → Data Mapping → Incremental Filtering → DuckDB
```

---

## SME Requirements & Data Model

### SME-Optimized Configuration

**80% Use Case - Simple Setup:**
```sql
SOURCE shopify_orders TYPE SHOPIFY PARAMS {
  "shop_domain": "${SHOPIFY_DOMAIN}",
  "access_token": "${SHOPIFY_TOKEN}",
  "sync_mode": "incremental"
};
```

**Advanced SME Configuration:**
```sql
SOURCE shopify_orders TYPE SHOPIFY PARAMS {
  "shop_domain": "${SHOPIFY_DOMAIN}",
  "access_token": "${SHOPIFY_TOKEN}",
  "sync_mode": "incremental",
  "cursor_field": "updated_at",
  "lookback_window": "P7D",
  "flatten_line_items": true,
  "include_fulfillments": true,
  "financial_status_filter": ["paid", "pending", "authorized"]
};
```

### SME Data Priorities

**Tier 1: Essential Business Data (Must Have)**
- **Orders with Flattened Line Items**: Single table for easy analysis
- **Financial Accuracy**: Revenue, tax, discounts, refunds
- **Operational Metrics**: Fulfillment status, shipping tracking

**Tier 2: Customer Analytics (High Value)**  
- **Customer LTV Data**: Order history, total spent, segment classification
- **Geographic Analysis**: Shipping addresses, regional performance
- **Privacy Compliance**: GDPR/CCPA-compliant data handling

**Tier 3: Product Performance (Medium Priority)**
- **Sales Analytics**: Product performance, inventory turnover
- **Variant Tracking**: SKU-level analytics, pricing trends

---

## Implementation Plan

### Phase 1: Core Implementation (Days 1-3)

**Day 1: Authentication & Connection**
- Implement `ShopifyAuthenticator` with secure token validation
- Add shop domain verification and security checks
- Integrate with existing resilience patterns
- Create connection testing and health monitoring

**Day 2: API Client & Data Mapping**  
- Implement `ShopifyAPIClient` with rate limiting (2 req/sec)
- Add Shopify API error handling and retry logic
- Create data model mapping for orders, customers, products
- Implement flattened line items structure for SME needs

**Day 3: Incremental Loading Integration**
- Integrate with Phase 2 watermark management
- Implement cursor-based incremental reading
- Add lookback window support for reliability
- Test incremental loading patterns

### Phase 2: SME Features & Testing (Days 4-6)

**Day 4: SME Data Models**
- Implement flattened orders table for easy analysis
- Add customer segmentation and LTV calculations
- Create product performance analytics model
- Add financial reconciliation and validation

**Day 5: Real Shopify Testing**
- Set up Shopify Partner development stores
- Generate realistic test data (orders, customers, products)
- Test with multiple store configurations (small, medium, large)
- Validate data accuracy and completeness

**Day 6: Error Handling & Edge Cases**
- Implement comprehensive error handling
- Add schema change detection and handling
- Test API rate limiting and recovery
- Validate shop maintenance and downtime scenarios

### Phase 3: Production Readiness (Days 7-8)

**Day 7: Performance & Security**
- Performance testing with large datasets (50k+ orders)
- Security validation and PII handling
- Memory usage optimization
- API cost optimization and monitoring

**Day 8: Documentation & Examples**  
- Create comprehensive implementation documentation
- Build working demo pipelines
- Create migration guide from Airbyte/Fivetran
- Final integration testing and validation

---

## Technical Specifications

### Parameter Schema (Industry-Standard)

```python
SHOPIFY_PARAMETER_SCHEMA = {
    "properties": {
        # Required parameters
        "shop_domain": {
            "type": "string",
            "description": "Shopify shop domain (e.g., 'mystore.myshopify.com')",
            "pattern": r"^[a-zA-Z0-9-]+\.myshopify\.com$"
        },
        "access_token": {
            "type": "string", 
            "description": "Shopify private app access token",
            "minLength": 20
        },
        
        # Standard sync parameters (Airbyte/Fivetran compatible)
        "sync_mode": {
            "type": "string",
            "enum": ["full_refresh", "incremental"],
            "default": "incremental"
        },
        "cursor_field": {
            "type": "string",
            "default": "updated_at",
            "description": "Field to use for incremental loading"
        },
        "lookback_window": {
            "type": "string", 
            "default": "P7D",
            "description": "ISO 8601 duration for lookback buffer"
        },
        
        # SME-specific parameters
        "flatten_line_items": {
            "type": "boolean",
            "default": true,
            "description": "Flatten order line items into separate rows"
        },
        "financial_status_filter": {
            "type": "array",
            "items": {"enum": ["authorized", "pending", "paid", "partially_paid", "refunded", "voided", "partially_refunded"]},
            "default": ["paid", "pending", "authorized"]
        },
        "include_fulfillments": {
            "type": "boolean", 
            "default": true
        },
        "include_refunds": {
            "type": "boolean",
            "default": true
        }
    },
    "required": ["shop_domain", "access_token"]
}
```

### Data Models

**Flattened Orders Model (SME Primary Requirement):**
```sql
CREATE TABLE shopify_orders_flat AS (
  -- Order identifiers
  order_id BIGINT,
  order_number VARCHAR,
  order_name VARCHAR,
  
  -- Customer information
  customer_id BIGINT,
  customer_email VARCHAR,
  customer_first_name VARCHAR,
  customer_last_name VARCHAR,
  
  -- Financial data
  total_price DECIMAL(10,2),
  subtotal_price DECIMAL(10,2), 
  total_tax DECIMAL(10,2),
  total_discounts DECIMAL(10,2),
  currency VARCHAR(3),
  
  -- Order status
  financial_status VARCHAR,
  fulfillment_status VARCHAR,
  cancelled_at TIMESTAMP,
  
  -- Timestamps
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  processed_at TIMESTAMP,
  
  -- Line item details (flattened)
  line_item_id BIGINT,
  product_id BIGINT,
  variant_id BIGINT,
  sku VARCHAR,
  product_title VARCHAR,
  variant_title VARCHAR,
  vendor VARCHAR,
  quantity INTEGER,
  price DECIMAL(10,2),
  line_total DECIMAL(10,2),
  
  -- Geographic data
  shipping_country VARCHAR,
  shipping_province VARCHAR,
  shipping_city VARCHAR,
  shipping_zip VARCHAR,
  
  -- Fulfillment tracking  
  tracking_company VARCHAR,
  tracking_number VARCHAR,
  tracking_url VARCHAR
);
```

### Resilience Configuration

```python
SHOPIFY_RESILIENCE_CONFIG = {
    "retry": {
        "max_attempts": 3,
        "backoff_type": "exponential", 
        "initial_delay": 1.0,
        "max_delay": 60.0,
        "jitter": True,
        "exceptions": [
            "requests.exceptions.RequestException",
            "shopify.ShopifyException"
        ]
    },
    "circuit_breaker": {
        "failure_threshold": 5,
        "recovery_timeout": 300,  # 5 minutes
        "expected_exception": "Exception"
    },
    "rate_limit": {
        "max_requests": 2,       # Shopify standard limit
        "window_seconds": 1,
        "burst_limit": 40,       # Shopify bucket size
        "backpressure": "wait"
    }
}
```

---

## Testing Strategy

### Real Shopify Testing Setup

**Development Store Options:**
1. **Shopify Partner Account** (Free, Recommended)
   - Sign up at: https://partners.shopify.com/
   - Create unlimited development stores
   - Full API access with test data

2. **Test Store Configuration Matrix:**
   - **Small SME**: 100-500 orders, 20-100 products
   - **Growing SME**: 1k-5k orders, 200-1k products  
   - **Stress Test**: 50k+ orders, 5k+ products

### Comprehensive Testing Checklist

**Authentication & Security:**
- ✅ Private app token validation
- ✅ Shop domain security checks
- ✅ Rate limit compliance
- ✅ PII handling and anonymization

**Data Accuracy:**
- ✅ Financial calculations (100% accuracy required)
- ✅ Order status tracking
- ✅ Line item flattening accuracy
- ✅ Incremental loading correctness

**Performance & Reliability:**
- ✅ Large dataset handling (50k+ orders)
- ✅ Memory usage optimization
- ✅ Automatic error recovery
- ✅ Schema change handling

### Demo Pipeline

```sql
-- examples/shopify_sme_analytics/01_shopify_orders_demo.sf

-- Load Shopify orders with SME-optimized configuration
SOURCE shopify_orders TYPE SHOPIFY PARAMS {
  "shop_domain": "${SHOPIFY_DOMAIN}",
  "access_token": "${SHOPIFY_TOKEN}",
  "sync_mode": "incremental",
  "flatten_line_items": true,
  "include_fulfillments": true
};

LOAD orders_raw FROM shopify_orders MODE APPEND;

-- Create SME business analytics
CREATE TABLE daily_sales_summary AS
SELECT 
  DATE(created_at) as order_date,
  COUNT(DISTINCT order_id) as total_orders,
  SUM(total_price) as daily_revenue,
  AVG(total_price) as avg_order_value,
  COUNT(CASE WHEN fulfillment_status = 'fulfilled' THEN 1 END) as fulfilled_orders
FROM orders_raw
WHERE created_at >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY DATE(created_at)
ORDER BY order_date DESC;

EXPORT SELECT * FROM daily_sales_summary
TO "output/shopify_daily_sales.csv"
TYPE CSV OPTIONS { "header": true };
```

---

## Success Criteria

### Business Value Metrics
- ✅ **Time to Insights**: <5 minutes from setup to working dashboard
- ✅ **Configuration Simplicity**: 3 parameters for 80% of SME use cases
- ✅ **Data Completeness**: 95% of SME-required fields without custom config
- ✅ **Performance**: Handle 50k+ orders with <2GB memory usage
- ✅ **Cost Efficiency**: Zero Shopify API overage charges

### Technical Validation
- ✅ **API Compatibility**: Works with all Shopify plan types
- ✅ **Error Recovery**: Graceful handling of failures and maintenance windows
- ✅ **Data Accuracy**: 100% financial calculation accuracy
- ✅ **Migration Ready**: Direct parameter compatibility with Airbyte connector
- ✅ **SME Optimized**: "Just works" reliability for small business teams

### Expert Panel Validation Process
1. **PDE Review**: Validate data model against real SME requirements
2. **PPM Testing**: Confirm business metrics match SME analytics needs
3. **PSA Audit**: Architecture integration and scalability assessment  
4. **JDA User Testing**: Usability validation with business users
5. **DE Performance Review**: Integration capabilities and advanced features

---

## Implementation Files

### Core Implementation
- `sqlflow/connectors/shopify_connector.py` - Main connector implementation
- `sqlflow/connectors/shopify_auth.py` - Authentication and security  
- `sqlflow/connectors/shopify_models.py` - Data model definitions
- `tests/unit/connectors/test_shopify_connector.py` - Unit tests
- `tests/integration/connectors/test_shopify_integration.py` - Integration tests

### Documentation & Examples
- `examples/shopify_sme_analytics/` - Complete demo project
- `docs/user/reference/connectors/shopify.md` - User documentation
- `docs/migration/shopify_from_airbyte.md` - Migration guide

### Testing Infrastructure  
- `tests/fixtures/shopify_test_data.json` - Realistic test data
- `scripts/setup_shopify_dev_store.sh` - Development store setup

---

## Delivery Timeline

| Phase | Duration | Deliverables | Validation |
|-------|----------|-------------|------------|
| **Core Implementation** | Days 1-3 | Authentication, API client, incremental loading | Unit tests, basic connectivity |
| **SME Features** | Days 4-6 | Data models, real testing, error handling | Integration tests, demo pipeline |
| **Production Ready** | Days 7-8 | Performance, security, documentation | Expert panel review, success criteria |

**Ready to Start Implementation:**
```bash
git checkout -b shopify-connector-implementation
cd docs/developer/technical/implementation/
# Reference: shopify_connector_implementation_plan.md
# Begin with core authentication and API client
```

This comprehensive plan consolidates all requirements, leverages Phase 2 infrastructure, follows expert panel guidance, and provides clear implementation path for developers. 