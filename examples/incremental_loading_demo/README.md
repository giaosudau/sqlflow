# SQLFlow Phase 1 Incremental Loading Demo

This demo showcases SQLFlow's Phase 1 enhanced features for incremental data loading, focusing on real-world scenarios and performance optimization concepts.

## 🚀 Quick Start

```bash
cd examples/incremental_loading_demo
./run_demo.sh
```

## 📁 Project Structure

```
incremental_loading_demo/
├── data/                              # Sample datasets
│   ├── customers_initial.csv          # Initial customer data (10 customers)
│   ├── customers_updates.csv          # Updated/new customers (5 records)
│   ├── orders_day1.csv               # Day 1 orders
│   ├── orders_day2.csv               # Day 2 orders  
│   └── products.csv                   # Product reference data
├── pipelines/                         # Demo pipelines
│   ├── 01_basic_incremental.sf       # Core incremental loading concepts
│   ├── 02_mixed_sync_modes.sf        # Different sync strategies
│   ├── 03_error_recovery.sf          # Error handling & debugging
│   └── 04_ecommerce_analytics.sf     # Complete analytics workflow
├── profiles/                          # Environment configurations
│   ├── dev.yml                       # Development profile
│   ├── test.yml                      # Testing profile
│   └── production.yml                # Production profile
├── output/                           # Generated results
└── run_demo.sh                      # Automated demo runner
```

## 🎯 Phase 1 Features Demonstrated

### 1. Core Incremental Loading (`01_basic_incremental.sf`)
- **MERGE-based Updates**: Demonstrates upsert operations using `MERGE_KEYS`
- **Performance Concepts**: Shows data volume reduction (10 → 5 records processed)
- **Variable Substitution**: Uses `${data_dir}` and `${output_dir}` from profiles
- **CREATE OR REPLACE**: Advanced table management with proper SQL generation

**Key Results:**
- Initial Load: 10 customers → Final: 13 customers (3 new + 2 updated)
- Status breakdown: 12 active, 1 pending customers
- Watermark tracking: Latest activity timestamp managed

### 2. Mixed Sync Modes (`02_mixed_sync_modes.sf`)
- **Full Refresh vs Incremental**: Different strategies per data source
- **Reference Data Handling**: Static data (products) vs transactional data
- **Load Mode Flexibility**: REPLACE, APPEND, and MERGE in one pipeline

### 3. Error Recovery & Debugging (`03_error_recovery.sf`)
- **Graceful Error Handling**: Pipeline continues after recoverable errors
- **Data Quality Patterns**: Validation and cleansing workflows
- **Debugging Concepts**: Clear error reporting and data lineage

### 4. Complete E-commerce Analytics (`04_ecommerce_analytics.sf`)
- **Multi-source Integration**: Customers, orders, and products
- **Business Logic**: Revenue calculations, customer segmentation
- **Performance Optimization**: Incremental aggregations

## 🔧 Technical Improvements Implemented

### Phase 1 Backend Enhancements

1. **CREATE OR REPLACE TABLE Support**
   - **Added is_replace field**: Extended `SQLBlockStep` in AST to track CREATE OR REPLACE usage
   - **Parser Enhancement**: Modified parser to detect and set `is_replace` flag correctly
   - **SQL Generation**: Updated `SQLGenerator` to respect `is_replace` flag when generating SQL
   - **Execution Plan**: Enhanced `Planner` to pass `is_replace` field to execution operations
   - **Local Executor**: Modified `LocalExecutor` to use `is_replace` flag in SQL execution
   - **Validation Logic**: Updated duplicate table validation to allow CREATE OR REPLACE redefining same table

2. **Industry-Standard SOURCE Parameters**
   - **Schema Enhancement**: Extended connector schemas in `schemas.py` to support:
     - `sync_mode`: "full_refresh" or "incremental"
     - `primary_key`: String field for incremental loading identification
     - `cursor_field`: Timestamp/ID field for incremental loading watermarks
   - **Validation Integration**: Added parameter validation with helpful error messages
   - **Airbyte/Fivetran Compatibility**: Industry-standard parameter naming for easy migration

3. **Enhanced Validation and Error Handling**
   - **Validation Cache**: Implemented smart caching system for improved validation performance
   - **Error Formatting**: Enhanced error messages with detailed suggestions and help URLs
   - **Variable Substitution**: Fixed validation to work correctly with variable substitution
   - **Profile Integration**: Improved variable handling from profiles during validation

4. **SQL Parser Improvements**
   - **UNION Support**: Added UNION and ALL tokens to lexer for complex analytical queries
   - **Token Handling**: Fixed linter errors and improved Optional[Token] return types
   - **SQL Syntax**: Enhanced SQL block parsing for complex CREATE statements

## 📊 Demo Results

The demo script (`run_demo.sh`) shows simple pipeline execution status:

```bash
📋 Pipeline Execution Summary:
  ✅ 01_basic_incremental: Successful
  ✅ 02_mixed_sync_modes: Successful
  ✅ 03_error_recovery: Successful
  ✅ 04_ecommerce_analytics: Successful
```

After running the basic incremental demo:

```bash
# Customer Summary
status,customer_count,first_signup,last_activity
pending,1,2024-01-02T13:00:00Z,2024-01-02T13:00:00Z 
active,12,2024-01-01T09:00:00Z,2024-01-02T12:00:00Z

# Performance Impact
- Initial Load: 10 customers processed (full dataset)
- Incremental Update: 5 customers processed (50% reduction)
- Update Types: 2 existing customers updated, 3 new customers added
```

## 🏗️ Phase 1 Roadmap Features

### Currently Available
- ✅ MERGE-based incremental loading
- ✅ Variable substitution from profiles
- ✅ Multiple load modes (REPLACE, APPEND, MERGE)
- ✅ Error recovery mechanisms
- ✅ Performance monitoring
- ✅ CREATE OR REPLACE TABLE support
- ✅ Industry-standard SOURCE parameters
- ✅ Enhanced validation with caching
- ✅ Improved error messages and debugging

### Phase 1 Enhancements (In Development)
- 🚧 Automatic watermark management
- 🚧 Zero-configuration incremental loading
- 🚧 Advanced performance analytics
- 🚧 Enhanced UDF integration

## 🏃‍♂️ Running Individual Pipelines

```bash
# Run specific pipeline
sqlflow pipeline run 01_basic_incremental --profile dev

# With custom variables
sqlflow pipeline run 01_basic_incremental --profile dev \
    --vars '{"data_dir": "custom_data", "output_dir": "custom_output"}'

# Validate before running
sqlflow pipeline validate 01_basic_incremental --profile dev

# Compile to execution plan
sqlflow pipeline compile 01_basic_incremental --profile dev
```

## 🔍 Understanding the Data Flow

### Initial State
```
customers_initial.csv (10 customers) 
└─ LOAD → customers_table (10 rows)
   └─ EXPORT → 01_initial_load_summary.csv
```

### Incremental Update
```
customers_updates.csv (5 records: 2 updates + 3 new)
└─ LOAD → customers_table (MERGE mode with customer_id)
   ├─ UPDATE: customer_id 2 (new email)
   ├─ UPDATE: customer_id 5 (new timestamp) 
   ├─ INSERT: customer_id 11 (new)
   ├─ INSERT: customer_id 12 (new)
   └─ INSERT: customer_id 13 (new, pending status)
```

### Final Analytics
```
customer_summary table:
├─ active: 12 customers
└─ pending: 1 customer
```

## 🐛 Known Issues & Workarounds

1. **Complex UNION Queries**: May generate parser warnings but still execute correctly
   - **Workaround**: Use simple SELECT statements for complex analytical queries
   - **Status**: Parser enhancement completed

2. **Database Cleanup**: Persistent mode requires manual database cleanup between runs
   - **Workaround**: `rm -f target/demo.duckdb` before re-running
   - **Status**: Auto-cleanup planned for Phase 1

## 🧪 Testing

This demo includes comprehensive integration tests following SQLFlow testing standards:
- Real data processing (no mocks)
- End-to-end pipeline validation
- CREATE OR REPLACE functionality tests
- Industry-standard parameter validation tests
- Error handling and recovery tests

## 🤝 Contributing

This demo represents SQLFlow's Phase 1 capabilities. Contributions welcome:

1. Additional demo scenarios
2. Performance benchmarks
3. Error handling improvements
4. Documentation enhancements

## 📞 Support

For issues or questions about this demo:
- Check the SQLFlow documentation
- Review the pipeline execution logs
- Test with simpler datasets first

---

**Demo Status**: ✅ Working (Phase 1 Backend Features)  
**Last Updated**: December 2024  
**SQLFlow Version**: 0.1.7+ 