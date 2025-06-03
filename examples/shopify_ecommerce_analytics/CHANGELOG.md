# Changelog

All notable changes to the SQLFlow Shopify connector will be documented in this file.

## [1.0.0] - 2025-06-03

### Added
- **Complete Shopify Connector Implementation**
  - Support for orders, customers, and products data streams
  - Full integration with SQLFlow validation system
  - Comprehensive parameter validation with proper error messages
  - Support for both incremental and full refresh sync modes

- **Environment Variable Support**
  - Full support for environment variables in pipeline validation
  - Automatic .env file loading
  - Fallback to environment variables when explicit variables not provided
  - Integration with modern VariableSubstitutionEngine

- **Example Pipelines**
  - Basic connection test pipeline
  - Secure environment variable pipeline
  - Comprehensive business analytics pipeline  
  - Hardcoded test pipeline for development

- **Business Analytics Templates**
  - Daily sales summary with key metrics
  - Top products analysis by revenue
  - Customer segmentation by lifetime value
  - Export functionality to CSV format

### Fixed
- **Connector Registration**: SHOPIFY connector now properly auto-loads with the connectors package
- **Validation Schema**: Added comprehensive validation schema for all Shopify connector parameters
- **Environment Variable Substitution**: Updated validation system to use modern VariableSubstitutionEngine that supports environment variables
- **JSON Parameter Parsing**: Fixed "Expected JSON object" errors when using environment variables in PARAMS sections

### Technical Details
- Updated `sqlflow/cli/validation_helpers.py` to use `VariableSubstitutionEngine` instead of legacy `VariableContext`
- Added comprehensive test coverage in `tests/unit/cli/test_pipeline_validation.py`
- Integrated with existing variable substitution architecture for consistency

### Usage
```bash
# Set credentials
export SHOPIFY_STORE="your-store.myshopify.com"
export SHOPIFY_TOKEN="shpat_your_token"

# Validate and run (both work seamlessly)
sqlflow pipeline validate shopify_analytics
sqlflow pipeline run shopify_analytics
```

### Breaking Changes
None - all changes are backward compatible.

### Migration Guide
No migration needed. Existing hardcoded pipelines continue to work. Environment variables now fully supported for both validation and execution.

---

## Development

### Requirements Met
- ✅ Connector properly registered and discoverable
- ✅ Validation schema complete with error handling
- ✅ Environment variable support in validation
- ✅ Full integration with SQLFlow pipeline system
- ✅ Production-ready error handling and logging
- ✅ Comprehensive test coverage

### Architecture
The Shopify connector follows SQLFlow's standard connector architecture:
1. **Registration**: Auto-loaded via `sqlflow.connectors.__init__.py`
2. **Validation**: Schema-based validation via `sqlflow.validation.schemas`
3. **Execution**: Standard connector interface implementation
4. **Variables**: Modern VariableSubstitutionEngine integration 