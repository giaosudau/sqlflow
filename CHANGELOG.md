# Changelog

All notable changes to the SQLFlow project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New two-stage release process for better quality control
  - Added TestPyPI stage for pre-release validation
  - Streamlined promotion to PyPI with dedicated scripts
  - Improved release documentation and guides
- Support for conditional execution in SQLFlow pipelines
- Enhanced variable support in SQL queries and configuration
- Region-based analysis capabilities in demos

### Fixed
- Critical bug: Fixed SQL query formatting to properly handle dots in table.column references
  - Added DOT token type to lexer
  - Implemented special handling for DOT tokens in SQL query formatting
  - Fixed spaces around dots that were causing SQL syntax errors
  - Added comprehensive tests for dot operator handling
- Fixed SQL function call formatting to remove spaces between function names and parentheses
  - Properly formats SQL functions like COUNT(DISTINCT...) instead of COUNT ( DISTINCT... )
  - Removes spaces between parentheses and their content
  - Ensures proper execution of SQL aggregate functions
- Fixed variable reference formatting in conditions and exports
  - Properly formats ${var|default} syntax without spaces between components
  - Handles spaces within variable references that were causing evaluation errors
  - Fixes spaces around pipes in default values
  - Ensures correct variable substitution in conditional expressions
- Enhanced SQL query parsing and formatting
- Improved error handling in parser for better debugging

### Added
- Support for conditional execution in SQLFlow pipelines
- Enhanced variable support in SQL queries and configuration
- Region-based analysis capabilities in demos
- **Comprehensive logging system improvements**:
  - Centralized logging configuration in `sqlflow/logging.py`
  - Multiple configuration methods (environment variables, command line, profiles, programmatic API)
  - Module-specific log level configuration
  - Enhanced logging across critical components:
    - SQL generation with variable substitution tracking
    - Pipeline planning and execution with detailed debugging info
    - UDF discovery and registration with improved error reporting
    - Query execution with detailed step tracking
  - Standardized log format across all modules

### Changed
- Restructured parser code for better maintainability
- Improved SQL tokenization for more accurate query parsing
- Updated documentation with clearer examples

## [0.1.0] - 2025-05-17

### Added
- Initial release of SQLFlow
- Basic SQL pipeline execution
- Support for CSV, PostgreSQL and S3 connectors
- Pipeline visualization support
- CLI for running SQLFlow pipelines 