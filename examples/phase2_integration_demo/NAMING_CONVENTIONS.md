# SQLFlow Naming Conventions Reference

Following the SQLFlow Engineering Principles for consistency and clarity.

## File Types

### Shell Scripts
- **User-facing**: `kebab-case.sh` (e.g., `quick-start.sh`)
- **Internal utilities**: `snake_case.sh` (e.g., `setup_environment.sh`)
- **Test scripts**: `test_component.sh` (e.g., `test_postgres_connector.sh`)

### Python Scripts  
- **All Python files**: `snake_case.py`
- **Examples**: `setup_s3_test_data.py`, `test_verification.py`

### Pipeline Files
- **Format**: `NN_descriptive_name.sf` (numbered for execution order)
- **Examples**: `01_postgres_basic_test.sf`, `06_enhanced_s3_connector_demo.sf`

### Configuration Files
- **Docker**: `docker-compose.yml`, `Dockerfile` 
- **Config**: `snake_case.yml` or `kebab-case.conf`

## Directory Naming
- **All directories**: `snake_case` or `kebab-case`
- **Examples**: `init-scripts/`, `debug/scripts/`, `target/`

## Variable Naming (in scripts)
- **Environment variables**: `UPPER_SNAKE_CASE`
- **Local variables**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`

## Examples of Good Naming

```bash
# Shell scripts
scripts/
├── ci_utils.sh                    # Internal utility (snake_case)  
├── run_integration_demo.sh        # Internal script (snake_case)
├── test_postgres_connector.sh     # Test script (snake_case)
└── setup_environment.sh           # Setup script (snake_case)

# User-facing entry points
quick_start.sh                     # Main entry (snake_case acceptable)

# Python scripts  
scripts/
├── setup_s3_test_data.py         # Data setup (snake_case)
└── validate_configuration.py      # Validation (snake_case)

# Pipeline files
pipelines/
├── 01_postgres_basic_test.sf      # Numbered, descriptive
├── 02_incremental_loading_test.sf # Clear functionality
└── 06_enhanced_s3_connector_demo.sf # Feature indication
```

## Rationale

1. **Consistency**: Same type of files follow same convention
2. **Predictability**: Developers can guess file names  
3. **Tooling-friendly**: Works well with shell completion and IDEs
4. **Readable**: Clear separation between words
5. **Cross-platform**: Avoids case-sensitivity issues
