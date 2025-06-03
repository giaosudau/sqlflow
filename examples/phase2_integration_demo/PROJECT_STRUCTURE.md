# SQLFlow Phase 2 Integration Demo - Project Structure

## Directory Organization

```
phase2_integration_demo/
├── pipelines/           # SQLFlow pipeline definitions (.sf files)
├── scripts/             # Main demo and utility scripts
├── debug/              # Development and debugging scripts
├── config/             # Service configuration files
├── init-scripts/       # Database initialization scripts
├── profiles/           # SQLFlow execution profiles
├── output/             # Generated output files
├── target/             # SQLFlow execution artifacts
├── docker-compose.yml  # Service orchestration
├── Dockerfile         # SQLFlow service container
├── quick_start.sh     # Main demo entry point
└── README.md          # Primary documentation
```

## Naming Conventions

Following SQLFlow Engineering Principles:

### Shell Scripts (kebab-case)
- `quick-start.sh` - Main entry point
- `run-integration-demo.sh` - Test execution
- `test-postgres-connector.sh` - Component testing

### Python Scripts (snake_case)
- `setup_s3_test_data.py` - Data preparation
- `test_s3_verification.py` - Debug utilities

### Pipeline Files (snake_case)
- `01_postgres_basic_test.sf` - Numbered for execution order
- `02_incremental_loading_test.sf` - Descriptive functionality
- `06_enhanced_s3_connector_demo.sf` - Clear feature indication

## File Purpose Classification

### Production Files
- Core demo functionality
- User-facing documentation
- Service configuration

### Development Files  
- Debug scripts in `debug/`
- Backup files (auto-cleaned)
- Temporary outputs
