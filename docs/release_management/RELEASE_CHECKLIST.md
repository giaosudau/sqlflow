# Release Quality Checklist

Before promoting a release from TestPyPI to PyPI, verify the following:

## Package Verification

- [ ] Package installs successfully from TestPyPI
  ```bash
  pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sqlflow==X.Y.Z
  ```

- [ ] Basic functionality works
  ```bash
  sqlflow --version
  sqlflow --help
  ```

- [ ] Initialize a new test project
  ```bash
  sqlflow init test_project
  cd test_project
  ```

## Smoke Tests

- [ ] Basic showcase examples (based on examples/ecommerce/README.md)
  ```bash
  cd examples/ecommerce
  # Basic CSV processing showcase
  sqlflow pipeline run showcase_01_basic_csv_processing --vars '{"date": "2023-10-25"}'
  # Multi-connector integration showcase
  sqlflow pipeline run showcase_02_multi_connector_integration --vars '{"date": "2023-10-25", "API_TOKEN": "test_token"}'
  # Conditional execution showcase
  sqlflow pipeline run showcase_03_conditional_execution --vars '{"environment": "production", "region": "us-east"}'
  # Advanced analytics showcase
  sqlflow pipeline run showcase_04_advanced_analytics
  ```

- [ ] Practical examples
  ```bash
  cd examples/ecommerce
  # Daily sales report example
  sqlflow pipeline run example_01_daily_sales_report --vars '{"date": "2023-10-25", "API_TOKEN": "test_token"}'
  # CSV-only processing example
  sqlflow pipeline run example_02_csv_only_processing
  ```

- [ ] Python UDFs functionality works correctly
  ```bash
  cd examples/python_udfs
  sqlflow pipeline run udf_demo
  ```

- [ ] Conditional pipelines execute properly
  ```bash
  cd examples/conditional_pipelines
  sqlflow pipeline run environment_based.sf
  ```

- [ ] Pipeline commands from CLI guide work correctly
  ```bash
  # Pipeline listing works
  sqlflow pipeline list
  # Pipeline validation works
  sqlflow pipeline validate example --profile dev
  # Pipeline compilation works
  sqlflow pipeline compile example
  ```

## Documentation Check

- [ ] CLI documentation is up-to-date with any new commands or options
  - Check against `docs/cli_guide.md`
  - Verify CLI commands work as documented
  
- [ ] All new features are documented in relevant guides:
  - `docs/syntax.md` for SQL syntax changes
  - `docs/python_udfs.md` for UDF changes
  - `docs/getting_started.md` for basic workflow changes
  - `docs/connector_usage_guide.md` for connector changes
  
- [ ] Examples are up-to-date with the latest features and changes
  - `examples/ecommerce` reflects current recommended patterns
  - `examples/python_udfs` demonstrates current UDF capabilities
  - `examples/conditional_pipelines` shows conditional execution features

- [ ] README.md reflects current feature set and installation instructions

## Compatibility Check

- [ ] Test on supported Python versions (3.8+)
  ```bash
  # If available, test with different Python environments
  # For example, using a Python 3.8 environment:
  python3.8 -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sqlflow==X.Y.Z
  python3.8 -c "import sqlflow; print(sqlflow.__version__)"
  ```

- [ ] Test connection profiles with supported data sources
  ```bash
  # Test with different profiles as mentioned in examples/ecommerce/README.md
  cd examples/ecommerce
  # Test development profile
  sqlflow connect test dev
  sqlflow pipeline run pipelines/showcase_01_basic_csv_processing.sf --profile dev
  # Test production profile
  sqlflow connect test production
  sqlflow pipeline run pipelines/showcase_01_basic_csv_processing.sf --profile production
  ```

- [ ] Verify DuckDB integration functions correctly
  ```bash
  # Test core DuckDB functionality
  cd examples/ecommerce
  sqlflow pipeline run orders --profile dev
  ```

## Advanced Testing (Optional)

- [ ] Full ecommerce demo environment setup works (if applicable)
  ```bash
  cd examples/ecommerce
  # Build SQLFlow package (optional)
  ./build-sqlflow-package.sh
  # Start the demo environment
  ./start-demo.sh
  # Run demo initialization
  ./init-demo.sh
  # Verify all containers are running
  docker compose ps
  # Clean up after testing
  docker compose down
  ```
## Final Approval

- [ ] All checks have passed without errors or unexpected warnings
- [ ] Any deprecation warnings or breaking changes are documented
- [ ] Performance is consistent with previous releases

If all checks pass, proceed with promoting the release:

```bash
./release_to_pypi.sh vX.Y.Z
```

After promoting to PyPI, verify the release is available:

```bash
# Uninstall test version
pip uninstall -y sqlflow

# Install from PyPI
pip install sqlflow==X.Y.Z

# Verify version
sqlflow --version
```

Create a GitHub release following the template in `docs/release_management/GITHUB_RELEASE_TEMPLATE.md`.
