# Contributing to SQLFlow: Building the Future of Data Pipelines

## Welcome to the SQLFlow Community

Thank you for your interest in contributing to SQLFlow! You're joining a mission to democratize data pipeline development - making it so simple that anyone can build powerful data analytics without complex infrastructure.

### Our Vision

SQLFlow exists because its founder experienced the pain of managing thousands of data pipelines at App Annie. The vision is simple: **SQL should be enough**. Every contribution you make helps realize this vision by making data accessible to analysts, engineers, and business people alike.

## 🚀 Getting Started

### Your First Contribution

**Found a Bug?** 
- Please open an issue with detailed reproduction steps, SQLFlow version, and environment
- Include sample pipeline code that demonstrates the issue
- Screenshots or error logs are incredibly helpful

**Have an Idea?** 
- We love feature requests! Open an issue describing:
  - The problem you're trying to solve
  - How it would help democratize data pipeline development
  - Your proposed solution (if you have one)

**Ready to Code?**
- Start by opening an issue to discuss your planned changes
- Look for issues tagged `good first issue` or `help wanted`
- Check our [extending-sqlflow.md](extending-sqlflow.md) guide for building connectors and UDFs

### The SQLFlow Development Philosophy

We value **simplicity and clarity** over cleverness. Code should be readable by someone learning SQLFlow for the first time. Documentation should explain not just *what* but *why*.

Remember: every line of code you write could be the one that makes data analysis possible for someone who previously couldn't access it.

## 🛠 Development Setup

### Prerequisites

- Python 3.8 or higher
- Git
- A SQLFlow-compatible operating system (Linux, macOS, Windows)

### Local Development Environment

```bash
# 1. Clone the repository
git clone https://github.com/sqlflow/sqlflow.git
cd sqlflow

# 2. Create and activate a virtual environment
python -m venv venv

# On Linux/Mac:
source venv/bin/activate

# On Windows:
.\venv\Scripts\activate

# 3. Install SQLFlow in development mode
pip install -e ".[dev]"

# 4. Install pre-commit hooks for code quality
pre-commit install

# 5. Verify your setup works
pytest tests/unit/
```

### Verify Your Installation

Run a simple test to ensure everything works:

```bash
# Create a test pipeline
echo 'SOURCE test TYPE CSV PARAMS {"path": "test.csv"};' > test.sf

# Create test data
echo -e "id,name\n1,Alice\n2,Bob" > test.csv

# Run SQLFlow (this should work without errors)
sqlflow --help
```

### Development Workflow

```bash
# 1. Create a feature branch
git checkout -b feature/your-feature-name

# 2. Make your changes
# ... edit code ...

# 3. Add tests for your changes
# ... write tests ...

# 4. Run tests locally
pytest tests/unit/your_test_file.py

# 5. Run all tests to ensure nothing breaks
pytest

# 6. Run code quality checks
pre-commit run --all-files

# 7. Commit your changes
git add .
git commit -m "Add feature: your descriptive message"

# 8. Push and create a pull request
git push origin feature/your-feature-name
```

## 🧪 Testing Your Changes

SQLFlow has comprehensive test coverage to ensure reliability. We test everything from individual functions to complete end-to-end workflows.

### Test Structure

```
tests/
├── unit/           # Fast, isolated tests
├── integration/    # Tests with real components
└── performance/    # Performance and scalability tests
```

### Running Tests

```bash
# Run all unit tests (fast)
pytest tests/unit/

# Run integration tests (slower, requires dependencies)  
pytest tests/integration/

# Run tests for specific component
pytest tests/unit/connectors/

# Run tests with coverage report
pytest --cov=sqlflow tests/unit/

# Run performance tests
pytest tests/performance/
```

### Writing Effective Tests

#### Unit Tests: Testing Individual Components

```python
# Example: Testing a UDF
from sqlflow.udfs import python_scalar_udf

@python_scalar_udf
def calculate_tax(price: float, rate: float = 0.1) -> float:
    """Calculate tax on a price."""
    return price * rate

def test_calculate_tax():
    """Test tax calculation UDF."""
    # Test normal case
    assert calculate_tax(100.0, 0.08) == 8.0
    
    # Test edge cases
    assert calculate_tax(0.0, 0.1) == 0.0
    assert calculate_tax(100.0, 0.0) == 0.0
    
    # Test default rate
    assert calculate_tax(100.0) == 10.0
```

#### Integration Tests: Testing Component Interaction

```python
# Example: Testing connector integration
import tempfile
import os
import pandas as pd
from sqlflow.connectors.csv_connector import CSVConnector

def test_csv_connector_integration():
    """Test CSV connector with real file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test data
        test_file = os.path.join(temp_dir, "test.csv")
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        test_data.to_csv(test_file, index=False)
        
        # Test connector
        connector = CSVConnector()
        connector.configure({"path": test_file})
        
        # Test connection
        result = connector.test_connection()
        assert result.success
        
        # Test reading data
        chunks = list(connector.read("test"))
        assert len(chunks) == 1
        assert len(chunks[0].pandas_df) == 3
```

#### End-to-End Tests: Complete Pipeline Testing

```python
# Example: Testing complete pipeline execution
def test_complete_analytics_pipeline():
    """Test a complete analytics pipeline."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test pipeline
        pipeline_content = f"""
        SOURCE customers TYPE CSV PARAMS {{
            "path": "{temp_dir}/customers.csv"
        }};
        
        LOAD customer_data FROM customers;
        
        CREATE TABLE customer_analysis AS
        SELECT 
            COUNT(*) as total_customers,
            AVG(purchase_amount) as avg_purchase,
            MAX(purchase_date) as latest_purchase
        FROM customer_data;
        
        EXPORT SELECT * FROM customer_analysis 
        TO "{temp_dir}/analysis.csv" 
        TYPE CSV;
        """
        
        # Execute pipeline and verify results
        # ... pipeline execution code ...
        
        # Verify output file exists and contains expected data
        assert os.path.exists(f"{temp_dir}/analysis.csv")
```

### Testing Best Practices

1. **Test the Happy Path**: Ensure normal usage works correctly
2. **Test Edge Cases**: Empty data, missing files, invalid parameters
3. **Test Error Conditions**: How does your code handle failures?
4. **Use Descriptive Names**: Test names should explain what they verify
5. **Keep Tests Independent**: Each test should be able to run in isolation
6. **Use Temporary Resources**: Don't leave test files lying around

## 📝 Documentation Standards

Good documentation is crucial for democratizing data pipeline development. Every feature should be accessible to newcomers.

### Documentation Principles

1. **Human-Centered**: Write for people, not machines
2. **Example-Driven**: Show real-world usage scenarios  
3. **Progressive Disclosure**: Start simple, add complexity gradually
4. **Explain the Why**: Don't just show how, explain why it matters

### Writing Documentation

```python
def calculate_customer_lifetime_value(
    avg_order_value: float,
    purchase_frequency: float, 
    customer_lifespan: float
) -> float:
    """Calculate Customer Lifetime Value for business analysis.
    
    This metric helps businesses understand the total value a customer
    brings over their entire relationship. Marketing teams use this to
    set acquisition budgets and identify high-value customer segments.
    
    Args:
        avg_order_value: Average value per order in dollars
        purchase_frequency: Number of purchases per year  
        customer_lifespan: Expected relationship duration in years
        
    Returns:
        Customer lifetime value in dollars
        
    Example:
        >>> # Customer with $100 average order, 4 purchases/year, 3 year lifespan
        >>> ltv = calculate_customer_lifetime_value(100.0, 4.0, 3.0)
        >>> print(f"Customer LTV: ${ltv:,.2f}")
        Customer LTV: $1,200.00
        
    Note:
        This uses a simple multiplication model. For more sophisticated
        calculations, consider churn rates and discount factors.
    """
    return avg_order_value * purchase_frequency * customer_lifespan
```

### Documentation Checklist

- [ ] Function/class has clear docstring
- [ ] Parameters and return values are explained
- [ ] Real-world example is provided
- [ ] Edge cases and limitations are noted
- [ ] Links to related documentation

## 🔄 Pull Request Process

### Before You Submit

1. **Run the full test suite**: `pytest`
2. **Check code formatting**: `pre-commit run --all-files`
3. **Update documentation** for any new features
4. **Add tests** for new functionality
5. **Verify examples work** if you've added any

### Pull Request Template

When creating a pull request, please include:

```markdown
## What This Changes

Brief description of what this PR accomplishes.

## Why This Matters

Explain how this helps democratize data pipeline development or improves the user experience.

## Testing

- [ ] Added unit tests
- [ ] Added integration tests  
- [ ] Verified existing tests pass
- [ ] Tested manually with real data

## Documentation

- [ ] Updated relevant documentation
- [ ] Added examples for new features
- [ ] Verified examples work

## Checklist

- [ ] Code follows SQLFlow style guidelines
- [ ] Commit messages are descriptive
- [ ] No breaking changes (or clearly documented)
- [ ] Performance impact considered
```

### Review Process

1. **Automated Checks**: CI will run tests and code quality checks
2. **Maintainer Review**: A core maintainer will review your code
3. **Community Feedback**: Other contributors may provide input
4. **Iteration**: Address feedback and update your PR
5. **Merge**: Once approved, your changes become part of SQLFlow!

## 🎯 Areas Where We Need Help

### High-Impact Contributions

**New Connectors**
- SaaS platforms (Salesforce, HubSpot, Zendesk)
- NoSQL databases (MongoDB, Cassandra)  
- Cloud services (BigQuery, Snowflake, Redshift)
- Message queues (Kafka, RabbitMQ)

**Python UDFs for Common Use Cases**
- Data quality validation functions
- Business metric calculations (LTV, CAC, churn)
- Data cleaning and normalization
- Statistical analysis functions

**Performance Improvements**  
- Parallel processing optimizations
- Memory usage optimizations
- Query optimization helpers
- Connector performance improvements

**Developer Experience**
- Better error messages
- Improved debugging tools
- IDE integrations and plugins
- CLI usability improvements

**Documentation and Examples**
- Industry-specific pipeline examples
- Best practices guides
- Video tutorials and walkthroughs
- Troubleshooting guides

### Good First Issues

Look for issues tagged:
- `good first issue`: Perfect for newcomers
- `documentation`: Help improve our docs
- `connector`: Build new data source integrations
- `udf`: Create useful analysis functions
- `examples`: Add real-world pipeline examples

## 🚢 Release Process

SQLFlow follows a rapid release cycle to get improvements to users quickly.

### Release Types

- **Patch (0.x.Y)**: Bug fixes, released as needed
- **Minor (0.X.0)**: New features, every 2-4 weeks  
- **Major (1.0.0)**: When the API is stable (future)

### Release Workflow

1. **TestPyPI**: All releases first go to TestPyPI for verification
2. **Testing**: Community testing on TestPyPI version
3. **PyPI**: Stable release promoted to PyPI
4. **Documentation**: Release notes and migration guides

### Contributing to Releases

- Report bugs in TestPyPI releases
- Test new features with real data
- Provide feedback on breaking changes
- Help update migration documentation

## 🌟 Recognition and Community

### How We Recognize Contributors

- **Code contributions**: Listed in release notes
- **Documentation**: Highlighted in community updates  
- **Bug reports**: Credited in issue resolution
- **Community support**: Recognized in monthly highlights

### Community Guidelines

**Be Respectful**: Everyone is learning and contributing at their own pace

**Be Helpful**: Share knowledge and help newcomers feel welcome

**Be Constructive**: Provide actionable feedback and suggestions

**Be Patient**: Good software takes time, and everyone has different availability

## 🆘 Getting Help

### Development Questions

- **GitHub Discussions**: Ask questions and share ideas
- **GitHub Issues**: Report bugs and request features
- **Code Review**: Get feedback on your changes

### Community Support

- **Documentation**: Check our comprehensive guides
- **Examples**: Look at real-world pipeline examples
- **Stack Overflow**: Tag questions with `sqlflow`

### Direct Contact

For sensitive issues or private discussions, contact the maintainers directly through GitHub.

## 🎉 Thank You

Every contribution, no matter how small, helps make data analysis more accessible to everyone. Whether you're fixing a typo, adding a feature, or helping someone in the community, you're part of making SQLFlow better.

The founder's vision was to create a tool so simple that business analysts could build their own data pipelines. Your contributions help make that vision a reality.

**Ready to contribute?** 
1. Pick an issue that interests you
2. Join the discussion
3. Start coding!
4. Share your knowledge with others

Together, we're building the future of data pipeline development. Welcome to the team! 