"""Main entry point for SQLFlow CLI."""

import csv
import os
import random
import sys
from datetime import datetime, timedelta

import typer

from sqlflow import __version__
from sqlflow.cli import connect
from sqlflow.cli.commands.udf import app as udf_app
from sqlflow.cli.pipeline import pipeline_app
from sqlflow.logging import configure_logging, suppress_third_party_loggers
from sqlflow.project import Project

app = typer.Typer(
    help="SQLFlow - SQL-based data pipeline tool.",
    no_args_is_help=True,
)

app.add_typer(pipeline_app, name="pipeline")
app.add_typer(connect.app, name="connect")
app.add_typer(udf_app, name="udf")


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        typer.echo(f"SQLFlow version: {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", callback=version_callback, help="Show version and exit."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output with technical details"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Reduce output to essential information only"
    ),
):
    """SQLFlow CLI main entrypoint."""
    # Configure logging based on command line flags
    configure_logging(verbose=verbose, quiet=quiet)

    # Suppress noisy third-party logs
    suppress_third_party_loggers()


def generate_sample_data(data_dir: str):
    """Generate realistic sample data for immediate use."""
    os.makedirs(data_dir, exist_ok=True)

    # Generate customers data
    customers_file = os.path.join(data_dir, "customers.csv")
    names = [
        "Alice Johnson",
        "Bob Smith",
        "Maria Garcia",
        "David Chen",
        "Sarah Wilson",
        "James Brown",
        "Emma Davis",
        "Michael Taylor",
        "Lisa Anderson",
        "Daniel Martinez",
        "Jennifer White",
        "Christopher Lee",
        "Ashley Thompson",
        "Matthew Harris",
        "Jessica Clark",
        "Joshua Lewis",
        "Amanda Walker",
        "Andrew Hall",
        "Stephanie Allen",
        "Ryan Young",
    ]
    countries = [
        "US",
        "UK",
        "Canada",
        "Germany",
        "France",
        "Spain",
        "Italy",
        "Australia",
    ]
    cities = {
        "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "UK": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
        "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
        "Germany": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"],
        "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
        "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"],
        "Italy": ["Rome", "Milan", "Naples", "Turin", "Florence"],
        "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    }
    tiers = ["bronze", "silver", "gold", "platinum"]

    with open(customers_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "customer_id",
                "name",
                "email",
                "country",
                "city",
                "signup_date",
                "age",
                "tier",
            ]
        )

        for i in range(1, 1001):  # 1000 customers
            name = (
                random.choice(names)
                if i <= 20
                else f"{random.choice(names).split()[0]} {random.choice(['Miller', 'Moore', 'Jackson', 'Martin', 'Thompson'])}"
            )
            country = random.choice(countries)
            city = random.choice(cities[country])
            email = f"{name.lower().replace(' ', '.')}@example.com"
            signup_date = (
                datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d")
            age = random.randint(18, 70)
            tier = random.choices(tiers, weights=[40, 30, 20, 10])[
                0
            ]  # Weighted distribution

            writer.writerow([i, name, email, country, city, signup_date, age, tier])

    # Generate products data
    products_file = os.path.join(data_dir, "products.csv")
    product_names = [
        "Wireless Headphones",
        "Coffee Mug",
        "Running Shoes",
        "Laptop Stand",
        "Water Bottle",
        "Desk Lamp",
        "Bluetooth Speaker",
        "Phone Case",
        "Notebook",
        "Pen Set",
        "Monitor",
        "Keyboard",
        "Mouse",
        "Webcam",
        "Tablet",
        "Backpack",
        "Sunglasses",
        "Watch",
        "Wallet",
        "Charger Cable",
    ]
    categories = ["Electronics", "Home", "Sports", "Office", "Fashion", "Books"]
    suppliers = [
        "TechCorp",
        "HomeGoods",
        "SportsCorp",
        "OfficeMax",
        "FashionHub",
        "BookWorld",
    ]

    with open(products_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["product_id", "name", "category", "price", "stock_quantity", "supplier"]
        )

        for i in range(101, 601):  # 500 products
            name = (
                random.choice(product_names)
                if i <= 120
                else f"{random.choice(['Premium', 'Deluxe', 'Pro', 'Basic'])} {random.choice(product_names)}"
            )
            category = random.choice(categories)
            price = round(random.uniform(5.99, 199.99), 2)
            stock_quantity = random.randint(0, 500)
            supplier = random.choice(suppliers)

            writer.writerow([i, name, category, price, stock_quantity, supplier])

    # Generate orders data
    orders_file = os.path.join(data_dir, "orders.csv")
    statuses = ["completed", "pending", "cancelled", "shipped"]

    with open(orders_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "price",
                "order_date",
                "status",
            ]
        )

        for i in range(1, 5001):  # 5000 orders
            customer_id = random.randint(1, 1000)
            product_id = random.randint(101, 600)
            quantity = random.randint(1, 5)
            price = round(random.uniform(5.99, 199.99), 2)
            order_date = (
                datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d")
            status = random.choices(statuses, weights=[70, 15, 10, 5])[
                0
            ]  # Weighted distribution

            writer.writerow(
                [i, customer_id, product_id, quantity, price, order_date, status]
            )


def create_enhanced_pipelines(pipelines_dir: str):
    """Create multiple working pipelines with sample data."""

    # Basic example pipeline (updated)
    example_pipeline_path = os.path.join(pipelines_dir, "example.sf")
    with open(example_pipeline_path, "w") as f:
        f.write(
            """-- Basic Example Pipeline
-- Simple demonstration of SQLFlow capabilities

CREATE TABLE sample_data AS
SELECT * FROM VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob', 'bob@example.com'),
  (3, 'Charlie', 'charlie@example.com')
AS t(id, name, email);

CREATE TABLE processed_data AS
SELECT 
  id,
  name,
  email,
  UPPER(name) AS name_upper,
  LENGTH(name) AS name_length
FROM sample_data;

EXPORT
  SELECT * FROM processed_data
TO "output/example_results.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        )

    # Customer analytics pipeline
    customer_analytics_path = os.path.join(pipelines_dir, "customer_analytics.sf")
    with open(customer_analytics_path, "w") as f:
        f.write(
            """-- Customer Analytics Pipeline
-- Analyzes customer behavior and creates summaries

-- Load data using DuckDB's read_csv_auto function
CREATE TABLE customers AS
SELECT * FROM read_csv_auto('data/customers.csv');

CREATE TABLE orders AS
SELECT * FROM read_csv_auto('data/orders.csv');

CREATE TABLE products AS
SELECT * FROM read_csv_auto('data/products.csv');

-- Create customer summary by country and tier
CREATE TABLE customer_summary AS
SELECT 
    c.country,
    c.tier,
    COUNT(*) as customer_count,
    AVG(c.age) as avg_age,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.price * o.quantity), 0) as total_revenue
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.country, c.tier
ORDER BY total_revenue DESC;

-- Find top customers by spending
CREATE TABLE top_customers AS
SELECT 
    c.name,
    c.email,
    c.tier,
    c.country,
    COUNT(o.order_id) as order_count,
    SUM(o.price * o.quantity) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email, c.tier, c.country
ORDER BY total_spent DESC
LIMIT 20;

-- Export results
EXPORT
  SELECT * FROM customer_summary
TO "output/customer_summary.csv"
TYPE CSV
OPTIONS { "header": true };

EXPORT
  SELECT * FROM top_customers
TO "output/top_customers.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        )

    # Data quality pipeline
    data_quality_path = os.path.join(pipelines_dir, "data_quality.sf")
    with open(data_quality_path, "w") as f:
        f.write(
            """-- Data Quality Pipeline
-- Monitors data quality and creates reports

CREATE TABLE customers AS SELECT * FROM read_csv_auto('data/customers.csv');
CREATE TABLE orders AS SELECT * FROM read_csv_auto('data/orders.csv');
CREATE TABLE products AS SELECT * FROM read_csv_auto('data/products.csv');

-- Check for data quality issues
CREATE TABLE data_quality_report AS
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as missing_emails,
    COUNT(CASE WHEN country IS NULL OR country = '' THEN 1 END) as missing_countries,
    COUNT(CASE WHEN age < 0 OR age > 120 THEN 1 END) as invalid_ages
FROM customers

UNION ALL

SELECT 
    'orders' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN price IS NULL OR price <= 0 THEN 1 END) as invalid_prices,
    COUNT(CASE WHEN order_date IS NULL THEN 1 END) as missing_dates,
    COUNT(CASE WHEN quantity IS NULL OR quantity <= 0 THEN 1 END) as invalid_quantities
FROM orders

UNION ALL

SELECT 
    'products' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN price IS NULL OR price <= 0 THEN 1 END) as invalid_prices,
    COUNT(CASE WHEN stock_quantity IS NULL OR stock_quantity < 0 THEN 1 END) as invalid_stock,
    COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as missing_names
FROM products;

-- Export quality report
EXPORT
  SELECT * FROM data_quality_report
TO "output/data_quality_report.csv"
TYPE CSV
OPTIONS { "header": true };
"""
        )


@app.command()
def init(
    project_name: str = typer.Argument(..., help="Name of the project"),
    minimal: bool = typer.Option(
        False, "--minimal", help="Create minimal project structure without sample data"
    ),
    demo: bool = typer.Option(
        False, "--demo", help="Initialize project and run a demo pipeline immediately"
    ),
):
    """Initialize a new SQLFlow project with sample data and working pipelines."""
    project_dir = os.path.abspath(project_name)
    if os.path.exists(project_dir):
        typer.echo(f"Directory '{project_name}' already exists.")
        if not typer.confirm(
            "Do you want to initialize the project in this directory?"
        ):
            typer.echo("Project initialization cancelled.")
            raise typer.Exit(code=1)
    else:
        os.makedirs(project_dir)

    # Initialize basic project structure
    Project.init(project_dir, project_name)

    pipelines_dir = os.path.join(project_dir, "pipelines")
    data_dir = os.path.join(project_dir, "data")
    output_dir = os.path.join(project_dir, "output")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    if minimal:
        # Minimal mode: only create basic structure with simple example
        example_pipeline_path = os.path.join(pipelines_dir, "example.sf")
        with open(example_pipeline_path, "w") as f:
            f.write(
                """-- Example SQLFlow pipeline

CREATE TABLE sample_data AS
SELECT * FROM VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob', 'bob@example.com')
AS t(id, name, email);

EXPORT
  SELECT * FROM sample_data
TO "output/sample_results.csv"
TYPE CSV
OPTIONS { "header": true };
"""
            )

        typer.echo(f"✅ Minimal project '{project_name}' initialized successfully!")
        typer.echo("\nNext steps:")
        typer.echo(f"  cd {project_name}")
        typer.echo("  sqlflow pipeline run example")

    else:
        # Full mode: create sample data and multiple pipelines
        typer.echo("🚀 Creating sample data and working pipelines...")

        # Generate sample data
        generate_sample_data(data_dir)

        # Create enhanced pipelines
        create_enhanced_pipelines(pipelines_dir)

        # Create project README
        readme_path = os.path.join(project_dir, "README.md")
        with open(readme_path, "w") as f:
            f.write(
                f"""# {project_name} - SQLFlow Project

This project was created with SQLFlow and includes sample data and working pipelines.

## Quick Start

```bash
# Run customer analytics (works immediately!)
sqlflow pipeline run customer_analytics

# View results
cat output/customer_summary.csv
cat output/top_customers.csv

# Run data quality check
sqlflow pipeline run data_quality
cat output/data_quality_report.csv
```

## Available Pipelines

- `example` - Basic example with inline data
- `customer_analytics` - Customer behavior analysis
- `data_quality` - Data quality monitoring

## Sample Data

The project includes realistic sample datasets:

- `data/customers.csv` - 1,000 customer records
- `data/orders.csv` - 5,000 order records  
- `data/products.csv` - 500 product records

## Profiles

- `dev` - In-memory mode (fast, no persistence)
- `prod` - Persistent mode (saves to disk)

Use `--profile prod` to save results permanently.

## Project Structure

```
{project_name}/
├── data/           # Input data files
├── pipelines/      # SQLFlow pipeline files (.sf)
├── profiles/       # Environment configurations
└── output/         # Pipeline outputs
```

## Next Steps

1. Explore the sample pipelines in `pipelines/`
2. Modify them for your use case
3. Add your own data files to the `data/` directory
4. Create new pipelines with the `.sf` extension

For more information, visit: https://github.com/sqlflow/sqlflow
"""
            )

        typer.echo(f"✅ Project '{project_name}' initialized successfully!")
        typer.echo("📊 Created 1,000 customers, 5,000 orders, and 500 products")
        typer.echo(
            "🔧 Ready-to-run pipelines: example, customer_analytics, data_quality"
        )

        if demo:
            typer.echo("\n🎬 Running demo pipeline...")
            os.chdir(project_dir)

            # Import here to avoid circular imports
            from sqlflow.cli.pipeline import run_pipeline

            try:
                # Run the customer analytics pipeline
                run_pipeline(
                    "customer_analytics", vars=None, profile="dev", from_compiled=False
                )

                typer.echo("\n🎉 Demo completed! Check these files:")
                typer.echo("  📄 output/customer_summary.csv")
                typer.echo("  📄 output/top_customers.csv")

            except Exception as e:
                typer.echo(f"⚠️  Demo pipeline failed: {e}")
                typer.echo(
                    "You can run it manually with: sqlflow pipeline run customer_analytics"
                )
        else:
            typer.echo("\nNext steps:")
            typer.echo(f"  cd {project_name}")
            typer.echo(
                "  sqlflow pipeline run customer_analytics  # Immediate results!"
            )
            typer.echo("  cat output/customer_summary.csv")


@app.command("logging_status")
def show_logging_status():
    """Show the current logging status of all modules."""
    from sqlflow.logging import get_logging_status

    status = get_logging_status()

    typer.echo("SQLFlow Logging Status")
    typer.echo(f"Root level: {status['root_level']}")
    typer.echo("\nModule levels:")

    # Get sorted module names for consistent output
    module_names = sorted(status["modules"].keys())
    for name in module_names:
        info = status["modules"][name]
        typer.echo(f"  {name}: {info['level']}")


def cli():
    """Entry point for the command line."""
    # Fix for the help command issue with Typer
    if len(sys.argv) == 1 or "--help" in sys.argv or "-h" in sys.argv:
        print("SQLFlow - SQL-based data pipeline tool.")
        print("\nCommands:")
        print("  pipeline    Work with SQLFlow pipelines.")
        print("  connect     Manage and test connection profiles.")
        print("  udf         Manage Python User-Defined Functions.")
        print("  init        Initialize a new SQLFlow project.")
        print("  logging_status  Show the current logging configuration.")
        print("\nOptions:")
        print("  --version   Show version and exit.")
        print("  --quiet     Reduce output to essential information only.")
        print("  --verbose   Enable verbose output with technical details.")
        print("  --help      Show this message and exit.")

        if len(sys.argv) == 1:
            # No arguments provided, exit with standard help code
            return 0

        # Check if help is requested for a specific command
        if len(sys.argv) > 2 and ("--help" in sys.argv or "-h" in sys.argv):
            command = sys.argv[1]
            if command == "pipeline":
                print("\nPipeline Commands:")
                print("  list        List available pipelines.")
                print("  compile     Compile a pipeline.")
                print("  run         Run a pipeline.")
                print("  validate    Validate a pipeline.")
            elif command == "connect":
                print("\nConnect Commands:")
                print("  list        List available connections.")
                print("  test        Test a connection.")
            elif command == "udf":
                print("\nUDF Commands:")
                print("  list        List available Python UDFs.")
                print("  info        Show detailed information about a specific UDF.")
            elif command == "init":
                print("\nInit Options:")
                print("  --minimal   Create minimal project without sample data.")
                print("  --demo      Initialize and run demo pipeline immediately.")
            return 0

        return 0

    # For non-help commands, attempt to run the app
    try:
        app()
    except Exception as e:
        typer.echo(f"Error: {str(e)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(cli())
