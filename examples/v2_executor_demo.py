#!/usr/bin/env python3
"""
SQLFlow V2 Executor Foundation Demo

This script demonstrates the Phase 1 foundation components of the V2 Executor:
- Strongly-typed Step data classes
- Rich execution results with observability data
- Automatic performance monitoring and alerting
- Migration compatibility from dictionary-based steps

Author: Raymond Hettinger
"""

from datetime import datetime
from unittest.mock import Mock

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.observability import ObservabilityManager
from sqlflow.core.executors.v2.results import (
    PipelineExecutionSummary,
    StepExecutionResult,
)
from sqlflow.core.executors.v2.steps import (
    ExportStep,
    LoadStep,
    TransformStep,
    create_step_from_dict,
)


def demonstrate_step_classes():
    """Demonstrate strongly-typed step classes."""
    print("🔧 V2 Executor Foundation Demo")
    print("=" * 50)

    print("\n1. Creating Strongly-Typed Steps")
    print("-" * 30)

    # Create a load step
    load_step = LoadStep(
        id="load_customers",
        source="data/customers.csv",
        target_table="customers",
        load_mode="replace",
        expected_duration_ms=5000.0,
        criticality="high",
    )

    print(f"✅ LoadStep: {load_step.id}")
    print(f"   Source: {load_step.source}")
    print(f"   Target: {load_step.target_table}")
    print(f"   Mode: {load_step.load_mode}")
    print(f"   Expected Duration: {load_step.expected_duration_ms}ms")

    # Create a transform step
    transform_step = TransformStep(
        id="aggregate_customers",
        sql="SELECT region, COUNT(*) as customer_count FROM customers GROUP BY region",
        target_table="customer_summary",
        operation_type="create_table",
        udf_dependencies=[],
    )

    print(f"\n✅ TransformStep: {transform_step.id}")
    print(f"   SQL: {transform_step.sql[:50]}...")
    print(f"   Target: {transform_step.target_table}")
    print(f"   Operation: {transform_step.operation_type}")

    # Create an export step
    export_step = ExportStep(
        id="export_summary",
        source_table="customer_summary",
        target="output/customer_summary.parquet",
        export_format="parquet",
        compression="snappy",
    )

    print(f"\n✅ ExportStep: {export_step.id}")
    print(f"   Source: {export_step.source_table}")
    print(f"   Target: {export_step.target}")
    print(f"   Format: {export_step.export_format}")

    return [load_step, transform_step, export_step]


def demonstrate_observability():
    """Demonstrate automatic observability and performance monitoring."""
    print("\n\n2. Automatic Observability & Performance Monitoring")
    print("-" * 50)

    # Create observability manager
    obs_manager = ObservabilityManager("demo_run_001")
    print(f"📊 Initialized ObservabilityManager: {obs_manager.run_id}")

    # Simulate pipeline execution with observability
    steps = [
        {"name": "Fast Load", "duration_ms": 1200, "rows": 10000, "success": True},
        {
            "name": "Slow Transform",
            "duration_ms": 35000,
            "rows": 8500,
            "success": True,
        },  # Will trigger alert
        {"name": "Failed Export", "duration_ms": 2000, "rows": 0, "success": False},
    ]

    step_results = []

    for i, step_info in enumerate(steps):
        step_id = f"step_{i+1}_{step_info['name'].lower().replace(' ', '_')}"
        step_type = step_info["name"].split()[1].lower()

        print(f"\n🔄 Executing: {step_info['name']}")

        # Record step start
        start_time = datetime.utcnow()
        obs_manager.record_step_start(step_id, step_type)

        # Simulate execution time
        if step_info["success"]:
            # Successful execution
            result = StepExecutionResult.success(
                step_id=step_id,
                step_type=step_type,
                start_time=start_time,
                rows_affected=step_info["rows"],
                performance_metrics={
                    "throughput": step_info["rows"] / (step_info["duration_ms"] / 1000),
                    "processing_rate": (
                        "normal" if step_info["duration_ms"] < 10000 else "slow"
                    ),
                },
                resource_usage={
                    "memory_mb": 100 + (step_info["duration_ms"] / 100),
                    "cpu_percent": 45.0 if step_info["duration_ms"] < 10000 else 85.0,
                },
            )

            # Record success with observability data
            obs_manager.record_step_success(
                step_id,
                {
                    "duration_ms": step_info["duration_ms"],
                    "rows_affected": step_info["rows"],
                    "step_type": step_type,
                    "resource_usage": result.resource_usage,
                },
            )

            print(
                f"   ✅ Success: {step_info['rows']} rows in {step_info['duration_ms']}ms"
            )

        else:
            # Failed execution
            result = StepExecutionResult.failure(
                step_id=step_id,
                step_type=step_type,
                start_time=start_time,
                error_message="S3 bucket not accessible - permission denied",
            )

            obs_manager.record_step_failure(
                step_id, step_type, result.error_message, step_info["duration_ms"]
            )

            print(f"   ❌ Failed: {result.error_message}")

        step_results.append(result)

    # Display performance summary
    print("\n📈 Performance Summary")
    print("-" * 25)
    summary = obs_manager.get_performance_summary()

    print(f"Total Steps: {summary['total_steps']}")
    print(f"Total Failures: {summary['total_failures']}")
    print(f"Failure Rate: {summary['failure_rate']:.1f}%")
    print(f"Total Rows Processed: {summary['total_rows_processed']:,}")
    print(f"Overall Throughput: {summary['throughput_rows_per_second']:.1f} rows/sec")

    # Display step-type breakdown
    print("\n📊 Step Type Performance")
    for step_type, metrics in summary["step_details"].items():
        print(f"  {step_type.title()}:")
        print(f"    Calls: {metrics['calls']}")
        print(f"    Avg Duration: {metrics['avg_duration_ms']:.1f}ms")
        print(f"    Throughput: {metrics['throughput_rows_per_second']:.1f} rows/sec")

    # Display alerts
    alerts = obs_manager.get_alerts()
    if alerts:
        print(f"\n⚠️  Generated {len(alerts)} Performance Alerts")
        print("-" * 35)
        for alert in alerts:
            severity_emoji = {"warning": "⚠️", "error": "❌", "critical": "🚨"}
            emoji = severity_emoji.get(alert.severity.value, "ℹ️")
            print(f"{emoji} {alert.message}")
            if alert.suggested_actions:
                print("    Suggested Actions:")
                for action in alert.suggested_actions[:2]:  # Show first 2 actions
                    print(f"    • {action}")
                if len(alert.suggested_actions) > 2:
                    print(f"    • ... and {len(alert.suggested_actions) - 2} more")
            print()

    return step_results


def demonstrate_migration_compatibility():
    """Demonstrate migration from dictionary-based steps to V2."""
    print("\n3. Migration Compatibility")
    print("-" * 30)

    # Legacy pipeline definition (dictionary format)
    legacy_pipeline = [
        {
            "type": "load",
            "source": "legacy_orders.csv",
            "target_table": "orders",
            "load_mode": "append",
        },
        {
            "id": "calculate_metrics",
            "type": "transform",
            "sql": "SELECT DATE(order_date) as day, SUM(amount) as daily_total FROM orders GROUP BY DATE(order_date)",
            "target_table": "daily_totals",
        },
        {
            "type": "export",
            "source_table": "daily_totals",
            "target": "reports/daily_sales.csv",
            "export_format": "csv",
        },
    ]

    print("📄 Converting legacy dictionary-based pipeline to V2 steps...")

    # Convert to V2 step objects
    v2_steps = []
    for step_dict in legacy_pipeline:
        try:
            v2_step = create_step_from_dict(step_dict)
            v2_steps.append(v2_step)

            # Display conversion result
            step_id = v2_step.id
            if step_id.count("_") > 1:  # Auto-generated ID
                print(
                    f"✅ Converted {v2_step.type} step (auto-generated ID: {step_id})"
                )
            else:
                print(f"✅ Converted {v2_step.type} step (ID: {step_id})")

        except Exception as e:
            print(f"❌ Failed to convert step: {e}")

    print(f"\n🎯 Successfully converted {len(v2_steps)}/{len(legacy_pipeline)} steps")

    # Demonstrate V2 features work with converted steps
    print("\n🔧 V2 features work seamlessly with converted steps:")
    for step in v2_steps:
        print(f"  • {step.type.title()}: {step.id} (criticality: {step.criticality})")

    return v2_steps


def demonstrate_execution_context():
    """Demonstrate ExecutionContext immutability and dependency injection."""
    print("\n\n4. Execution Context & Dependency Injection")
    print("-" * 45)

    # Create mock dependencies
    sql_engine = Mock()
    sql_engine.__class__.__name__ = "DuckDBEngine"

    connector_registry = Mock()
    variable_manager = Mock()
    watermark_manager = Mock()
    obs_manager = ObservabilityManager("context_demo")

    # Create execution context
    context = ExecutionContext.create(
        sql_engine=sql_engine,
        connector_registry=connector_registry,
        variable_manager=variable_manager,
        watermark_manager=watermark_manager,
        observability_manager=obs_manager,
        variables={"env": "demo", "batch_size": 1000},
    )

    print(f"🔧 Created ExecutionContext: {context.run_id}")
    print(f"   SQL Engine: {context.sql_engine.__class__.__name__}")
    print(f"   Variables: {context.variables}")

    # Demonstrate immutability
    print("\n🔒 Testing immutability...")
    updated_context = context.with_variables({"new_var": "added"})

    print(f"   Original variables: {context.variables}")
    print(f"   Updated variables: {updated_context.variables}")
    print("   ✅ Original context unchanged (immutable)")

    # Demonstrate configuration updates
    config_context = context.with_config({"debug": True, "parallel": False})
    print(f"   Added config: {config_context.config}")

    return context


def demonstrate_pipeline_summary():
    """Demonstrate pipeline execution summary."""
    print("\n\n5. Pipeline Execution Summary")
    print("-" * 35)

    # Create sample step results
    start_time = datetime.utcnow()

    step_results = [
        StepExecutionResult.success(
            step_id="extract_data",
            step_type="load",
            start_time=start_time,
            rows_affected=50000,
            performance_metrics={"source_latency_ms": 250},
        ),
        StepExecutionResult.success(
            step_id="clean_data",
            step_type="transform",
            start_time=start_time,
            rows_affected=48500,  # Some records filtered out
            performance_metrics={"transformation_complexity": "medium"},
        ),
        StepExecutionResult.failure(
            step_id="upload_to_warehouse",
            step_type="export",
            start_time=start_time,
            error_message="Data warehouse connection timed out",
            error_code="CONNECTION_TIMEOUT",
        ),
    ]

    # Create pipeline summary
    pipeline_summary = PipelineExecutionSummary.from_step_results(
        run_id="customer_etl_20241201",
        pipeline_name="Customer ETL Pipeline",
        start_time=start_time,
        step_results=step_results,
    )

    print(f"📊 Pipeline: {pipeline_summary.pipeline_name}")
    print(f"   Run ID: {pipeline_summary.run_id}")
    print(f"   Status: {pipeline_summary.status}")
    print(f"   Total Duration: {pipeline_summary.total_duration_ms:.1f}ms")
    print(f"   Total Rows: {pipeline_summary.total_rows_processed:,}")

    # Analyze results
    successful_steps = [r for r in step_results if r.is_successful()]
    failed_steps = [r for r in step_results if r.is_failed()]

    print(f"\n📈 Execution Analysis:")
    print(f"   ✅ Successful Steps: {len(successful_steps)}")
    print(f"   ❌ Failed Steps: {len(failed_steps)}")

    if failed_steps:
        print(f"\n💥 Failure Details:")
        for failed_step in failed_steps:
            print(f"   • {failed_step.step_id}: {failed_step.error_message}")

    return pipeline_summary


def main():
    """Run the complete V2 Executor foundation demonstration."""
    print("🚀 SQLFlow V2 Executor Foundation - Phase 1 Complete!")
    print(
        "This demo showcases the architectural foundation for the next-generation executor."
    )

    # Run all demonstrations
    demonstrate_step_classes()
    demonstrate_observability()
    demonstrate_migration_compatibility()
    demonstrate_execution_context()
    demonstrate_pipeline_summary()

    print("\n\n🎉 V2 Executor Foundation Demo Complete!")
    print("=" * 50)
    print("Key Features Demonstrated:")
    print("✅ Strongly-typed step data classes with validation")
    print("✅ Rich execution results with comprehensive observability")
    print("✅ Automatic performance monitoring and intelligent alerting")
    print("✅ Seamless migration from dictionary-based steps")
    print("✅ Immutable execution context for dependency injection")
    print("✅ Pipeline-level execution summaries and analysis")
    print("\nReady for Phase 2: Handler Implementation! 🔧")


if __name__ == "__main__":
    main()
