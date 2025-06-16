#!/usr/bin/env python3
"""
SQLFlow Pipeline Runner - Simple, Extensible, Maintenance-Free
Auto-discovers and validates all SQLFlow pipelines with clear reporting.
"""

import subprocess
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional


class TestResult(Enum):
    SUCCESS = "✅ SUCCESS"
    FAILED = "❌ FAILED"
    SKIPPED = "⏭️ SKIPPED"


@dataclass
class PipelineTest:
    """Single pipeline test configuration"""

    name: str
    pipeline_file: str
    expected_outputs: List[str]
    description: str

    @property
    def pipeline_path(self) -> str:
        return f"pipelines/{self.pipeline_file}"


@dataclass
class TestExecutionResult:
    """Result of running a single test"""

    test: PipelineTest
    result: TestResult
    execution_time: float
    error_message: Optional[str] = None
    outputs_found: List[str] = None

    def __post_init__(self):
        if self.outputs_found is None:
            self.outputs_found = []


class PipelineRunner:
    """Main pipeline runner - follows Single Responsibility Principle"""

    def __init__(self, base_dir: str = None):
        if base_dir is None:
            # Auto-detect: Docker context (/app) or local context
            if Path("/app").exists():
                base_dir = "/app"
            else:
                base_dir = str(Path(__file__).parent.parent)

        self.base_dir = Path(base_dir)
        self.output_dir = self.base_dir / "output"
        self.pipelines_dir = self.base_dir / "pipelines"

    def discover_pipelines(self) -> List[PipelineTest]:
        """Auto-discover all pipeline tests - Extensible by design"""
        pipeline_configs = {
            "01_postgres_basic_test.sf": PipelineTest(
                name="PostgreSQL Basic",
                pipeline_file="01_postgres_basic_test.sf",
                expected_outputs=["postgres_connection_test_results.csv"],
                description="PostgreSQL connectivity and parameter compatibility",
            ),
            "02_incremental_loading_test.sf": PipelineTest(
                name="Incremental Loading",
                pipeline_file="02_incremental_loading_test.sf",
                expected_outputs=[
                    "incremental_test_results.csv",
                    "customer_order_summary.csv",
                ],
                description="Watermark-based incremental loading",
            ),
            "03_s3_connector_test.sf": PipelineTest(
                name="S3 Connector",
                pipeline_file="03_s3_connector_test.sf",
                expected_outputs=[],  # S3 exports go to MinIO, verified separately
                description="S3 multi-format export functionality",
            ),
            "04_multi_connector_workflow.sf": PipelineTest(
                name="Multi-Connector Workflow",
                pipeline_file="04_multi_connector_workflow.sf",
                expected_outputs=[
                    "workflow_summary.csv",
                    "customer_segment_report.csv",
                    "monthly_sales_report.csv",
                ],
                description="Complete PostgreSQL → Transform → S3 pipeline",
            ),
            "05_resilient_postgres_test.sf": PipelineTest(
                name="Resilient Connectors",
                pipeline_file="05_resilient_postgres_test.sf",
                expected_outputs=[
                    "resilience_test_results.csv",
                    "resilience_stress_test_results.csv",
                ],
                description="Resilience patterns and failure recovery",
            ),
            "06_enhanced_s3_connector_demo.sf": PipelineTest(
                name="Enhanced S3 Features",
                pipeline_file="06_enhanced_s3_connector_demo.sf",
                expected_outputs=[
                    "enhanced_s3_test_results.csv",
                    "s3_performance_comparison.csv",
                ],
                description="Cost management and partition awareness",
            ),
        }

        # Auto-discover available pipelines (extensible)
        available_pipelines = []
        for pipeline_file in sorted(self.pipelines_dir.glob("*.sf")):
            config = pipeline_configs.get(pipeline_file.name)
            if config:
                available_pipelines.append(config)
            else:
                # Auto-create config for new pipelines
                available_pipelines.append(
                    PipelineTest(
                        name=pipeline_file.stem.replace("_", " ").title(),
                        pipeline_file=pipeline_file.name,
                        expected_outputs=[],
                        description=f"Auto-discovered pipeline: {pipeline_file.name}",
                    )
                )

        return available_pipelines

    def run_pipeline(self, test: PipelineTest) -> TestExecutionResult:
        """Execute single pipeline - follows Single Responsibility"""
        import time

        start_time = time.time()

        try:
            # Run SQLFlow pipeline inside Docker container where services are accessible
            # Extract pipeline name without .sf extension
            pipeline_name = Path(test.pipeline_file).stem
            cmd = [
                "docker",
                "compose",
                "exec",
                "-T",
                "sqlflow",
                "sqlflow",
                "pipeline",
                "run",
                pipeline_name,
                "--profile",
                "docker",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            execution_time = time.time() - start_time

            if result.returncode == 0:
                # Check expected outputs
                outputs_found = []
                for expected_output in test.expected_outputs:
                    output_path = self.output_dir / expected_output
                    if output_path.exists():
                        outputs_found.append(expected_output)

                # Success if pipeline ran and all expected outputs exist (or no outputs expected)
                if not test.expected_outputs or len(outputs_found) == len(
                    test.expected_outputs
                ):
                    return TestExecutionResult(
                        test,
                        TestResult.SUCCESS,
                        execution_time,
                        outputs_found=outputs_found,
                    )
                else:
                    missing = set(test.expected_outputs) - set(outputs_found)
                    return TestExecutionResult(
                        test,
                        TestResult.FAILED,
                        execution_time,
                        f"Missing outputs: {', '.join(missing)}",
                        outputs_found,
                    )
            else:
                return TestExecutionResult(
                    test,
                    TestResult.FAILED,
                    execution_time,
                    f"Pipeline failed: {result.stderr[-200:] if result.stderr else 'Unknown error'}",
                )

        except subprocess.TimeoutExpired:
            return TestExecutionResult(
                test,
                TestResult.FAILED,
                time.time() - start_time,
                "Pipeline timeout (5min)",
            )
        except Exception as e:
            return TestExecutionResult(
                test, TestResult.FAILED, time.time() - start_time, str(e)
            )

    def verify_s3_exports(self) -> bool:
        """Verify S3 exports in MinIO - Separated concern"""
        try:
            # Quick S3 verification using localhost since this runs on host
            import boto3

            s3 = boto3.client(
                "s3",
                endpoint_url="http://localhost:9000",  # Use localhost from host machine
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            response = s3.list_objects_v2(Bucket="sqlflow-demo", Prefix="exports/")
            return bool(response.get("Contents"))
        except:
            return False


class ServiceHealthChecker:
    """Service health verification - Single Responsibility"""

    @staticmethod
    def check_all_services() -> Dict[str, bool]:
        """Check all required services"""
        services = {}

        # PostgreSQL
        try:
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "postgres",
                    "pg_isready",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                ],
                capture_output=True,
                timeout=10,
            )
            services["postgres"] = result.returncode == 0
        except:
            services["postgres"] = False

        # MinIO
        try:
            result = subprocess.run(
                ["curl", "-sf", "http://localhost:9000/minio/health/live"],
                capture_output=True,
                timeout=10,
            )
            services["minio"] = result.returncode == 0
        except:
            services["minio"] = False

        # SQLFlow
        try:
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "sqlflow", "sqlflow", "--version"],
                capture_output=True,
                timeout=10,
            )
            services["sqlflow"] = result.returncode == 0
        except:
            services["sqlflow"] = False

        # Redis
        try:
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "redis", "redis-cli", "ping"],
                capture_output=True,
                timeout=10,
            )
            services["redis"] = result.returncode == 0
        except:
            services["redis"] = False

        return services


class ReportGenerator:
    """Test results reporting - Single Responsibility"""

    @staticmethod
    def print_summary(results: List[TestExecutionResult], services: Dict[str, bool]):
        """Generate concise test summary"""
        print(f"\n{'='*60}")
        print(f"SQLFlow Phase 2 Integration Demo Results")
        print(f"{'='*60}")

        # Service status
        print(f"\n🔧 Service Health:")
        for service, healthy in services.items():
            status = "✅ UP" if healthy else "❌ DOWN"
            print(f"  {service:12} {status}")

        # Test results
        print(f"\n🧪 Pipeline Tests:")
        success_count = 0
        for result in results:
            print(
                f"  {result.test.name:20} {result.result.value:12} ({result.execution_time:.1f}s)"
            )
            if result.result == TestResult.SUCCESS:
                success_count += 1
            elif result.error_message:
                print(f"    └─ {result.error_message}")

        # Summary
        total_tests = len(results)
        print(f"\n📊 Results: {success_count}/{total_tests} tests passed")

        if success_count == total_tests:
            print(f"🎉 ALL TESTS PASSED - Phase 2 implementation is working correctly!")
            return True
        else:
            print(
                f"⚠️  {total_tests - success_count} test(s) failed - See details above"
            )
            return False


def main():
    """Main execution - MVP focused"""
    print("SQLFlow Pipeline Validator - Phase 2 Integration Demo")

    # Initialize components
    runner = PipelineRunner()
    health_checker = ServiceHealthChecker()

    # Check services first
    services = health_checker.check_all_services()
    if not all(services.values()):
        print("❌ Services not ready - run 'docker compose up -d' first")
        return 1

    # Discover and run all pipeline tests
    tests = runner.discover_pipelines()
    print(f"\n🔍 Discovered {len(tests)} pipeline tests")

    results = []
    for i, test in enumerate(tests, 1):
        print(f"\n[{i}/{len(tests)}] Running {test.name}...")
        result = runner.run_pipeline(test)
        results.append(result)

        # Immediate feedback
        if result.result == TestResult.SUCCESS:
            print(f"  ✅ {test.name} completed successfully")
        else:
            print(f"  ❌ {test.name} failed: {result.error_message}")

    # Verify S3 exports
    if runner.verify_s3_exports():
        print(f"\n✅ S3 exports verified in MinIO bucket")

    # Generate final report
    success = ReportGenerator.print_summary(results, services)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
