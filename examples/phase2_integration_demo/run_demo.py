#!/usr/bin/env python3
"""
SQLFlow Phase 2 Integration Demo - Unified Runner
Simple, maintainable, extensible pipeline validation system.

Usage:
    python run_demo.py              # Run full demo (start services + test pipelines)
    python run_demo.py --test-only  # Test pipelines only (assume services running)
    python run_demo.py --start-only # Start services only
    python run_demo.py --stop       # Stop all services
"""

import argparse
import sys
from pathlib import Path

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "scripts"))

from pipeline_runner import PipelineRunner, ReportGenerator
from service_manager import ServiceManager


class DemoRunner:
    """Main demo orchestrator - follows Facade pattern"""

    def __init__(self):
        self.service_manager = ServiceManager()
        self.pipeline_runner = PipelineRunner()
        self._s3_data_setup = False  # Track if S3 data has been setup

    def run_full_demo(self) -> bool:
        """Complete demo: start services + run pipelines"""
        print("🚀 SQLFlow Phase 2 Integration Demo - Full Run")
        print("=" * 60)

        # Start services
        if not self.service_manager.start_and_wait():
            print("❌ Failed to start services")
            return False

        # Setup S3 test data
        self._setup_s3_data()

        # Run pipeline tests
        return self._run_pipeline_tests()

    def run_test_only(self) -> bool:
        """Test pipelines only (assume services running)"""
        print("🧪 SQLFlow Phase 2 Integration Demo - Pipeline Tests Only")
        print("=" * 60)

        # Quick health check
        health_status = self.service_manager.get_health_status()
        if not all(health_status.values()):
            print("❌ Some services are not healthy:")
            for service, healthy in health_status.items():
                if not healthy:
                    print(f"  - {service}: DOWN")
            print("\nRun 'python run_demo.py --start-only' first")
            return False

        # Setup S3 test data (required for Enhanced S3 connector tests)
        self._setup_s3_data()

        return self._run_pipeline_tests()

    def start_services_only(self) -> bool:
        """Start services only"""
        print("🚀 Starting SQLFlow Services...")
        if self.service_manager.start_and_wait():
            # Setup S3 test data when services are started
            self._setup_s3_data()
            return True
        return False

    def stop_services(self) -> bool:
        """Stop all services"""
        print("🛑 Stopping SQLFlow Services...")
        return self.service_manager.stop()

    def _setup_s3_data(self):
        """Setup S3 test data if script exists (idempotent)"""
        # Skip if already setup to avoid duplicate work
        if self._s3_data_setup:
            print("📊 S3 test data already setup, skipping...")
            return

        try:
            import subprocess

            setup_script = Path(__file__).parent / "scripts" / "setup_s3_test_data.py"
            if setup_script.exists():
                print("📊 Setting up S3 test data...")
                result = subprocess.run(
                    [
                        "docker",
                        "compose",
                        "exec",
                        "-T",
                        "sqlflow",
                        "python3",
                        "scripts/setup_s3_test_data.py",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,  # Increased timeout for larger files
                )
                if result.returncode == 0:
                    print("✅ S3 test data setup completed")
                    self._s3_data_setup = True  # Mark as completed
                    # Print success output for debugging
                    if result.stdout and "🎉" in result.stdout:
                        print("   S3 bucket populated with test files")
                else:
                    print("⚠️ S3 test data setup failed, continuing with mock mode")
                    # Print error details for debugging
                    if result.stderr:
                        print(f"   Error: {result.stderr.strip()}")
                    if result.stdout:
                        print(f"   Output: {result.stdout.strip()}")
            else:
                print(f"⚠️ S3 setup script not found at {setup_script}")
        except subprocess.TimeoutExpired:
            print("⚠️ S3 setup timed out, continuing with existing data")
        except Exception as e:
            print(f"⚠️ S3 setup error: {e}")

    def _run_pipeline_tests(self) -> bool:
        """Run all pipeline tests and generate report"""
        # Discover and run tests
        tests = self.pipeline_runner.discover_pipelines()
        print(f"\n🔍 Discovered {len(tests)} pipeline tests")

        results = []
        for i, test in enumerate(tests, 1):
            print(f"\n[{i}/{len(tests)}] Running {test.name}...")
            result = self.pipeline_runner.run_pipeline(test)
            results.append(result)

            # Immediate feedback
            if result.result.value == "✅ SUCCESS":
                print(
                    f"  ✅ {test.name} completed successfully ({result.execution_time:.1f}s)"
                )
            else:
                print(f"  ❌ {test.name} failed: {result.error_message}")

        # Verify S3 exports
        if self.pipeline_runner.verify_s3_exports():
            print(f"\n✅ S3 exports verified in MinIO bucket")

        # Generate final report
        services = self.service_manager.get_health_status()
        success = ReportGenerator.print_summary(results, services)

        if success:
            self._show_access_info()

        return success

    def _show_access_info(self):
        """Show access information for successful demo"""
        print(f"\n🌐 Access Information:")
        print(f"  • pgAdmin:       http://localhost:8080")
        print(f"    Login:         admin@sqlflow.com / sqlflow123")
        print(f"  • MinIO Console: http://localhost:9001")
        print(f"    Login:         minioadmin / minioadmin")
        print(f"  • PostgreSQL:    localhost:5432 (demo/sqlflow/sqlflow123)")
        print(f"\n📁 Output files:   ./output/")
        print(f"🛑 Stop demo:      python run_demo.py --stop")


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="SQLFlow Phase 2 Integration Demo Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_demo.py              # Full demo (recommended)
  python run_demo.py --test-only  # Test pipelines only
  python run_demo.py --start-only # Start services only
  python run_demo.py --stop       # Stop services
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--test-only",
        action="store_true",
        help="Run pipeline tests only (assume services running)",
    )
    group.add_argument("--start-only", action="store_true", help="Start services only")
    group.add_argument("--stop", action="store_true", help="Stop all services")

    args = parser.parse_args()

    runner = DemoRunner()

    try:
        if args.test_only:
            success = runner.run_test_only()
        elif args.start_only:
            success = runner.start_services_only()
        elif args.stop:
            success = runner.stop_services()
        else:
            # Default: full demo
            success = runner.run_full_demo()

        return 0 if success else 1

    except KeyboardInterrupt:
        print("\n\n⚠️ Demo interrupted by user")
        print("🛑 Run 'python run_demo.py --stop' to clean up services")
        return 1
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
