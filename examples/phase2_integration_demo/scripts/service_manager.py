#!/usr/bin/env python3
"""
Service Manager - Simple Docker Compose management
Follows Single Responsibility and Interface Segregation principles
"""

import subprocess
import sys
import time
from abc import ABC, abstractmethod
from typing import Dict, List


class DockerComposeInterface(ABC):
    """Interface for Docker Compose operations - Interface Segregation Principle"""

    @abstractmethod
    def start_services(self) -> bool:
        pass

    @abstractmethod
    def stop_services(self) -> bool:
        pass

    @abstractmethod
    def get_service_status(self) -> Dict[str, str]:
        pass


class ServiceHealthInterface(ABC):
    """Interface for health checking - Interface Segregation Principle"""

    @abstractmethod
    def wait_for_services(self, timeout: int = 60) -> bool:
        pass

    @abstractmethod
    def check_individual_service(self, service: str) -> bool:
        pass


class DockerComposeManager(DockerComposeInterface):
    """Docker Compose operations - Single Responsibility"""

    def __init__(self):
        self.compose_cmd = self._detect_compose_command()

    def _detect_compose_command(self) -> List[str]:
        """Auto-detect docker compose vs docker-compose"""
        try:
            subprocess.run(
                ["docker", "compose", "version"], capture_output=True, check=True
            )
            return ["docker", "compose"]
        except:
            try:
                subprocess.run(
                    ["docker-compose", "version"], capture_output=True, check=True
                )
                return ["docker-compose"]
            except:
                raise RuntimeError(
                    "Neither 'docker compose' nor 'docker-compose' found"
                )

    def start_services(self) -> bool:
        """Start all services"""
        try:
            cmd = self.compose_cmd + ["up", "-d", "--build"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            print(f"Failed to start services: {e}")
            return False

    def stop_services(self) -> bool:
        """Stop all services"""
        try:
            cmd = self.compose_cmd + ["down", "--volumes", "--remove-orphans"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            print(f"Failed to stop services: {e}")
            return False

    def get_service_status(self) -> Dict[str, str]:
        """Get status of all services"""
        try:
            cmd = self.compose_cmd + ["ps", "--format", "json"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                import json

                services = {}
                for line in result.stdout.strip().split("\n"):
                    if line:
                        service_info = json.loads(line)
                        services[service_info["Service"]] = service_info["State"]
                return services
            return {}
        except Exception:
            return {}


class ServiceHealthChecker(ServiceHealthInterface):
    """Health checking for individual services - Single Responsibility"""

    def __init__(self, compose_manager: DockerComposeManager):
        self.compose_manager = compose_manager

    def check_individual_service(self, service: str) -> bool:
        """Check health of individual service"""
        health_checks = {
            "postgres": self._check_postgres,
            "minio": self._check_minio,
            "sqlflow": self._check_sqlflow,
            "redis": self._check_redis,
        }

        checker = health_checks.get(service)
        return checker() if checker else False

    def wait_for_services(self, timeout: int = 60) -> bool:
        """Wait for all critical services to be ready"""
        critical_services = ["postgres", "minio", "sqlflow"]

        print("Waiting for services to be ready...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            all_ready = True
            for service in critical_services:
                if not self.check_individual_service(service):
                    all_ready = False
                    break

            if all_ready:
                print("✅ All services ready!")
                return True

            time.sleep(5)
            print("⏳ Still waiting...")

        print("❌ Services failed to start within timeout")
        return False

    def _check_postgres(self) -> bool:
        """Check PostgreSQL health"""
        try:
            cmd = self.compose_manager.compose_cmd + [
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                "postgres",
                "-d",
                "postgres",
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            return result.returncode == 0
        except:
            return False

    def _check_minio(self) -> bool:
        """Check MinIO health"""
        try:
            result = subprocess.run(
                ["curl", "-sf", "http://localhost:9000/minio/health/live"],
                capture_output=True,
                timeout=10,
            )
            return result.returncode == 0
        except:
            return False

    def _check_sqlflow(self) -> bool:
        """Check SQLFlow service health"""
        try:
            cmd = self.compose_manager.compose_cmd + [
                "exec",
                "-T",
                "sqlflow",
                "sqlflow",
                "--version",
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            return result.returncode == 0
        except:
            return False

    def _check_redis(self) -> bool:
        """Check Redis health"""
        try:
            cmd = self.compose_manager.compose_cmd + [
                "exec",
                "-T",
                "redis",
                "redis-cli",
                "ping",
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            return result.returncode == 0
        except:
            return False


class ServiceManager:
    """Facade pattern - Simple interface for all service operations"""

    def __init__(self):
        self.compose_manager = DockerComposeManager()
        self.health_checker = ServiceHealthChecker(self.compose_manager)

    def start_and_wait(self, timeout: int = 120) -> bool:
        """Start services and wait for them to be ready"""
        print("🚀 Starting Docker services...")

        if not self.compose_manager.start_services():
            print("❌ Failed to start services")
            return False

        print("✅ Services started, waiting for readiness...")
        return self.health_checker.wait_for_services(timeout)

    def stop(self) -> bool:
        """Stop all services"""
        print("🛑 Stopping Docker services...")
        return self.compose_manager.stop_services()

    def get_health_status(self) -> Dict[str, bool]:
        """Get health status of all services"""
        services = ["postgres", "minio", "sqlflow", "redis"]
        return {
            service: self.health_checker.check_individual_service(service)
            for service in services
        }


def main():
    """CLI interface for service management"""
    if len(sys.argv) < 2:
        print("Usage: python service_manager.py <start|stop|status>")
        return 1

    command = sys.argv[1].lower()
    manager = ServiceManager()

    if command == "start":
        success = manager.start_and_wait()
        return 0 if success else 1
    elif command == "stop":
        success = manager.stop()
        return 0 if success else 1
    elif command == "status":
        status = manager.get_health_status()
        print("\n🔧 Service Health Status:")
        for service, healthy in status.items():
            status_icon = "✅ UP" if healthy else "❌ DOWN"
            print(f"  {service:12} {status_icon}")
        return 0
    else:
        print(f"Unknown command: {command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
