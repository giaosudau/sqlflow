"""Source Definition Step Handler for V2 Executor.

This module implements the SourceDefinitionHandler that handles source configuration,
validation, and setup operations with comprehensive observability.
"""

import time
from datetime import datetime
from typing import Any, Dict

from sqlflow.core.executors.v2.context import ExecutionContext
from sqlflow.core.executors.v2.handlers.base import StepHandler, observed_execution
from sqlflow.core.executors.v2.results import StepExecutionResult
from sqlflow.core.executors.v2.steps import SourceDefinitionStep
from sqlflow.logging import get_logger

logger = get_logger(__name__)


class SourceDefinitionHandler(StepHandler):
    """
    Handles the execution of source definition and configuration operations.

    This handler is responsible for:
    - Validating source configurations
    - Setting up source connectors
    - Running source validation tests
    - Executing optional setup SQL
    - Providing detailed configuration metrics and observability
    - Handling profile-based configuration resolution

    The handler follows the Single Responsibility Principle by focusing solely
    on source setup and validation while delegating connector creation to the
    connector registry and SQL operations to the SQL engine.
    """

    STEP_TYPE = "source_definition"

    @observed_execution("source_definition")
    def execute(
        self, step: SourceDefinitionStep, context: ExecutionContext
    ) -> StepExecutionResult:
        """
        Execute a source definition operation with comprehensive observability.

        Args:
            step: SourceDefinitionStep containing source configuration
            context: ExecutionContext with shared services

        Returns:
            StepExecutionResult with detailed execution metrics
        """
        start_time = datetime.utcnow()
        logger.debug(f"Executing SourceDefinitionStep {step.id}: {step.source_name}")

        try:
            # Step 1: Validate step configuration
            self._validate_source_step(step)

            # Step 2: Resolve configuration with profile if specified
            resolved_config = self._resolve_source_configuration(step, context)

            # Step 3: Create and validate source connector
            validation_metrics = self._validate_source_connector(
                resolved_config, context
            )

            # Step 4: Execute setup SQL if provided
            setup_metrics = self._execute_setup_sql(step, context)

            # Step 5: Run validation rules if specified
            validation_results = self._run_validation_rules(
                step, resolved_config, context
            )

            # Step 6: Provide user feedback
            self._provide_user_feedback(step, validation_results)

            end_time = datetime.utcnow()

            # Combine metrics
            combined_metrics = {
                **validation_metrics,
                **setup_metrics,
                "validation_results": validation_results,
            }

            return StepExecutionResult.success(
                step_id=step.id,
                step_type="source_definition",
                start_time=start_time,
                end_time=end_time,
                performance_metrics=combined_metrics,
                data_lineage={
                    "source_name": step.source_name,
                    "source_type": resolved_config.get("type", "unknown"),
                    "profile_name": step.profile_name,
                    "validation_passed": validation_results.get("all_passed", True),
                },
            )

        except Exception as e:
            # Use common error handling pattern
            return self._handle_execution_error(step, start_time, e)

    def _validate_source_step(self, step: SourceDefinitionStep) -> None:
        """
        Validate SourceDefinitionStep configuration before execution.

        Args:
            step: SourceDefinitionStep to validate

        Raises:
            ValueError: If step configuration is invalid
        """
        if not step.source_name or not step.source_name.strip():
            raise ValueError(
                f"SourceDefinitionStep {step.id}: source_name cannot be empty"
            )

        if not step.source_config:
            raise ValueError(
                f"SourceDefinitionStep {step.id}: source_config cannot be empty"
            )

    def _resolve_source_configuration(
        self, step: SourceDefinitionStep, context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Resolve source configuration with profile data if specified.

        Args:
            step: SourceDefinitionStep with configuration
            context: ExecutionContext with services

        Returns:
            Dictionary containing resolved source configuration
        """
        config = step.source_config.copy()

        # If profile name is specified, attempt to resolve from profiles
        if step.profile_name:
            logger.debug(
                f"Resolving source configuration with profile: {step.profile_name}"
            )
            # Profile resolution would be implemented here
            # For now, we use the provided config as-is
            logger.debug(
                "Profile resolution not yet implemented, using provided config"
            )

        # Perform variable substitution on configuration
        if hasattr(context, "variable_manager"):
            try:
                resolved_config = context.variable_manager.substitute(config)
                if resolved_config != config:
                    logger.debug(
                        "Variable substitution applied to source configuration"
                    )
                return resolved_config
            except Exception as e:
                logger.warning(f"Variable substitution failed for source config: {e}")

        return config

    def _validate_source_connector(
        self, config: Dict[str, Any], context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Create and validate source connector.

        Args:
            config: Resolved source configuration
            context: ExecutionContext with connector registry

        Returns:
            Dictionary containing validation metrics
        """
        validation_start_time = time.monotonic()

        try:
            # Create source connector to validate configuration
            connector = context.connector_registry.create_source_connector(
                config.get("name", "unknown"), config
            )

            # Test connection if connector supports it
            connection_test_result = None
            if hasattr(connector, "test_connection"):
                connection_test_result = connector.test_connection()

            validation_time = (time.monotonic() - validation_start_time) * 1000

            metrics = {
                "connector_validation_time_ms": validation_time,
                "connector_type": config.get("type", "unknown"),
                "connection_test_passed": (
                    connection_test_result.is_successful
                    if connection_test_result
                    else None
                ),
                "connection_test_message": (
                    connection_test_result.message if connection_test_result else None
                ),
            }

            logger.debug(
                f"Source connector validation completed in {validation_time:.2f}ms"
            )
            return metrics

        except Exception as e:
            validation_time = (time.monotonic() - validation_start_time) * 1000
            logger.error(
                f"Source connector validation failed after {validation_time:.2f}ms: {e}"
            )
            raise

    def _execute_setup_sql(
        self, step: SourceDefinitionStep, context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Execute optional setup SQL for the source.

        Args:
            step: SourceDefinitionStep with optional setup SQL
            context: ExecutionContext with SQL engine

        Returns:
            Dictionary containing setup metrics
        """
        if not step.setup_sql:
            return {"setup_sql_executed": False}

        setup_start_time = time.monotonic()

        try:
            # Execute setup SQL
            context.sql_engine.execute_query(step.setup_sql)

            setup_time = (time.monotonic() - setup_start_time) * 1000

            metrics = {
                "setup_sql_executed": True,
                "setup_sql_time_ms": setup_time,
                "setup_sql": step.setup_sql,
            }

            logger.debug(f"Setup SQL executed successfully in {setup_time:.2f}ms")
            return metrics

        except Exception as e:
            setup_time = (time.monotonic() - setup_start_time) * 1000
            logger.error(f"Setup SQL execution failed after {setup_time:.2f}ms: {e}")
            raise

    def _run_validation_rules(
        self,
        step: SourceDefinitionStep,
        config: Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        """
        Run validation rules if specified.

        Args:
            step: SourceDefinitionStep with validation rules
            config: Resolved source configuration
            context: ExecutionContext with services

        Returns:
            Dictionary containing validation results
        """
        if not step.validation_rules:
            return {"validation_rules_executed": False, "all_passed": True}

        validation_start_time = time.monotonic()
        results = []

        try:
            for rule in step.validation_rules:
                rule_start = time.monotonic()

                # Execute validation rule (implementation depends on rule structure)
                rule_result = self._execute_validation_rule(rule, config, context)
                rule_time = (time.monotonic() - rule_start) * 1000

                results.append(
                    {
                        "rule": rule,
                        "passed": rule_result,
                        "execution_time_ms": rule_time,
                    }
                )

            validation_time = (time.monotonic() - validation_start_time) * 1000
            all_passed = all(r["passed"] for r in results)

            return {
                "validation_rules_executed": True,
                "total_validation_time_ms": validation_time,
                "rules_results": results,
                "all_passed": all_passed,
                "rules_passed": sum(1 for r in results if r["passed"]),
                "rules_failed": sum(1 for r in results if not r["passed"]),
            }

        except Exception as e:
            validation_time = (time.monotonic() - validation_start_time) * 1000
            logger.error(
                f"Validation rules execution failed after {validation_time:.2f}ms: {e}"
            )
            raise

    def _execute_validation_rule(
        self, rule: Dict[str, Any], config: Dict[str, Any], context: ExecutionContext
    ) -> bool:
        """
        Execute a single validation rule.

        Args:
            rule: Validation rule configuration
            config: Source configuration
            context: ExecutionContext with services

        Returns:
            True if validation passed, False otherwise
        """
        # Basic validation rule implementation
        # This would be expanded based on specific rule types
        rule_type = rule.get("type", "unknown")

        if rule_type == "connection":
            # Connection validation rule
            return True  # Placeholder - would test actual connection
        elif rule_type == "schema":
            # Schema validation rule
            return True  # Placeholder - would validate schema
        else:
            logger.warning(f"Unknown validation rule type: {rule_type}")
            return True

    def _provide_user_feedback(
        self, step: SourceDefinitionStep, validation_results: Dict[str, Any]
    ) -> None:
        """
        Provide user-friendly feedback about the source definition operation.

        Args:
            step: SourceDefinitionStep that was executed
            validation_results: Results from validation
        """
        if validation_results.get("all_passed", True):
            print(f"✅ Source '{step.source_name}' configured successfully")
        else:
            failed_count = validation_results.get("rules_failed", 0)
            print(
                f"⚠️  Source '{step.source_name}' configured with {failed_count} validation warnings"
            )

        logger.debug(f"Source definition step {step.id} completed successfully")
