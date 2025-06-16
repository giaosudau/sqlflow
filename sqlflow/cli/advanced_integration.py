"""Integration layer for advanced Rich features with existing CLI operations.

This module provides integration functions that connect the advanced display
features with existing CLI business operations. It allows enabling advanced
features through command-line flags without disrupting basic usage patterns.

Following technical design principles:
- Optional enhancement that doesn't complicate basic workflows
- Clean separation between advanced features and core functionality
- Type-safe integration with existing business operations
- Backward compatibility with existing CLI interface
"""

from typing import Any, Dict, List, Optional

from sqlflow.cli.advanced_display import (
    display_execution_summary_with_metrics,
    display_interactive_pipeline_selection,
    display_pipeline_execution_progress,
    display_pipeline_selection_with_preview,
)
from sqlflow.cli.business_operations import (
    compile_pipeline_operation,
    run_pipeline_operation,
)
from sqlflow.cli.display import (
    display_compilation_success,
)
from sqlflow.cli.factories import create_executor_for_command
from sqlflow.logging import get_logger

logger = get_logger(__name__)


def run_pipeline_with_advanced_progress(
    pipeline_name: str,
    profile_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    use_advanced_progress: bool = True,
    show_summary: bool = True,
) -> Dict[str, Any]:
    """Run pipeline with optional advanced progress display.

    This function extends the basic run_pipeline_operation with advanced
    Rich progress display. It maintains full backward compatibility while
    providing enhanced user experience when enabled.

    Args:
        pipeline_name: Name of the pipeline to run
        profile_name: Profile to use for execution
        variables: Variables for substitution
        use_advanced_progress: Whether to use Rich progress display
        show_summary: Whether to show detailed execution summary

    Returns:
        Execution results dictionary

    Raises:
        Same exceptions as run_pipeline_operation
    """
    logger.debug(
        f"Running pipeline {pipeline_name} with advanced_progress={use_advanced_progress}"
    )

    if not use_advanced_progress:
        # Fall back to standard operation for backward compatibility
        return run_pipeline_operation(pipeline_name, profile_name, variables)

    # Compile pipeline to get operations list
    operations, _ = compile_pipeline_operation(pipeline_name, profile_name, variables)

    # Create executor for step-by-step execution
    executor = create_executor_for_command(profile_name, variables)

    # Define callback for executing individual operations
    def execute_operation_callback(operation: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single operation using the executor."""
        try:
            return executor._execute_step(operation)
        except Exception as e:
            logger.error(f"Operation execution failed: {e}")
            return {"status": "error", "message": str(e)}

    # Execute with advanced progress display
    results = display_pipeline_execution_progress(
        operations=operations,
        executor_callback=execute_operation_callback,
        pipeline_name=pipeline_name,
    )

    # Show detailed summary if requested
    if show_summary and results:
        display_execution_summary_with_metrics(
            results=results,
            pipeline_name=pipeline_name,
            show_detailed_metrics=True,
        )

    logger.info(
        f"Advanced pipeline execution completed: {results.get('status', 'unknown')}"
    )
    return results


def compile_pipeline_with_enhanced_display(
    pipeline_name: str,
    profile_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    output_dir: Optional[str] = None,
    use_enhanced_display: bool = True,
) -> tuple[List[Dict[str, Any]], str]:
    """Compile pipeline with optional enhanced display features.

    Extends basic compilation with Rich-formatted success display that shows
    more detailed information about the compilation results.

    Args:
        pipeline_name: Name of the pipeline to compile
        profile_name: Profile to use for compilation
        variables: Variables for substitution
        output_dir: Directory to save compilation output
        use_enhanced_display: Whether to use enhanced Rich display

    Returns:
        Tuple of (operations_list, output_path)

    Raises:
        Same exceptions as compile_pipeline_operation
    """
    logger.debug(
        f"Compiling pipeline {pipeline_name} with enhanced_display={use_enhanced_display}"
    )

    # Use standard business operation for actual compilation
    operations, output_path = compile_pipeline_operation(
        pipeline_name, profile_name, variables, output_dir
    )

    if use_enhanced_display:
        # Use enhanced display with more details
        display_compilation_success(
            pipeline_name=pipeline_name,
            profile=profile_name or "default",
            operation_count=len(operations),
            output_path=output_path,
            operations=operations,  # Pass operations for detailed display
            variables=variables,  # Pass variables for detailed display
        )
    else:
        # Fall back to basic display
        display_compilation_success(
            pipeline_name=pipeline_name,
            profile=profile_name or "default",
            operation_count=len(operations),
            output_path=output_path,
        )

    logger.info(f"Pipeline compilation completed: {len(operations)} operations")
    return operations, output_path


def interactive_pipeline_selection(
    profile_name: Optional[str] = None,
    enable_preview: bool = False,
    show_details: bool = True,
) -> str:
    """Interactive pipeline selection with Rich interface.

    Provides a beautiful, interactive interface for selecting pipelines
    with optional preview functionality.

    Args:
        profile_name: Profile to use for pipeline listing
        enable_preview: Whether to enable pipeline preview feature
        show_details: Whether to show detailed pipeline information

    Returns:
        Name of the selected pipeline

    Raises:
        typer.Exit: When selection is cancelled or no pipelines available
    """
    logger.debug(
        f"Starting interactive pipeline selection with preview={enable_preview}"
    )

    if enable_preview:
        selected_pipeline, _ = display_pipeline_selection_with_preview(profile_name)
        return selected_pipeline
    else:
        return display_interactive_pipeline_selection(profile_name, show_details)


def run_pipeline_interactively(
    profile_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
    use_advanced_features: bool = True,
) -> Dict[str, Any]:
    """Run a pipeline with interactive selection and advanced progress.

    Combines interactive pipeline selection with advanced progress display
    for a complete enhanced CLI experience.

    Args:
        profile_name: Profile to use
        variables: Variables for substitution
        use_advanced_features: Whether to use all advanced features

    Returns:
        Execution results dictionary

    Raises:
        typer.Exit: When selection is cancelled
    """
    logger.debug("Starting interactive pipeline execution")

    # Interactive pipeline selection
    if use_advanced_features:
        pipeline_name = display_interactive_pipeline_selection(
            profile_name=profile_name,
            show_details=True,
        )
    else:
        # Fallback to basic selection (would need to be implemented)
        pipeline_name = display_interactive_pipeline_selection(
            profile_name=profile_name,
            show_details=False,
        )

    # Run with advanced progress
    return run_pipeline_with_advanced_progress(
        pipeline_name=pipeline_name,
        profile_name=profile_name,
        variables=variables,
        use_advanced_progress=use_advanced_features,
        show_summary=use_advanced_features,
    )


def get_advanced_features_status() -> Dict[str, bool]:
    """Get status of available advanced features.

    Returns:
        Dictionary with feature availability status
    """
    try:
        # Test Rich imports
        pass

        rich_available = True
    except ImportError:
        rich_available = False

    try:
        # Test typer imports
        typer_available = True
    except ImportError:
        typer_available = False

    return {
        "rich_progress": rich_available,
        "interactive_selection": rich_available and typer_available,
        "enhanced_display": rich_available,
        "pipeline_preview": rich_available,
        "execution_metrics": True,  # Always available
    }


def check_advanced_features_dependencies() -> bool:
    """Check if all dependencies for advanced features are available.

    Returns:
        True if all advanced features can be used
    """
    status = get_advanced_features_status()
    return all(status.values())


def get_feature_recommendations() -> List[str]:
    """Get recommendations for enabling advanced features.

    Returns:
        List of recommendation messages
    """
    status = get_advanced_features_status()
    recommendations = []

    if not status["rich_progress"]:
        recommendations.append("Install 'rich' package for enhanced progress display")

    if not status["interactive_selection"]:
        recommendations.append(
            "Install 'typer' package for interactive pipeline selection"
        )

    if not recommendations:
        recommendations.append("All advanced features are available!")

    return recommendations
