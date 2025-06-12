"""Standardized error handling for planner components.

Following Zen of Python:
- Simple is better than complex: Clear error hierarchy
- Explicit is better than implicit: Detailed error context
- Errors should never pass silently: Comprehensive error information
"""

from typing import Dict, List, Optional


class PlannerError(Exception):
    """Base class for all planner-related errors.
    
    Provides consistent error formatting and context management.
    Following Zen of Python: Explicit is better than implicit.
    """

    def __init__(self, message: str, context: Optional[Dict[str, any]] = None):
        self.message = message
        self.context = context or {}
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with context information."""
        formatted = self.message
        
        if self.context:
            context_parts = []
            for key, value in self.context.items():
                if isinstance(value, list) and len(value) > 0:
                    context_parts.append(f"{key}: {', '.join(map(str, value))}")
                elif value:
                    context_parts.append(f"{key}: {value}")
            
            if context_parts:
                formatted += f"\n\nContext:\n  - " + "\n  - ".join(context_parts)
        
        return formatted


class ValidationError(PlannerError):
    """Error for validation failures.
    
    Provides detailed information about what failed validation
    and where the issues were found.
    """

    def __init__(
        self,
        message: str,
        missing_variables: Optional[List[str]] = None,
        missing_tables: Optional[List[str]] = None,
        invalid_references: Optional[List[str]] = None,
        context_locations: Optional[Dict[str, List[str]]] = None,
    ):
        context = {}
        if missing_variables:
            context["missing_variables"] = missing_variables
        if missing_tables:
            context["missing_tables"] = missing_tables
        if invalid_references:
            context["invalid_references"] = invalid_references
        if context_locations:
            context["reference_locations"] = self._format_locations(context_locations)
        
        self.missing_variables = missing_variables or []
        self.missing_tables = missing_tables or []
        self.invalid_references = invalid_references or []
        self.context_locations = context_locations or {}
        
        super().__init__(message, context)

    def _format_locations(self, locations: Dict[str, List[str]]) -> List[str]:
        """Format location information for display."""
        formatted = []
        for item, locs in locations.items():
            formatted.append(f"{item} referenced at: {', '.join(locs)}")
        return formatted


class DependencyError(PlannerError):
    """Error for dependency resolution issues.
    
    Includes information about circular dependencies,
    missing dependencies, and dependency conflicts.
    """

    def __init__(
        self,
        message: str,
        cycles: Optional[List[List[str]]] = None,
        missing_dependencies: Optional[List[str]] = None,
        conflicting_dependencies: Optional[Dict[str, List[str]]] = None,
    ):
        context = {}
        if cycles:
            context["circular_dependencies"] = self._format_cycles(cycles)
        if missing_dependencies:
            context["missing_dependencies"] = missing_dependencies
        if conflicting_dependencies:
            context["conflicting_dependencies"] = self._format_conflicts(conflicting_dependencies)
        
        self.cycles = cycles or []
        self.missing_dependencies = missing_dependencies or []
        self.conflicting_dependencies = conflicting_dependencies or {}
        
        super().__init__(message, context)

    def _format_cycles(self, cycles: List[List[str]]) -> List[str]:
        """Format cycle information for display."""
        formatted = []
        for i, cycle in enumerate(cycles, 1):
            cycle_str = " → ".join(cycle)
            formatted.append(f"Cycle {i}: {cycle_str}")
        return formatted

    def _format_conflicts(self, conflicts: Dict[str, List[str]]) -> List[str]:
        """Format conflict information for display."""
        formatted = []
        for step, deps in conflicts.items():
            formatted.append(f"{step} conflicts with: {', '.join(deps)}")
        return formatted


class StepBuildError(PlannerError):
    """Error for step building failures.
    
    Provides information about which steps failed to build
    and why the building process failed.
    """

    def __init__(
        self,
        message: str,
        failed_steps: Optional[List[str]] = None,
        step_errors: Optional[Dict[str, str]] = None,
    ):
        context = {}
        if failed_steps:
            context["failed_steps"] = failed_steps
        if step_errors:
            context["step_errors"] = [f"{step}: {error}" for step, error in step_errors.items()]
        
        self.failed_steps = failed_steps or []
        self.step_errors = step_errors or {}
        
        super().__init__(message, context)


# Utility functions for error handling
def create_validation_error(
    missing_variables: List[str] = None,
    missing_tables: List[str] = None,
    invalid_references: List[str] = None,
    context_locations: Dict[str, List[str]] = None,
) -> ValidationError:
    """Create a standardized validation error with appropriate message.
    
    Following Zen of Python: Simple is better than complex.
    Provides a simple way to create validation errors with standard messaging.
    """
    message_parts = []
    
    if missing_variables:
        message_parts.append(f"Missing variables: {', '.join(missing_variables)}")
    
    if missing_tables:
        message_parts.append(f"Missing tables: {', '.join(missing_tables)}")
    
    if invalid_references:
        message_parts.append(f"Invalid references: {', '.join(invalid_references)}")
    
    if not message_parts:
        message = "Pipeline validation failed"
    else:
        message = "Pipeline validation failed: " + "; ".join(message_parts)
    
    return ValidationError(
        message=message,
        missing_variables=missing_variables,
        missing_tables=missing_tables,
        invalid_references=invalid_references,
        context_locations=context_locations,
    )


def create_dependency_error(
    cycles: List[List[str]] = None,
    missing_dependencies: List[str] = None,
    conflicting_dependencies: Dict[str, List[str]] = None,
) -> DependencyError:
    """Create a standardized dependency error with appropriate message.
    
    Following Zen of Python: Simple is better than complex.
    """
    message_parts = []
    
    if cycles:
        message_parts.append(f"Circular dependencies detected ({len(cycles)} cycles)")
    
    if missing_dependencies:
        message_parts.append(f"Missing dependencies: {', '.join(missing_dependencies)}")
    
    if conflicting_dependencies:
        message_parts.append(f"Conflicting dependencies for {len(conflicting_dependencies)} steps")
    
    if not message_parts:
        message = "Dependency resolution failed"
    else:
        message = "Dependency resolution failed: " + "; ".join(message_parts)
    
    return DependencyError(
        message=message,
        cycles=cycles,
        missing_dependencies=missing_dependencies,
        conflicting_dependencies=conflicting_dependencies,
    ) 