"""Runtime-related exceptions for SQLFlow V2 Executor.

These exceptions handle errors during runtime operations like variable substitution,
UDF execution, and security/permissions.
"""

from typing import Any, Dict, List, Optional

from .base import SQLFlowError


class VariableSubstitutionError(SQLFlowError):
    """Error during variable substitution."""

    def __init__(
        self,
        message: str,
        variable_name: Optional[str] = None,
        template_text: Optional[str] = None,
        missing_variables: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Check variable definitions and values",
            "Verify variable names are spelled correctly",
            "Ensure all required variables are provided",
        ]

        if variable_name:
            actions.append(f"Define variable: {variable_name}")

        if missing_variables:
            actions.append(f"Define missing variables: {', '.join(missing_variables)}")

        var_context = {}
        if variable_name:
            var_context["variable_name"] = variable_name
        if template_text:
            # Truncate long templates for readability
            display_text = (
                template_text[:200] + "..."
                if len(template_text) > 200
                else template_text
            )
            var_context["template_text"] = display_text
        if missing_variables:
            var_context["missing_variables"] = missing_variables

        if context:
            var_context.update(context)

        super().__init__(
            message=message,
            context=var_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.variable_name = variable_name
        self.missing_variables = missing_variables or []


class UDFExecutionError(SQLFlowError):
    """Error during User Defined Function execution."""

    def __init__(
        self,
        udf_name: str,
        message: str,
        udf_code: Optional[str] = None,
        input_args: Optional[List[Any]] = None,
        original_error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            f"Review UDF implementation: {udf_name}",
            "Check UDF input parameters and types",
            "Test UDF with sample data",
            "Verify UDF dependencies are available",
        ]

        if original_error:
            actions.append(f"Address underlying error: {type(original_error).__name__}")

        udf_context = {"udf_name": udf_name}
        if udf_code:
            # Truncate long code for readability
            display_code = udf_code[:300] + "..." if len(udf_code) > 300 else udf_code
            udf_context["udf_code"] = display_code
        if input_args is not None:
            udf_context["input_args"] = input_args
        if original_error:
            udf_context["original_error"] = str(original_error)
            udf_context["original_error_type"] = type(original_error).__name__

        if context:
            udf_context.update(context)

        super().__init__(
            message=f"UDF '{udf_name}' execution failed: {message}",
            context=udf_context,
            suggested_actions=actions,
            recoverable=True,
        )

        self.udf_name = udf_name
        self.original_error = original_error


class PermissionError(SQLFlowError):
    """Error due to insufficient permissions."""

    def __init__(
        self,
        resource: str,
        operation: str,
        message: str,
        user: Optional[str] = None,
        required_permissions: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            f"Ensure user has {operation} permissions for {resource}",
            "Check user authentication and authorization",
            "Contact administrator for permission changes",
        ]

        if required_permissions:
            actions.append(f"Required permissions: {', '.join(required_permissions)}")

        perm_context = {
            "resource": resource,
            "operation": operation,
        }
        if user:
            perm_context["user"] = user
        if required_permissions:
            perm_context["required_permissions"] = required_permissions

        if context:
            perm_context.update(context)

        super().__init__(
            message=f"Permission denied for {operation} on {resource}: {message}",
            context=perm_context,
            suggested_actions=actions,
            recoverable=False,  # Usually requires manual permission changes
        )

        self.resource = resource
        self.operation = operation
        self.user = user


class SecurityError(SQLFlowError):
    """Error related to security violations."""

    def __init__(
        self,
        message: str,
        security_check: Optional[str] = None,
        violation_type: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        actions = [
            "Review security policies and compliance requirements",
            "Check for unauthorized access attempts",
            "Verify data handling procedures",
            "Contact security team if needed",
        ]

        if security_check:
            actions.append(f"Address security check failure: {security_check}")

        security_context = {}
        if security_check:
            security_context["security_check"] = security_check
        if violation_type:
            security_context["violation_type"] = violation_type

        if context:
            security_context.update(context)

        super().__init__(
            message=f"Security violation: {message}",
            context=security_context,
            suggested_actions=actions,
            recoverable=False,  # Security errors typically require investigation
        )

        self.security_check = security_check
        self.violation_type = violation_type
