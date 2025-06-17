"""Handler registration for V2 Orchestrator.

This module handles the registration of step handlers,
separating registration concerns from main orchestration logic.

Following Andy Hunt & Dave Thomas' orthogonality principle:
Handler registration is a separate concern from execution orchestration.
"""

from sqlflow.logging import get_logger

logger = get_logger(__name__)


def ensure_handlers_registered():
    """Ensure all step handlers are properly registered.

    Following the Zen of Python: explicit is better than implicit.
    All handlers are explicitly registered here for clarity.
    """
    try:
        from sqlflow.core.executors.v2.handlers.export_handler import ExportStepHandler
        from sqlflow.core.executors.v2.handlers.factory import StepHandlerFactory
        from sqlflow.core.executors.v2.handlers.load_handler import LoadStepHandler
        from sqlflow.core.executors.v2.handlers.source_handler import (
            SourceDefinitionHandler,
        )
        from sqlflow.core.executors.v2.handlers.transform_handler import (
            TransformStepHandler,
        )

        StepHandlerFactory.register_handler("load", LoadStepHandler)
        StepHandlerFactory.register_handler("transform", TransformStepHandler)
        StepHandlerFactory.register_handler("export", ExportStepHandler)
        StepHandlerFactory.register_handler(
            "source_definition", SourceDefinitionHandler
        )

        logger.debug("Successfully registered all V2 step handlers")

    except ImportError as e:
        logger.warning(f"Could not register some handlers: {e}")
