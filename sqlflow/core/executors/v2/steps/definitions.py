"""Step definitions using Python's built-in strengths.

Following Raymond Hettinger's guidance:
- NamedTuple for immutable data with minimal overhead
- Simple validation functions rather than complex methods
- Clear, readable code over enterprise patterns
- "Flat is better than nested"
"""

from enum import Enum
from typing import Any, Dict, List, NamedTuple, Union


class LoadMode(Enum):
    """Load mode enumeration - explicit is better than implicit."""

    REPLACE = "replace"
    APPEND = "append"
    UPSERT = "upsert"


class ExportFormat(Enum):
    """Export format enumeration."""

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    XLSX = "xlsx"


# Simple, immutable step definitions using NamedTuple
class LoadStep(NamedTuple):
    """Immutable load step - simple and clear."""

    id: str
    source: str
    target_table: str
    mode: LoadMode = LoadMode.REPLACE
    upsert_keys: List[str] = []

    @property
    def step_type(self) -> str:
        return "load"


class TransformStep(NamedTuple):
    """Immutable transform step."""

    id: str
    sql: str
    target_table: str = ""
    dependencies: List[str] = []

    @property
    def step_type(self) -> str:
        return "transform"


class ExportStep(NamedTuple):
    """Immutable export step."""

    id: str
    source_table: str
    destination: str
    format: ExportFormat = ExportFormat.CSV

    @property
    def step_type(self) -> str:
        return "export"


class SourceStep(NamedTuple):
    """Immutable source step."""

    id: str
    name: str
    connector_type: str
    configuration: Dict[str, Any] = {}

    @property
    def step_type(self) -> str:
        return "source"


# Union type for all steps (Python 3.10+ style)
Step = Union[LoadStep, TransformStep, ExportStep, SourceStep]


# Simple validation functions - functional approach
def validate_load_step(step: LoadStep) -> None:
    """Validate load step - simple and clear."""
    if not step.id.strip():
        raise ValueError("Step ID cannot be empty")
    if not step.source.strip():
        raise ValueError("Source cannot be empty")
    if not step.target_table.strip():
        raise ValueError("Target table cannot be empty")
    if step.mode == LoadMode.UPSERT and not step.upsert_keys:
        raise ValueError("UPSERT mode requires upsert_keys")


def validate_transform_step(step: TransformStep) -> None:
    """Validate transform step."""
    if not step.id.strip():
        raise ValueError("Step ID cannot be empty")
    if not step.sql.strip():
        raise ValueError("SQL cannot be empty")


def validate_export_step(step: ExportStep) -> None:
    """Validate export step."""
    if not step.id.strip():
        raise ValueError("Step ID cannot be empty")
    if not step.source_table.strip():
        raise ValueError("Source table cannot be empty")
    if not step.destination.strip():
        raise ValueError("Destination cannot be empty")


def validate_source_step(step: SourceStep) -> None:
    """Validate source step."""
    if not step.id.strip():
        raise ValueError("Step ID cannot be empty")
    if not step.name.strip():
        raise ValueError("Source name cannot be empty")
    if not step.connector_type.strip():
        raise ValueError("Connector type cannot be empty")


# Simple factory functions - no complex pattern matching
def create_load_step(data: Dict[str, Any]) -> LoadStep:
    """Create load step from dictionary."""
    # Extract with sensible defaults
    mode_str = data.get("mode") or data.get("load_mode", "replace")
    mode = LoadMode(mode_str.lower()) if isinstance(mode_str, str) else mode_str

    step = LoadStep(
        id=data.get("id") or data.get("name") or "",
        source=data.get("source", "")
        or data.get("source_name", "")
        or data.get("uri", ""),
        target_table=data.get("target_table") or data.get("name") or "",
        mode=mode,
        upsert_keys=data.get("upsert_keys", []),
    )
    validate_load_step(step)
    return step


def create_transform_step(data: Dict[str, Any]) -> TransformStep:
    """Create transform step from dictionary."""
    step = TransformStep(
        id=data.get("id") or data.get("name") or "",
        sql=data.get("sql", "") or data.get("query", ""),
        target_table=data.get("target_table") or data.get("name") or "",
        dependencies=data.get("dependencies", []),
    )
    validate_transform_step(step)
    return step


def create_export_step(data: Dict[str, Any]) -> ExportStep:
    """Create export step from dictionary."""
    # Handle nested structure if present
    destination = data.get("destination", "")
    format_str = data.get("format", "csv")

    if not destination and "query" in data:
        query_data = data["query"]
        destination = query_data.get("destination_uri", "")
        format_str = query_data.get("type", "csv")

    format_enum = (
        ExportFormat(format_str.lower()) if isinstance(format_str, str) else format_str
    )

    step = ExportStep(
        id=data.get("id") or data.get("name") or "",
        source_table=data.get("source_table", ""),
        destination=destination,
        format=format_enum,
    )
    validate_export_step(step)
    return step


def create_source_step(data: Dict[str, Any]) -> SourceStep:
    """Create source step from dictionary."""
    configuration = (
        data.get("configuration", {}) or data.get("params", {}) or data.get("query", {})
    )

    step_id = data.get("id") or data.get("name") or ""
    step = SourceStep(
        id=step_id,
        name=data.get("name") or step_id,
        connector_type=data.get("connector_type", ""),
        configuration=configuration,
    )
    validate_source_step(step)
    return step


# Simple dispatch table - Pythonic!
STEP_FACTORIES = {
    "load": create_load_step,
    "transform": create_transform_step,
    "export": create_export_step,
    "source": create_source_step,
    "source_definition": create_source_step,
}


def create_step_from_dict(step_dict: Dict[str, Any]) -> Step:
    """Create typed step from dictionary - simple dispatch."""
    step_type = (step_dict.get("step") or step_dict.get("type", "")).lower()

    if not step_type:
        raise ValueError("Step must have a 'step' or 'type' field")

    factory = STEP_FACTORIES.get(step_type)
    if not factory:
        available = ", ".join(STEP_FACTORIES.keys())
        raise ValueError(f"Unknown step type: {step_type}. Available: {available}")

    return factory(step_dict)
