"""UDF query processing for DuckDB engine."""

import re
from typing import TYPE_CHECKING, Callable, Dict, List

from sqlflow.logging import get_logger

from ..constants import RegexPatterns

if TYPE_CHECKING:
    from ..engine import DuckDBEngine

logger = get_logger(__name__)


class UDFQueryProcessor:
    """Processes SQL queries to handle UDF references."""

    def __init__(self, engine: "DuckDBEngine", udfs: Dict[str, Callable]):
        """Initialize the UDF query processor.

        Args:
            engine: DuckDB engine instance
            udfs: Dictionary of UDFs to process
        """
        self.engine = engine
        self.udfs = udfs
        self.discovered_udfs: List[str] = []

    def process(self, query: str) -> str:
        """Process a query to handle UDF references.

        Args:
            query: SQL query with UDF references

        Returns:
            Processed query with UDF references replaced
        """
        if not self.udfs:
            logger.debug("No UDF replacements made in query")
            return query

        self._register_missing_udfs()
        processed_query = self._replace_udf_references(query)
        self._log_transformation(query, processed_query)
        return processed_query

    def _register_missing_udfs(self) -> None:
        """Register UDFs that are not yet registered with the engine."""
        udfs_to_register = self._identify_udfs_to_register()

        for udf_name, udf_function in udfs_to_register.items():
            flat_name = udf_name.split(".")[-1]
            try:
                logger.debug(f"Registering UDF {udf_name} with flat_name {flat_name}")
                self.engine.register_python_udf(udf_name, udf_function)
                self.engine.registered_udfs[flat_name] = udf_function
                logger.debug(f"Successfully registered UDF {flat_name}")
            except Exception as e:
                if "already created" in str(e):
                    logger.debug(
                        f"UDF {flat_name} already registered, updating reference"
                    )
                    self.engine.registered_udfs[flat_name] = udf_function
                else:
                    logger.error(f"Error registering UDF {flat_name}: {e}")

    def _identify_udfs_to_register(self) -> Dict[str, Callable]:
        """Identify UDFs that need to be registered.

        Returns:
            Dictionary of UDFs that need registration
        """
        udfs_to_register = {}

        for udf_name, udf_function in self.udfs.items():
            flat_name = udf_name.split(".")[-1]
            logger.debug(f"Processing UDF {udf_name} with flat_name {flat_name}")

            # Check UDF metadata
            udf_type = getattr(udf_function, "_udf_type", "scalar")
            output_schema = getattr(udf_function, "_output_schema", None)
            infer_schema = getattr(udf_function, "_infer_schema", False)

            logger.debug(f"UDF {flat_name} type: {udf_type}")
            logger.debug(f"UDF {flat_name} output_schema: {output_schema}")
            logger.debug(f"UDF {flat_name} infer_schema: {infer_schema}")

            # Check if UDF needs registration
            if self._should_register_udf(flat_name, udf_function):
                logger.debug(f"UDF {flat_name} needs registration")
                udfs_to_register[udf_name] = udf_function
            else:
                logger.debug(f"UDF {flat_name} already registered, skipping")

        return udfs_to_register

    def _should_register_udf(self, flat_name: str, udf_function: Callable) -> bool:
        """Check if a UDF should be registered.

        Args:
            flat_name: Flat name of the UDF
            udf_function: UDF function

        Returns:
            True if UDF should be registered
        """
        if flat_name not in self.engine.registered_udfs:
            return True

        # Check if the existing UDF has the right attributes
        existing_func = self.engine.registered_udfs[flat_name]
        existing_output_schema = getattr(existing_func, "_output_schema", None)
        existing_infer = getattr(existing_func, "_infer_schema", False)

        output_schema = getattr(udf_function, "_output_schema", None)
        infer_schema = getattr(udf_function, "_infer_schema", False)

        if (output_schema and not existing_output_schema) or (
            infer_schema and not existing_infer
        ):
            logger.debug(f"UDF {flat_name} has better metadata in newer version")
            return True

        return False

    def _replace_udf_references(self, query: str) -> str:
        """Replace UDF references in the query.

        Args:
            query: Original query

        Returns:
            Query with UDF references replaced
        """
        return re.sub(
            RegexPatterns.UDF_PYTHON_FUNC,
            self._replace_udf_call,
            query,
            flags=re.IGNORECASE,
        )

    def _replace_udf_call(self, match: re.Match) -> str:
        """Replace a single UDF call match.

        Args:
            match: Regex match object

        Returns:
            Replacement string
        """
        udf_name = match.group(1)  # Full UDF name like python_udfs.module.function
        udf_args = match.group(2)  # Arguments passed to UDF

        # Extract module components and flat name
        udf_parts = udf_name.split(".")
        flat_name = udf_parts[-1]

        # Record this UDF as discovered in the query
        if udf_name not in self.discovered_udfs:
            self.discovered_udfs.append(udf_name)

        logger.debug(
            f"replace_udf_call - udf_name: {udf_name}, flat_name: {flat_name}, args: {udf_args}"
        )

        if flat_name in self.engine.registered_udfs:
            udf_function = self.engine.registered_udfs[flat_name]
            udf_type = getattr(udf_function, "_udf_type", "scalar")

            if udf_type == "table":
                return self._handle_table_udf_replacement(match, flat_name, udf_args)
            else:
                # For scalar UDFs, just replace with flat name
                replacement = f"{flat_name}({udf_args})"
                logger.debug("Scalar UDF replacement: %s", replacement)
                return replacement
        else:
            logger.warning(f"UDF {flat_name} referenced in query but not registered")
            return match.group(0)

    def _handle_table_udf_replacement(
        self, match: re.Match, flat_name: str, udf_args: str
    ) -> str:
        """Handle replacement for table UDFs.

        Args:
            match: Regex match object
            flat_name: Flat name of the UDF
            udf_args: UDF arguments

        Returns:
            Replacement string
        """
        # For table UDFs in DuckDB, we need special handling
        # Check if this is a SELECT * FROM PYTHON_FUNC pattern or a scalar call pattern
        parent_context = match.string[
            max(0, match.start() - 20) : match.start()
        ].upper()

        if "FROM" in parent_context and "SELECT" in parent_context:
            # This is likely a FROM clause reference - use flat name directly
            replacement = f"{flat_name}({udf_args})"
            logger.debug("Table UDF in FROM clause: %s", replacement)
            return replacement
        else:
            # This is likely a scalar context - use flat name
            replacement = f"{flat_name}({udf_args})"
            logger.debug("Table UDF in scalar context: %s", replacement)
            return replacement

    def _log_transformation(self, original_query: str, processed_query: str) -> None:
        """Log the query transformation.

        Args:
            original_query: Original query
            processed_query: Processed query
        """
        if processed_query != original_query:
            logger.debug("UDFs discovered in query: %s", self.discovered_udfs)
            logger.debug("Original query: %s", original_query)
            logger.debug("Processed query: %s", processed_query)
            logger.info("Processed query with UDF replacements")
        else:
            logger.debug("No UDF replacements made in query")
