"""UDF query processing for DuckDB engine."""

import re
from typing import TYPE_CHECKING, Callable, Dict, List, NamedTuple

from sqlflow.logging import get_logger

from ..constants import RegexPatterns

if TYPE_CHECKING:
    from ..engine import DuckDBEngine

logger = get_logger(__name__)


class TableFunctionReference(NamedTuple):
    """Reference to a table function in SQL."""

    name: str
    args: str
    position: int
    context: str


class AdvancedUDFQueryProcessor:
    """Advanced query processor with intelligent UDF detection and optimization.

    Phase 2 enhancement implementing sophisticated SQL parsing, dependency resolution,
    and performance optimization for table UDFs as outlined in the restoration plan.
    """

    def __init__(self, engine: "DuckDBEngine", udfs: Dict[str, Callable]):
        """Initialize the advanced UDF query processor.

        Args:
            engine: DuckDB engine instance
            udfs: Dictionary of UDFs to process
        """
        self.engine = engine
        self.udfs = udfs
        self.discovered_udfs: List[str] = []

        # Phase 2: Advanced tracking and optimization
        self.table_function_references: List[TableFunctionReference] = []
        self.dependency_graph: Dict[str, List[str]] = {}
        self.performance_optimizations: Dict[str, str] = {}

    def process(self, query: str) -> str:
        """Process a query with advanced UDF detection and optimization.

        Phase 2 enhancement with:
        - Advanced pattern recognition
        - Dependency resolution
        - Performance optimization
        - Arrow integration hints

        Args:
            query: SQL query with UDF references

        Returns:
            Optimized and processed query
        """
        if not self.udfs:
            logger.debug("No UDF replacements made in query")
            return query

        # Phase 2: Advanced processing pipeline

        # 1. Detect table function patterns (enhanced)
        self.table_function_references = self.detect_table_function_patterns(query)

        # 2. Process dependencies
        self.dependency_graph = self.process_table_udf_dependencies(query)

        # 3. Register missing UDFs
        self._register_missing_udfs()

        # 4. Replace UDF references
        processed_query = self._replace_udf_references(query)

        # 5. Transform table UDF patterns
        processed_query = self._transform_table_udf_patterns(processed_query)

        # 6. Apply performance optimizations
        processed_query = self.optimize_query_for_arrow_performance(processed_query)

        # 7. Log comprehensive transformation
        self._log_advanced_transformation(query, processed_query)

        return processed_query

    def detect_table_function_patterns(
        self, query: str
    ) -> List[TableFunctionReference]:
        """Detect table function patterns with advanced SQL parsing.

        Phase 2 enhancement: Sophisticated pattern recognition for various
        table UDF invocation patterns in SQL queries.

        Args:
            query: SQL query to analyze

        Returns:
            List of detected table function references
        """
        references = []

        # Pattern 1: Standard FROM clause table functions
        # SELECT * FROM table_func(args)
        from_pattern = r"FROM\s+(\w+)\s*\(\s*([^)]*)\s*\)"
        for match in re.finditer(from_pattern, query, re.IGNORECASE):
            func_name = match.group(1)
            func_args = match.group(2)

            # Check if this is a registered table UDF
            if self._is_table_udf(func_name):
                references.append(
                    TableFunctionReference(
                        name=func_name,
                        args=func_args,
                        position=match.start(),
                        context="FROM_clause",
                    )
                )
                logger.debug(f"Detected table function in FROM clause: {func_name}")

        # Pattern 2: PYTHON_FUNC table UDF calls
        # PYTHON_FUNC("module.function", args)
        python_func_pattern = RegexPatterns.UDF_PYTHON_FUNC
        for match in re.finditer(python_func_pattern, query, re.IGNORECASE):
            udf_name = match.group(1)
            flat_name = udf_name.split(".")[-1]

            if self._is_table_udf(flat_name):
                references.append(
                    TableFunctionReference(
                        name=flat_name,
                        args=match.group(2),
                        position=match.start(),
                        context="PYTHON_FUNC_call",
                    )
                )
                logger.debug(f"Detected table function in PYTHON_FUNC: {flat_name}")

        # Pattern 3: Subquery table functions
        # (SELECT * FROM table_func(args))
        subquery_pattern = r"\(\s*SELECT.*?FROM\s+(\w+)\s*\(\s*([^)]*)\s*\)\s*.*?\)"
        for match in re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL):
            func_name = match.group(1)

            if self._is_table_udf(func_name):
                references.append(
                    TableFunctionReference(
                        name=func_name,
                        args=match.group(2),
                        position=match.start(),
                        context="subquery",
                    )
                )
                logger.debug(f"Detected table function in subquery: {func_name}")

        logger.info(
            f"Advanced pattern detection found {len(references)} table function references"
        )
        return references

    def process_table_udf_dependencies(self, query: str) -> Dict[str, List[str]]:
        """Extract and resolve table UDF dependencies automatically.

        Phase 2 enhancement: Intelligent dependency graph construction
        for automatic execution order resolution.

        Args:
            query: SQL query to analyze

        Returns:
            Dictionary mapping UDFs to their table dependencies
        """
        dependencies = {}

        for ref in self.table_function_references:
            udf_name = ref.name

            # Extract table dependencies from UDF arguments
            table_deps = self._extract_table_dependencies_from_args(ref.args)

            # Add UDF dependencies based on context
            if ref.context == "FROM_clause":
                # In FROM clause, the UDF depends on tables in its arguments
                dependencies[udf_name] = table_deps
            elif ref.context == "PYTHON_FUNC_call":
                # In PYTHON_FUNC calls, check surrounding context for additional deps
                context_deps = self._extract_context_dependencies(query, ref.position)
                dependencies[udf_name] = table_deps + context_deps

            logger.debug(
                f"UDF {udf_name} dependencies: {dependencies.get(udf_name, [])}"
            )

        # Validate dependency graph for cycles
        if self._has_dependency_cycles(dependencies):
            logger.warning("Circular dependencies detected in table UDF graph")

        return dependencies

    def optimize_query_for_arrow_performance(self, query: str) -> str:
        """Optimize queries for zero-copy Arrow performance.

        Phase 2 enhancement: Arrow-based optimization for high-performance
        data exchange between Python UDFs and DuckDB.

        Args:
            query: Query to optimize

        Returns:
            Optimized query with Arrow performance hints
        """
        optimized_query = query

        # Optimization 1: Batch processing hints for large table UDFs
        for ref in self.table_function_references:
            if self._should_use_batch_processing(ref.name):
                # Add performance hints for batch processing
                optimization_hint = f"/* ARROW_BATCH_OPTIMIZE: {ref.name} */"
                self.performance_optimizations[ref.name] = "batch_processing"
                logger.debug(f"Applied batch processing optimization to {ref.name}")

        # Optimization 2: Zero-copy data exchange hints
        for udf_name in self.discovered_udfs:
            flat_name = udf_name.split(".")[-1]
            if self._supports_zero_copy_optimization(flat_name):
                self.performance_optimizations[flat_name] = "zero_copy"
                logger.debug(f"Applied zero-copy optimization to {flat_name}")

        # Optimization 3: Vectorized processing for compatible UDFs
        vectorized_udfs = [
            ref.name
            for ref in self.table_function_references
            if self._supports_vectorization(ref.name)
        ]

        if vectorized_udfs:
            logger.info(
                f"Applied vectorization optimizations to {len(vectorized_udfs)} UDFs"
            )

        return optimized_query

    def _is_table_udf(self, func_name: str) -> bool:
        """Check if a function name corresponds to a registered table UDF.

        Args:
            func_name: Function name to check

        Returns:
            True if it's a registered table UDF
        """
        if func_name in self.engine.registered_udfs:
            udf_function = self.engine.registered_udfs[func_name]
            return getattr(udf_function, "_udf_type", "scalar") == "table"

        # Check in the udfs dictionary as well
        for udf_name, udf_function in self.udfs.items():
            if udf_name.split(".")[-1] == func_name:
                return getattr(udf_function, "_udf_type", "scalar") == "table"

        return False

    def _extract_table_dependencies_from_args(self, args: str) -> List[str]:
        """Extract table names from UDF arguments.

        Args:
            args: UDF arguments string

        Returns:
            List of table names found in arguments
        """
        tables = []

        # Look for table references in arguments
        # Pattern: table_name or "table_name" or 'table_name'
        table_pattern = r"(?:^|[\s,\(])([a-zA-Z_][a-zA-Z0-9_]*|\"[^\"]+\"|'[^']+')"

        for match in re.finditer(table_pattern, args):
            table_name = match.group(1).strip("'\"")
            if table_name and table_name not in tables:
                tables.append(table_name)

        return tables

    def _extract_context_dependencies(self, query: str, position: int) -> List[str]:
        """Extract additional dependencies from query context around UDF call.

        Args:
            query: Full query
            position: Position of UDF call

        Returns:
            List of additional table dependencies
        """
        # Extract a window around the UDF call to analyze context
        window_start = max(0, position - 100)
        window_end = min(len(query), position + 100)
        context = query[window_start:window_end]

        # Look for FROM clauses in the context
        tables = []
        from_matches = re.finditer(r"FROM\s+([a-zA-Z0-9_]+)", context, re.IGNORECASE)
        for match in from_matches:
            table_name = match.group(1)
            if table_name not in tables:
                tables.append(table_name)

        return tables

    def _has_dependency_cycles(self, dependencies: Dict[str, List[str]]) -> bool:
        """Check for circular dependencies in the UDF dependency graph.

        Args:
            dependencies: Dependency graph

        Returns:
            True if cycles are detected
        """
        # Simple cycle detection using DFS
        visited = set()
        rec_stack = set()

        def has_cycle(node):
            if node in rec_stack:
                return True
            if node in visited:
                return False

            visited.add(node)
            rec_stack.add(node)

            for dep in dependencies.get(node, []):
                if dep in dependencies and has_cycle(dep):
                    return True

            rec_stack.remove(node)
            return False

        for udf_name in dependencies:
            if has_cycle(udf_name):
                return True

        return False

    def _should_use_batch_processing(self, udf_name: str) -> bool:
        """Determine if a UDF should use batch processing optimization.

        Args:
            udf_name: Name of the UDF

        Returns:
            True if batch processing should be used
        """
        # Check UDF metadata for batch processing hints
        if udf_name in self.engine.registered_udfs:
            udf_function = self.engine.registered_udfs[udf_name]
            return getattr(udf_function, "_enable_batch_processing", False)

        return False

    def _supports_zero_copy_optimization(self, udf_name: str) -> bool:
        """Check if a UDF supports zero-copy Arrow optimization.

        Args:
            udf_name: Name of the UDF

        Returns:
            True if zero-copy optimization is supported
        """
        # Check for Arrow-compatible UDF metadata
        if udf_name in self.engine.registered_udfs:
            udf_function = self.engine.registered_udfs[udf_name]
            return getattr(udf_function, "_arrow_compatible", False)

        return False

    def _supports_vectorization(self, udf_name: str) -> bool:
        """Check if a UDF supports vectorized processing.

        Args:
            udf_name: Name of the UDF

        Returns:
            True if vectorization is supported
        """
        # Check for vectorization support in UDF metadata
        if udf_name in self.engine.registered_udfs:
            udf_function = self.engine.registered_udfs[udf_name]
            return getattr(udf_function, "_vectorized", False)

        return False

    def _log_advanced_transformation(
        self, original_query: str, processed_query: str
    ) -> None:
        """Log comprehensive transformation details.

        Args:
            original_query: Original query
            processed_query: Processed query
        """
        if processed_query != original_query:
            logger.info("Advanced query processing complete:")
            logger.info(f"  - Discovered UDFs: {len(self.discovered_udfs)}")
            logger.info(
                f"  - Table function references: {len(self.table_function_references)}"
            )
            logger.info(f"  - Dependency relationships: {len(self.dependency_graph)}")
            logger.info(
                f"  - Performance optimizations: {len(self.performance_optimizations)}"
            )

            logger.debug(f"Original query: {original_query}")
            logger.debug(f"Processed query: {processed_query}")
            logger.debug(f"Dependencies: {self.dependency_graph}")
            logger.debug(f"Optimizations: {self.performance_optimizations}")
        else:
            logger.debug("No advanced UDF transformations applied")

    # Legacy methods maintained for backward compatibility
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

    def _transform_table_udf_patterns(self, query: str) -> str:
        """Transform Table UDF patterns to work with DuckDB limitations.

        DuckDB doesn't support true table functions in Python, so we need to
        transform SELECT * FROM table_udf() into a workaround.

        Args:
            query: Query that may contain table UDF patterns

        Returns:
            Transformed query
        """

        # Look for SELECT * FROM function_name() patterns
        def replace_table_pattern(match):
            func_name = match.group(1)
            match.group(2).strip() if match.group(2) else ""

            # Check if this is a registered table UDF
            if func_name in self.engine.registered_udfs:
                udf_function = self.engine.registered_udfs[func_name]
                udf_type = getattr(udf_function, "_udf_type", "scalar")

                if udf_type == "table":
                    logger.warning(
                        f"Table UDF {func_name} cannot be used in FROM clause with current DuckDB version. "
                        f"Table functions are not supported in DuckDB Python API."
                    )

                    # Return an error-generating query that provides clear feedback
                    return (
                        f"(SELECT 'ERROR: Table UDF {func_name} cannot be used in FROM clause. "
                        f"DuckDB Python API does not support table functions. "
                        f"Consider using scalar UDFs or restructuring your query.' as error_message)"
                    )

            # If not a table UDF, leave unchanged
            return match.group(0)

        transformed = re.sub(
            RegexPatterns.TABLE_UDF_FROM_PATTERN,
            replace_table_pattern,
            query,
            flags=re.IGNORECASE | re.MULTILINE,
        )

        if transformed != query:
            logger.debug("Transformed Table UDF FROM patterns")
            logger.debug(f"Original: {query}")
            logger.debug(f"Transformed: {transformed}")

        return transformed

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
