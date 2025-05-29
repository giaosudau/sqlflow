"""Intelligent dependency resolution for table UDFs.

Phase 2 enhancement implementing sophisticated dependency graph construction,
cycle detection, and execution order optimization for table UDFs.
"""

import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from sqlflow.logging import get_logger

logger = get_logger(__name__)


class TableUDFDependencyResolver:
    """Intelligent dependency resolution for table UDFs.
    
    Phase 2 enhancement providing:
    - Advanced dependency extraction from SQL patterns
    - Cycle detection in UDF dependency graphs  
    - Optimal execution order resolution
    - Integration with SQLFlow planner
    """

    def __init__(self, udfs: Dict[str, Callable]):
        """Initialize the dependency resolver.
        
        Args:
            udfs: Dictionary of available UDFs
        """
        self.udfs = udfs
        self.dependency_cache: Dict[str, List[str]] = {}
        self.resolution_cache: Dict[str, List[str]] = {}
        
        logger.debug(f"Initialized dependency resolver with {len(udfs)} UDFs")

    def extract_table_dependencies(self, sql_query: str) -> List[str]:
        """Extract table dependencies from UDF SQL patterns.
        
        Phase 2 enhancement with sophisticated pattern recognition for:
        - Standard table references in FROM clauses
        - Subquery table dependencies
        - UDF parameter table references
        - CTEs and temporary table dependencies
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            List of table names that the query depends on
        """
        dependencies = []
        
        # Pattern 1: Standard FROM clause dependencies
        from_dependencies = self._extract_from_clause_tables(sql_query)
        dependencies.extend(from_dependencies)
        
        # Pattern 2: JOIN clause dependencies
        join_dependencies = self._extract_join_clause_tables(sql_query)
        dependencies.extend(join_dependencies)
        
        # Pattern 3: UDF parameter dependencies
        udf_dependencies = self._extract_udf_parameter_tables(sql_query)
        dependencies.extend(udf_dependencies)
        
        # Pattern 4: Subquery dependencies
        subquery_dependencies = self._extract_subquery_tables(sql_query)
        dependencies.extend(subquery_dependencies)
        
        # Pattern 5: CTE dependencies
        cte_dependencies = self._extract_cte_tables(sql_query)
        dependencies.extend(cte_dependencies)
        
        # Remove duplicates and built-in functions
        unique_dependencies = self._filter_and_deduplicate(dependencies)
        
        logger.debug(f"Extracted {len(unique_dependencies)} table dependencies: {unique_dependencies}")
        return unique_dependencies

    def validate_dependency_graph(self, dependencies: Dict[str, List[str]]) -> bool:
        """Validate UDF dependency graph for cycles and consistency.
        
        Args:
            dependencies: Dictionary mapping UDF names to their dependencies
            
        Returns:
            True if dependency graph is valid, False if cycles detected
        """
        logger.debug(f"Validating dependency graph with {len(dependencies)} nodes")
        
        # Check for cycles using DFS
        if self._has_cycles(dependencies):
            logger.error("Circular dependencies detected in UDF dependency graph")
            self._log_cycle_details(dependencies)
            return False
        
        # Check for missing dependencies
        missing_deps = self._find_missing_dependencies(dependencies)
        if missing_deps:
            logger.warning(f"Missing dependencies detected: {missing_deps}")
            # This is a warning, not an error - dependencies might be external tables
        
        logger.info("Dependency graph validation successful")
        return True

    def resolve_execution_order(self, udfs: Dict[str, Callable]) -> List[str]:
        """Resolve optimal UDF execution order based on dependencies.
        
        Phase 2 enhancement implementing topological sorting with:
        - Dependency-aware ordering
        - Parallel execution hints
        - Performance optimization considerations
        
        Args:
            udfs: Dictionary of UDFs to order
            
        Returns:
            List of UDF names in optimal execution order
        """
        logger.info(f"Resolving execution order for {len(udfs)} UDFs")
        
        # Build dependency graph for these UDFs
        udf_dependencies = {}
        for udf_name, udf_function in udfs.items():
            dependencies = self._get_udf_dependencies(udf_name, udf_function)
            udf_dependencies[udf_name] = dependencies
        
        # Validate the dependency graph
        if not self.validate_dependency_graph(udf_dependencies):
            logger.error("Cannot resolve execution order due to invalid dependency graph")
            # Return original order as fallback
            return list(udfs.keys())
        
        # Perform topological sort
        execution_order = self._topological_sort(udf_dependencies)
        
        logger.info(f"Resolved execution order: {execution_order}")
        return execution_order

    def _extract_from_clause_tables(self, sql_query: str) -> List[str]:
        """Extract table names from FROM clauses.
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            List of table names from FROM clauses
        """
        tables = []
        
        # Pattern: FROM table_name
        # Also handles: FROM schema.table_name
        from_pattern = r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)"
        
        for match in re.finditer(from_pattern, sql_query, re.IGNORECASE):
            table_name = match.group(1)
            if table_name and table_name not in tables:
                tables.append(table_name)
        
        return tables

    def _extract_join_clause_tables(self, sql_query: str) -> List[str]:
        """Extract table names from JOIN clauses.
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            List of table names from JOIN clauses
        """
        tables = []
        
        # Pattern: JOIN table_name
        # Handles: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, etc.
        join_pattern = r"\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+OUTER\s+|CROSS\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)"
        
        for match in re.finditer(join_pattern, sql_query, re.IGNORECASE):
            table_name = match.group(1)
            if table_name and table_name not in tables:
                tables.append(table_name)
        
        return tables

    def _extract_udf_parameter_tables(self, sql_query: str) -> List[str]:
        """Extract table names from UDF parameters.
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            List of table names referenced in UDF parameters
        """
        tables = []
        
        # Pattern 1: PYTHON_FUNC("module.function", table_name)
        python_func_pattern = r"PYTHON_FUNC\s*\(\s*['\"][^'\"]+['\"]\s*,\s*([a-zA-Z_][a-zA-Z0-9_]*)"
        
        for match in re.finditer(python_func_pattern, sql_query, re.IGNORECASE):
            table_name = match.group(1)
            if table_name and table_name not in tables:
                tables.append(table_name)
        
        # Pattern 2: Direct UDF calls with table parameters
        # udf_function(table_name, other_params)
        for udf_name in self.udfs:
            flat_name = udf_name.split(".")[-1]
            udf_call_pattern = fr"\b{re.escape(flat_name)}\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)"
            
            for match in re.finditer(udf_call_pattern, sql_query, re.IGNORECASE):
                table_name = match.group(1)
                if table_name and table_name not in tables:
                    tables.append(table_name)
        
        return tables

    def _extract_subquery_tables(self, sql_query: str) -> List[str]:
        """Extract table names from subqueries.
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            List of table names from subqueries
        """
        tables = []
        
        # Find subqueries (simplified pattern)
        subquery_pattern = r"\(\s*(SELECT.*?)\)"
        
        for match in re.finditer(subquery_pattern, sql_query, re.IGNORECASE | re.DOTALL):
            subquery = match.group(1)
            # Recursively extract dependencies from subquery
            subquery_deps = self.extract_table_dependencies(subquery)
            tables.extend(subquery_deps)
        
        return tables

    def _extract_cte_tables(self, sql_query: str) -> List[str]:
        """Extract table names from Common Table Expressions (CTEs).
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            List of table names from CTEs
        """
        tables = []
        
        # Pattern: WITH cte_name AS (SELECT ... FROM table_name ...)
        cte_pattern = r"\bWITH\s+[a-zA-Z_][a-zA-Z0-9_]*\s+AS\s*\(\s*(SELECT.*?)\)"
        
        for match in re.finditer(cte_pattern, sql_query, re.IGNORECASE | re.DOTALL):
            cte_query = match.group(1)
            # Recursively extract dependencies from CTE query
            cte_deps = self.extract_table_dependencies(cte_query)
            tables.extend(cte_deps)
        
        return tables

    def _filter_and_deduplicate(self, dependencies: List[str]) -> List[str]:
        """Filter out built-in functions and deduplicate dependencies.
        
        Args:
            dependencies: Raw list of dependencies
            
        Returns:
            Filtered and deduplicated list
        """
        # DuckDB built-in functions that are not table references
        builtin_functions = {
            "read_csv_auto", "read_csv", "read_parquet", "read_json",
            "information_schema", "pg_catalog", "main", "memory",
            "temp", "temporary", "pg_temp", "sqlite_temp_master"
        }
        
        # Filter and deduplicate
        filtered = []
        seen = set()
        
        for dep in dependencies:
            if dep and dep.lower() not in builtin_functions and dep not in seen:
                filtered.append(dep)
                seen.add(dep)
        
        return filtered

    def _has_cycles(self, dependencies: Dict[str, List[str]]) -> bool:
        """Check for cycles in dependency graph using DFS.
        
        Args:
            dependencies: Dependency graph
            
        Returns:
            True if cycles are detected
        """
        # Colors: 0=white (unvisited), 1=gray (visiting), 2=black (visited)
        colors = {node: 0 for node in dependencies}
        
        def dfs(node: str) -> bool:
            if colors[node] == 1:  # Gray - back edge found (cycle)
                return True
            if colors[node] == 2:  # Black - already processed
                return False
            
            colors[node] = 1  # Mark as gray (visiting)
            
            # Visit all dependencies
            for dep in dependencies.get(node, []):
                if dep in dependencies and dfs(dep):
                    return True
            
            colors[node] = 2  # Mark as black (visited)
            return False
        
        # Check all nodes
        for node in dependencies:
            if colors[node] == 0 and dfs(node):
                return True
        
        return False

    def _log_cycle_details(self, dependencies: Dict[str, List[str]]) -> None:
        """Log detailed information about detected cycles.
        
        Args:
            dependencies: Dependency graph with cycles
        """
        logger.error("Dependency cycle details:")
        for udf_name, deps in dependencies.items():
            if deps:
                logger.error(f"  {udf_name} depends on: {deps}")

    def _find_missing_dependencies(self, dependencies: Dict[str, List[str]]) -> Set[str]:
        """Find dependencies that are referenced but not defined.
        
        Args:
            dependencies: Dependency graph
            
        Returns:
            Set of missing dependency names
        """
        defined_udfs = set(dependencies.keys())
        referenced_deps = set()
        
        for deps in dependencies.values():
            referenced_deps.update(deps)
        
        # Missing dependencies are those referenced but not defined as UDFs
        # Note: External tables are not considered missing
        missing = referenced_deps - defined_udfs
        
        # Filter out external table references (these are expected)
        actual_missing = set()
        for dep in missing:
            # Check if this looks like a UDF name (has module structure)
            if "." in dep or dep in self.udfs:
                actual_missing.add(dep)
        
        return actual_missing

    def _get_udf_dependencies(self, udf_name: str, udf_function: Callable) -> List[str]:
        """Get dependencies for a specific UDF.
        
        Args:
            udf_name: Name of the UDF
            udf_function: UDF function object
            
        Returns:
            List of dependencies for this UDF
        """
        # Check cache first
        if udf_name in self.dependency_cache:
            return self.dependency_cache[udf_name]
        
        dependencies = []
        
        # Try to get dependencies from UDF metadata
        explicit_deps = getattr(udf_function, "_table_dependencies", None)
        if explicit_deps:
            dependencies.extend(explicit_deps)
        
        # Try to extract from UDF docstring if available
        docstring_deps = self._extract_deps_from_docstring(udf_function)
        dependencies.extend(docstring_deps)
        
        # Cache and return
        self.dependency_cache[udf_name] = dependencies
        return dependencies

    def _extract_deps_from_docstring(self, udf_function: Callable) -> List[str]:
        """Extract dependencies from UDF docstring.
        
        Args:
            udf_function: UDF function
            
        Returns:
            List of dependencies found in docstring
        """
        if not hasattr(udf_function, "__doc__") or not udf_function.__doc__:
            return []
        
        docstring = udf_function.__doc__
        dependencies = []
        
        # Look for dependency patterns in docstring
        # Pattern: "Depends on: table1, table2"
        dep_pattern = r"[Dd]epends?\s+on:?\s*([a-zA-Z0-9_,\s]+)"
        
        match = re.search(dep_pattern, docstring)
        if match:
            dep_list = match.group(1)
            # Split by comma and clean up
            for dep in dep_list.split(","):
                clean_dep = dep.strip()
                if clean_dep:
                    dependencies.append(clean_dep)
        
        return dependencies

    def _topological_sort(self, dependencies: Dict[str, List[str]]) -> List[str]:
        """Perform topological sort on dependency graph.
        
        Args:
            dependencies: Dependency graph
            
        Returns:
            Topologically sorted list of UDF names
        """
        # Calculate in-degrees
        in_degree = {node: 0 for node in dependencies}
        
        for node in dependencies:
            for dep in dependencies[node]:
                if dep in in_degree:
                    in_degree[dep] += 1
        
        # Find nodes with no incoming edges
        queue = [node for node, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # Process node with no dependencies
            node = queue.pop(0)
            result.append(node)
            
            # Update in-degrees of dependent nodes
            for dep in dependencies.get(node, []):
                if dep in in_degree:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        queue.append(dep)
        
        # Check if all nodes were processed (no cycles)
        if len(result) != len(dependencies):
            logger.warning("Topological sort incomplete - possible cycles remain")
            # Add remaining nodes
            for node in dependencies:
                if node not in result:
                    result.append(node)
        
        return result

    def get_dependency_statistics(self) -> Dict[str, Any]:
        """Get statistics about the dependency resolver.
        
        Returns:
            Dictionary with dependency resolver statistics
        """
        return {
            "total_udfs": len(self.udfs),
            "cached_dependencies": len(self.dependency_cache),
            "cached_resolutions": len(self.resolution_cache)
        } 