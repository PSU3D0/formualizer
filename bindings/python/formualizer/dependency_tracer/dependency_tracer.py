"""
Dependency Tracer for Formula Dependencies

This module provides a robust, resolver-agnostic system for tracing formula dependencies
using an ABC-based architecture with intelligent caching.

Key Features:
- Abstract resolver interface for flexible data source integration
- Cached formula parsing and reference resolution
- Precedent and dependent tracing with cycle detection
- Cache management with selective invalidation
- Functional programming patterns for composable operations

Example Usage:
    ```python
    from formualizer.dependency_tracer import DependencyTracer, JsonResolver

    # Create resolver and tracer
    resolver = JsonResolver(workbook_data)
    tracer = DependencyTracer(resolver)

    # Trace precedents (what this formula depends on)
    precedents = tracer.trace_precedents("Sheet1!A1")

    # Trace dependents (what depends on this cell)
    dependents = tracer.trace_dependents("Sheet1!B2")

    # Build full dependency graph
    graph = tracer.build_dependency_graph()
    ```
"""

from abc import ABC, abstractmethod
from typing import Dict, Set, List, Optional, Union, Tuple, Any, Iterator, NamedTuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import weakref
from functools import lru_cache
import logging

try:
    from formualizer import parse, ASTNode, CellRef, RangeRef
    from formualizer.visitor import collect_references, walk_ast, VisitControl
except ImportError:
    # For standalone testing
    pass

logger = logging.getLogger(__name__)


class PrecedentInfo(NamedTuple):
    """Information about a precedent with evaluation order."""

    ref: Union[CellRef, RangeRef]
    order: int


@dataclass
class DependencyInfo:
    """Information about a cell's dependencies."""

    cell: CellRef
    formula: Optional[str] = None
    precedents: Set[Union[CellRef, RangeRef]] = field(default_factory=set)
    dependents: Set[CellRef] = field(default_factory=set)
    ast: Optional[ASTNode] = None
    last_updated: Optional[float] = None


class FormulaResolver(ABC):
    """Abstract base class for resolving formulas from various data sources."""

    @abstractmethod
    def get_formula(self, address: CellRef) -> Optional[str]:
        """Get the formula for a given cell address."""
        pass

    @abstractmethod
    def get_value(self, address: CellRef) -> Any:
        """Get the computed value for a given cell address."""
        pass

    @abstractmethod
    def get_all_formula_cells(self) -> Iterator[CellRef]:
        """Iterate over all cells that contain formulas."""
        pass

    def get_sheet_names(self) -> List[str]:
        """Get list of available sheet names. Default implementation."""
        return ["Sheet1"]

    def cell_exists(self, address: CellRef) -> bool:
        """Check if a cell exists. Default implementation."""
        try:
            self.get_value(address)
            return True
        except (KeyError, ValueError, AttributeError):
            return False


class CachingResolver:
    """Wrapper that adds caching to any FormulaResolver."""

    def __init__(self, resolver: FormulaResolver, cache_size: int = 1000):
        self._resolver = resolver
        self._formula_cache: Dict[CellRef, Optional[str]] = {}
        self._value_cache: Dict[CellRef, Any] = {}
        self._existence_cache: Dict[CellRef, bool] = {}
        self._cache_size = cache_size

    def get_formula(self, address: CellRef) -> Optional[str]:
        """Cached formula lookup."""
        if address not in self._formula_cache:
            if len(self._formula_cache) >= self._cache_size:
                # Simple LRU eviction - remove oldest 10%
                to_remove = list(self._formula_cache.keys())[: self._cache_size // 10]
                for addr in to_remove:
                    del self._formula_cache[addr]

            self._formula_cache[address] = self._resolver.get_formula(address)

        return self._formula_cache[address]

    def get_value(self, address: CellRef) -> Any:
        """Cached value lookup."""
        if address not in self._value_cache:
            if len(self._value_cache) >= self._cache_size:
                to_remove = list(self._value_cache.keys())[: self._cache_size // 10]
                for addr in to_remove:
                    del self._value_cache[addr]

            self._value_cache[address] = self._resolver.get_value(address)

        return self._value_cache[address]

    def cell_exists(self, address: CellRef) -> bool:
        """Cached existence check."""
        if address not in self._existence_cache:
            self._existence_cache[address] = self._resolver.cell_exists(address)
        return self._existence_cache[address]

    def get_all_formula_cells(self) -> Iterator[CellRef]:
        """Delegate to underlying resolver."""
        return self._resolver.get_all_formula_cells()

    def get_sheet_names(self) -> List[str]:
        """Delegate to underlying resolver."""
        return self._resolver.get_sheet_names()

    def invalidate_cell(self, address: CellRef) -> None:
        """Invalidate cache entries for a specific cell."""
        self._formula_cache.pop(address, None)
        self._value_cache.pop(address, None)
        self._existence_cache.pop(address, None)

    def invalidate_sheet(self, sheet_name: str) -> None:
        """Invalidate all cache entries for a sheet."""
        to_remove = [
            addr for addr in self._formula_cache.keys() if addr.sheet == sheet_name
        ]
        for addr in to_remove:
            self.invalidate_cell(addr)

    def clear_cache(self) -> None:
        """Clear all caches."""
        self._formula_cache.clear()
        self._value_cache.clear()
        self._existence_cache.clear()

    def cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "formula_cache_size": len(self._formula_cache),
            "value_cache_size": len(self._value_cache),
            "existence_cache_size": len(self._existence_cache),
        }


class DependencyTracer:
    """
    Main dependency tracing engine with caching and cycle detection.

    This class provides high-level operations for tracing formula dependencies
    while maintaining internal caches for performance and consistency.
    """

    def __init__(
        self,
        resolver: FormulaResolver,
        enable_caching: bool = True,
        default_sheet: str = "Sheet1",
    ):
        """
        Initialize the dependency tracer.

        Args:
            resolver: The formula resolver to use for data access
            enable_caching: Whether to enable caching wrapper
        """
        if enable_caching:
            self.resolver = CachingResolver(resolver)
        else:
            self.resolver = resolver

        self._dependency_cache: Dict[CellRef, DependencyInfo] = {}
        self._ast_cache: Dict[str, ASTNode] = {}  # formula -> AST
        self._reverse_deps: Dict[CellRef, Set[CellRef]] = defaultdict(set)
        self._cache_dirty = True
        self._default_sheet = default_sheet

    def _parse_formula(self, formula: str) -> ASTNode:
        """Parse formula with caching."""
        if formula not in self._ast_cache:
            try:
                self._ast_cache[formula] = parse(formula)
            except Exception as e:
                logger.warning(f"Failed to parse formula '{formula}': {e}")
                raise
        return self._ast_cache[formula]

    def _extract_references_from_formula(
        self, formula: str
    ) -> Set[Union[CellRef, RangeRef]]:
        """Extract all references from a formula without expanding ranges."""
        try:
            ast = self._parse_formula(formula)
            refs = collect_references(ast)

            addresses = set()
            for ref in refs:
                if hasattr(ref, "row"):  # CellRef
                    sheet = ref.sheet if ref.sheet is not None else self._default_sheet
                    cell_ref = CellRef(sheet=sheet, row=ref.row, col=ref.col)
                    addresses.add(cell_ref)
                elif hasattr(ref, "start") and hasattr(ref, "end"):  # RangeRef
                    # Keep as RangeRef instead of expanding
                    sheet = ref.sheet if ref.sheet is not None else self._default_sheet
                    range_ref = RangeRef(sheet=sheet, start=ref.start, end=ref.end)
                    addresses.add(range_ref)

            return addresses

        except Exception as e:
            logger.error(f"Error extracting references from '{formula}': {e}")
            return set()

    def _extract_references_from_formula_with_order(
        self, formula: str
    ) -> List[PrecedentInfo]:
        """Extract references with AST evaluation order."""
        try:
            ast = self._parse_formula(formula)
            precedents = []
            order = 0

            def visit_node(node: ASTNode) -> None:
                nonlocal order
                # Visit children first (post-order traversal for evaluation order)
                for child in node.children():
                    visit_node(child)

                # Check if this node contains a reference
                ref = node.get_reference()
                if ref is not None:
                    if hasattr(ref, "row"):  # CellRef
                        sheet = (
                            ref.sheet if ref.sheet is not None else self._default_sheet
                        )
                        cell_ref = CellRef(sheet=sheet, row=ref.row, col=ref.col)
                        precedents.append(PrecedentInfo(cell_ref, order))
                        order += 1
                    elif hasattr(ref, "start") and hasattr(ref, "end"):  # RangeRef
                        sheet = (
                            ref.sheet if ref.sheet is not None else self._default_sheet
                        )
                        range_ref = RangeRef(sheet=sheet, start=ref.start, end=ref.end)
                        precedents.append(PrecedentInfo(range_ref, order))
                        order += 1

            visit_node(ast)
            return precedents

        except Exception as e:
            logger.error(f"Error extracting ordered references from '{formula}': {e}")
            return []

    def _get_or_create_dependency_info(self, address: CellRef) -> DependencyInfo:
        """Get or create dependency info for a cell."""
        if address not in self._dependency_cache:
            formula = self.resolver.get_formula(address)
            info = DependencyInfo(cell=address, formula=formula)

            if formula:
                info.precedents = self._extract_references_from_formula(formula)
                info.ast = self._parse_formula(formula)

            self._dependency_cache[address] = info

            # Update reverse dependencies (only for CellRef precedents)
            for precedent in info.precedents:
                if isinstance(precedent, CellRef):
                    self._reverse_deps[precedent].add(address)

        return self._dependency_cache[address]

    def trace_precedents(
        self,
        target: Union[str, CellRef],
        recursive: bool = True,
        max_depth: int = 100,
        ordered: bool = False,
    ) -> Union[Set[Union[CellRef, RangeRef]], List[PrecedentInfo]]:
        """
        Trace all precedents (dependencies) of a given cell.

        Args:
            target: Cell to trace precedents for
            recursive: Whether to recursively trace precedents of precedents
            max_depth: Maximum recursion depth to prevent infinite loops
            ordered: Whether to return precedents in AST evaluation order

        Returns:
            Set of all precedent references or ordered list if ordered=True
        """
        if isinstance(target, str):
            # Handle sheet references manually if needed
            if "!" in target:
                sheet_name, cell_ref = target.split("!", 1)
                target = CellRef.from_string(cell_ref, default_sheet=sheet_name)
            else:
                target = CellRef.from_string(target, default_sheet=self._default_sheet)

        # Normalize to absolute reference for consistent lookup
        target = CellRef(
            target.sheet, target.row, target.col, abs_row=True, abs_col=True
        )

        if max_depth <= 0:
            logger.warning(f"Max depth reached tracing precedents for {target}")
            return [] if ordered else set()

        info = self._get_or_create_dependency_info(target)

        if ordered:
            # Return ordered precedents with evaluation order
            if info.formula:
                ordered_precedents = self._extract_references_from_formula_with_order(
                    info.formula
                )
                if recursive:
                    # For recursive ordered tracing, we need to maintain the evaluation order
                    # but also include recursive dependencies
                    all_precedents = []
                    seen_refs = set()

                    for prec_info in ordered_precedents:
                        if prec_info.ref not in seen_refs:
                            all_precedents.append(prec_info)
                            seen_refs.add(prec_info.ref)

                            # Add recursive precedents
                            if isinstance(prec_info.ref, CellRef):
                                recursive_precs = self.trace_precedents(
                                    prec_info.ref,
                                    recursive=True,
                                    max_depth=max_depth - 1,
                                    ordered=True,
                                )
                                for rec_prec in recursive_precs:
                                    if rec_prec.ref not in seen_refs:
                                        all_precedents.append(rec_prec)
                                        seen_refs.add(rec_prec.ref)
                            elif isinstance(prec_info.ref, RangeRef):
                                # For ranges, trace precedents of individual cells in the range
                                start_row, start_col = (
                                    prec_info.ref.start.row,
                                    prec_info.ref.start.col,
                                )
                                end_row, end_col = (
                                    prec_info.ref.end.row,
                                    prec_info.ref.end.col,
                                )

                                for row in range(start_row, end_row + 1):
                                    for col in range(start_col, end_col + 1):
                                        cell_ref = CellRef(
                                            sheet=prec_info.ref.sheet, row=row, col=col
                                        )
                                        recursive_precs = self.trace_precedents(
                                            cell_ref,
                                            recursive=True,
                                            max_depth=max_depth - 1,
                                            ordered=True,
                                        )
                                        for rec_prec in recursive_precs:
                                            if rec_prec.ref not in seen_refs:
                                                all_precedents.append(rec_prec)
                                                seen_refs.add(rec_prec.ref)

                    return all_precedents
                else:
                    return ordered_precedents
            else:
                return []
        else:
            # Return set of precedents (original behavior)
            precedents = info.precedents.copy()

            if recursive:
                for precedent in info.precedents:
                    if isinstance(precedent, CellRef):
                        recursive_precs = self.trace_precedents(
                            precedent,
                            recursive=True,
                            max_depth=max_depth - 1,
                            ordered=False,
                        )
                        precedents.update(recursive_precs)
                    elif isinstance(precedent, RangeRef):
                        # For ranges, trace precedents of individual cells in the range
                        start_row, start_col = precedent.start.row, precedent.start.col
                        end_row, end_col = precedent.end.row, precedent.end.col

                        for row in range(start_row, end_row + 1):
                            for col in range(start_col, end_col + 1):
                                cell_ref = CellRef(
                                    sheet=precedent.sheet, row=row, col=col
                                )
                                recursive_precs = self.trace_precedents(
                                    cell_ref,
                                    recursive=True,
                                    max_depth=max_depth - 1,
                                    ordered=False,
                                )
                                precedents.update(recursive_precs)

            return precedents

    def trace_dependents(
        self, target: Union[str, CellRef], recursive: bool = True, max_depth: int = 100
    ) -> Set[CellRef]:
        """
        Trace all dependents (cells that depend on this one).

        Args:
            target: Cell to trace dependents for
            recursive: Whether to recursively trace dependents of dependents
            max_depth: Maximum recursion depth to prevent infinite loops

        Returns:
            Set of all dependent cell addresses
        """
        if isinstance(target, str):
            # Handle sheet references manually if needed
            if "!" in target:
                sheet_name, cell_ref = target.split("!", 1)
                target = CellRef.from_string(cell_ref, default_sheet=sheet_name)
            else:
                target = CellRef.from_string(target, default_sheet=self._default_sheet)

        # Normalize to absolute reference for consistent lookup
        target = CellRef(
            target.sheet, target.row, target.col, abs_row=True, abs_col=True
        )

        if max_depth <= 0:
            logger.warning(f"Max depth reached tracing dependents for {target}")
            return set()

        # Ensure we have analyzed all formula cells
        self._ensure_dependency_cache_complete()

        dependents = self._reverse_deps[target].copy()

        if recursive:
            for dependent in self._reverse_deps[target]:
                dependents.update(
                    self.trace_dependents(
                        dependent, recursive=True, max_depth=max_depth - 1
                    )
                )

        return dependents

    def _ensure_dependency_cache_complete(self) -> None:
        """Ensure all formula cells have been analyzed."""
        if self._cache_dirty:
            logger.debug("Rebuilding dependency cache...")
            for address in self.resolver.get_all_formula_cells():
                self._get_or_create_dependency_info(address)
            self._cache_dirty = False

    def find_circular_dependencies(self) -> List[List[CellRef]]:
        """
        Find circular dependency chains in the workbook.

        Returns:
            List of circular dependency chains (each chain is a list of cells)
        """
        self._ensure_dependency_cache_complete()

        visited = set()
        rec_stack = set()
        cycles = []

        def dfs_find_cycle(cell: CellRef, path: List[CellRef]) -> None:
            if cell in rec_stack:
                # Found a cycle
                cycle_start = path.index(cell)
                cycles.append(path[cycle_start:] + [cell])
                return

            if cell in visited:
                return

            visited.add(cell)
            rec_stack.add(cell)

            info = self._dependency_cache.get(cell)
            if info:
                for precedent in info.precedents:
                    dfs_find_cycle(precedent, path + [cell])

            rec_stack.remove(cell)

        for address in self._dependency_cache:
            if address not in visited:
                dfs_find_cycle(address, [])

        return cycles

    def build_dependency_graph(self) -> Dict[CellRef, Set[Union[CellRef, RangeRef]]]:
        """
        Build complete dependency graph for the workbook.

        Returns:
            Dictionary mapping each cell to its direct precedents
        """
        self._ensure_dependency_cache_complete()

        graph = {}
        for address, info in self._dependency_cache.items():
            graph[address] = info.precedents.copy()

        return graph

    def topological_sort(self) -> List[CellRef]:
        """
        Return cells in topological order (dependencies before dependents).

        Returns:
            List of cells in evaluation order

        Raises:
            ValueError: If circular dependencies exist
        """
        cycles = self.find_circular_dependencies()
        if cycles:
            raise ValueError(f"Circular dependencies detected: {cycles}")

        graph = self.build_dependency_graph()
        in_degree = defaultdict(int)

        # Calculate in-degrees
        for cell in graph:
            in_degree[cell] = 0

        for cell, precedents in graph.items():
            for precedent in precedents:
                if isinstance(precedent, CellRef):
                    in_degree[cell] += 1

        # Kahn's algorithm
        queue = deque([cell for cell, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            cell = queue.popleft()
            result.append(cell)

            # Update in-degrees for dependents
            for dependent in self._reverse_deps[cell]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def invalidate_cell(self, address: Union[str, CellRef]) -> None:
        """Invalidate cache for a specific cell and mark cache as dirty."""
        if isinstance(address, str):
            # Handle sheet references manually if needed
            if "!" in address:
                sheet_name, cell_ref = address.split("!", 1)
                address = CellRef.from_string(cell_ref, default_sheet=sheet_name)
            else:
                address = CellRef.from_string(
                    address, default_sheet=self._default_sheet
                )

        # Normalize to absolute reference for consistent lookup
        address = CellRef(
            address.sheet, address.row, address.col, abs_row=True, abs_col=True
        )

        # Remove from dependency cache
        if address in self._dependency_cache:
            old_info = self._dependency_cache[address]
            # Clean up reverse dependencies (only for CellRef precedents)
            for precedent in old_info.precedents:
                if isinstance(precedent, CellRef):
                    self._reverse_deps[precedent].discard(address)
            del self._dependency_cache[address]

        # Invalidate resolver cache if applicable
        if hasattr(self.resolver, "invalidate_cell"):
            self.resolver.invalidate_cell(address)

        self._cache_dirty = True

    def invalidate_sheet(self, sheet_name: str) -> None:
        """Invalidate cache for all cells in a sheet."""
        addresses_to_remove = [
            addr for addr in self._dependency_cache.keys() if addr.sheet == sheet_name
        ]

        for address in addresses_to_remove:
            self.invalidate_cell(address)

    def clear_cache(self) -> None:
        """Clear all internal caches."""
        self._dependency_cache.clear()
        self._ast_cache.clear()
        self._reverse_deps.clear()

        if hasattr(self.resolver, "clear_cache"):
            self.resolver.clear_cache()

        self._cache_dirty = True

    def get_stats(self) -> Dict[str, Any]:
        """Get tracer statistics."""
        stats = {
            "dependency_cache_size": len(self._dependency_cache),
            "ast_cache_size": len(self._ast_cache),
            "reverse_deps_size": len(self._reverse_deps),
            "cache_dirty": self._cache_dirty,
        }

        if hasattr(self.resolver, "cache_stats"):
            stats["resolver_cache"] = self.resolver.cache_stats()

        return stats
