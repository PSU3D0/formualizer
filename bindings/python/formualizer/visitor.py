"""
Visitor utility for traversing AST nodes.

This module provides a simple DFS traversal utility with early-exit capability.
"""

from typing import Callable, Union

try:
    from . import PyASTNode
except ImportError:
    # For when this is used standalone
    pass


class VisitControl:
    """Control flow for AST visitor."""

    CONTINUE = "continue"
    SKIP = "skip"
    STOP = "stop"


def walk_ast(
    node: "PyASTNode", visitor: Callable[["PyASTNode"], Union[str, None]]
) -> None:
    """
    DFS traversal of AST with early-exit capability.

    Args:
        node: The AST node to start traversal from
        visitor: A function that takes a PyASTNode and returns:
                - 'continue' or None: continue normal traversal
                - 'skip': skip children of this node
                - 'stop': stop traversal entirely

    Example:
        ```python
        def count_references(node):
            if node.node_type() == "Reference":
                ref_count[0] += 1
            return VisitControl.CONTINUE

        ref_count = [0]
        walk_ast(ast_root, count_references)
        print(f"Found {ref_count[0]} references")
        ```

    Example with early exit:
        ```python
        def find_first_function(node):
            if node.node_type() == "Function":
                found_functions.append(node.get_function_name())
                return VisitControl.STOP
            return VisitControl.CONTINUE

        found_functions = []
        walk_ast(ast_root, find_first_function)
        ```
    """

    def _walk_recursive(current_node: "PyASTNode") -> str:
        # Call visitor on current node
        result = visitor(current_node)

        # Handle visitor response
        if result == VisitControl.STOP:
            return VisitControl.STOP
        elif result == VisitControl.SKIP:
            return VisitControl.CONTINUE

        # Continue with children if not skipping
        for child in current_node.children():
            if _walk_recursive(child) == VisitControl.STOP:
                return VisitControl.STOP

        return VisitControl.CONTINUE

    _walk_recursive(node)


def collect_nodes_by_type(node: "PyASTNode", node_type: str) -> list:
    """
    Collect all nodes of a specific type from the AST.

    Args:
        node: The AST node to start traversal from
        node_type: The type of nodes to collect (e.g., "Reference", "Function")

    Returns:
        List of PyASTNode objects matching the specified type
    """
    nodes = []

    def collector(current_node):
        if current_node.node_type() == node_type:
            nodes.append(current_node)
        return VisitControl.CONTINUE

    walk_ast(node, collector)
    return nodes


def collect_references(node: "PyASTNode") -> list:
    """
    Collect all reference nodes from the AST as rich reference objects.

    Args:
        node: The AST node to start traversal from

    Returns:
        List of reference objects (CellRef, RangeRef, etc.)
    """
    references = []

    def ref_collector(current_node):
        ref = current_node.get_reference()
        if ref is not None:
            references.append(ref)
        return VisitControl.CONTINUE

    walk_ast(node, ref_collector)
    return references


def collect_function_names(node: "PyASTNode") -> list:
    """
    Collect all function names used in the AST.

    Args:
        node: The AST node to start traversal from

    Returns:
        List of function name strings
    """
    function_names = []

    def name_collector(current_node):
        if current_node.node_type() == "Function":
            name = current_node.get_function_name()
            if name:
                function_names.append(name)
        return VisitControl.CONTINUE

    walk_ast(node, name_collector)
    return function_names
