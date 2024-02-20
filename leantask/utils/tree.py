from typing import Any, List


def sort_tree_nodes(nodes: List[Any], children_attr: str):
    ordered_nodes = []
    visited_nodes = set()

    def dfs(node):
        nonlocal visited_nodes
        if node in visited_nodes:
            return

        visited_nodes.add(node)
        for child_node in getattr(node, children_attr):
            dfs(child_node)

        ordered_nodes.append(node)

    for node in nodes:
        dfs(node)

    return ordered_nodes[::-1]
