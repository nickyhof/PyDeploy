"""Synthesis: convert a DAG into a deployment manifest.

Walks the DAG's registered nodes, performs topological sort,
and serializes each node via its ``to_manifest()`` method.
"""

from __future__ import annotations

import graphlib
from typing import Any

from pydeploy.nodes import AssetNode, JobNode


class CyclicDependencyError(Exception):
    """Raised when the DAG contains a cycle."""


def topological_sort(nodes: list[AssetNode | JobNode]) -> list[str]:
    """Return node names in topological (dependency-first) order."""
    graph: dict[str, set[str]] = {}

    for node in nodes:
        name = node.id
        if hasattr(node, "depends_on") and node.depends_on:
            graph[name] = {dep.id for dep in node.depends_on}
        else:
            graph[name] = set()

    try:
        sorter = graphlib.TopologicalSorter(graph)
        return list(sorter.static_order())
    except graphlib.CycleError as e:
        raise CyclicDependencyError(
            f"Dependency cycle detected: {e}"
        ) from e


def synthesize(dag: Any) -> dict[str, Any]:
    """Synthesize a DAG into a deployment manifest."""
    nodes = list(dag._nodes.values())
    order = topological_sort(nodes)
    name_to_node = {n.id: n for n in nodes}

    return {
        "dag": dag.id,
        "nodes": [name_to_node[name].to_manifest() for name in order],
        "execution_order": order,
    }
