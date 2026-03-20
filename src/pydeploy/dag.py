"""DAG — the root object that users interact with.

Provides ``@dag.asset`` and ``@dag.job()`` decorators.
Dependencies are inferred from function parameter names.
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable

from pydeploy.deploy import Context, execute
from pydeploy.nodes import (
    Asset,
    AssetNode,
    Table,
    JobNode,
    Token,
    _resolve_asset_dependencies,
    _resolve_job_dependencies,
)
from pydeploy.synth import synthesize


class DAG:
    """A directed acyclic graph of assets and jobs.

    Example usage::

        dag = DAG("my_pipeline")

        @dag.asset
        def raw_data() -> Table:
            return Table(schema={"id": "int"})

        @dag.job()
        def transform(raw_data):
            print(raw_data.resource_id)  # Token("${raw_data.resource_id}")
    """

    def __init__(self, id: str) -> None:  # noqa: A002
        self.id = id
        self._nodes: dict[str, AssetNode | JobNode] = {}

    # ── Node registration ───────────────────────────────────────────

    def asset(self, fn: Callable[..., Asset]) -> Callable[..., Asset]:
        """Decorator to register an Asset node.

        The decorated function must return an Asset subclass instance
        (e.g., Table). The function name becomes the node name.

        Token attributes (like ``resource_id``) are initialized as
        unresolved tokens referencing this asset. They resolve at
        deploy time::

            @dag.asset
            def raw_data() -> Table:
                return Table(schema={"id": "int"})
            # raw_data.resource_id → Token("${raw_data.resource_id}")
        """
        # Resolve deps (must all be other assets)
        deps = _resolve_asset_dependencies(fn, self._nodes)

        # Call fn with dependency Asset instances as args
        dep_assets = [dep.asset for dep in deps]
        asset_instance = fn(*dep_assets)

        if not isinstance(asset_instance, Asset):
            raise TypeError(
                f"'{fn.__name__}' must return an Asset subclass, "
                f"got {type(asset_instance).__name__}"
            )

        name = fn.__name__
        if hasattr(asset_instance, "name") and not asset_instance.name:
            asset_instance.name = name

        # Initialize Token refs for known attributes
        if isinstance(asset_instance, Table):
            asset_instance.team = Token(f"${{{name}.team}}")
            asset_instance.resource_id = Token(f"${{{name}.resource_id}}")

        node = AssetNode(name, asset=asset_instance, depends_on=deps)
        self._nodes[name] = node
        return fn

    def job(self, _fn: Callable[..., Any] | None = None) -> Any:
        """Decorator to register a Job node.

        Dependencies are inferred from the function's parameter names.
        Each parameter must match a previously registered asset::

            @dag.job()
            def process(raw_data, config):
                ...

        Can be used with or without parentheses::

            @dag.job
            def simple_job():
                ...
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            name = fn.__name__
            deps = _resolve_job_dependencies(fn, self._nodes)
            node = JobNode(name, handler=fn, depends_on=deps)
            self._nodes[name] = node
            return fn

        if _fn is not None:
            return decorator(_fn)
        return decorator

    def get_node(self, name: str) -> AssetNode | JobNode:
        """Retrieve a registered node by name."""
        return self._nodes[name]

    # ── Phase entry points ──────────────────────────────────────────

    def synth(self) -> dict[str, Any]:
        """Plan phase: synthesize the DAG into a deployment manifest."""
        return synthesize(self)

    def synth_to_file(self, path: str | None = None) -> str:
        """Synthesize and write the manifest to a JSON file."""
        manifest = self.synth()
        if path is None:
            path = f"output/{self.id}.manifest.json"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)
        return path

    def deploy(
        self,
        state_file: str | None = None,
        params: dict[str, dict[str, Any]] | None = None,
        pipeline_module: str = "pipeline",
    ) -> Context:
        """Deploy phase: provision assets and generate job scripts.

        Args:
            state_file: path for state persistence.
            params: mapping of asset name → {token: value}
                to resolve tokens at deploy time.
            pipeline_module: importable module name for job imports.
        """
        if state_file is None:
            state_file = f"output/{self.id}.state.json"
        manifest = self.synth()
        nodes = list(self._nodes.values())
        return execute(
            nodes, manifest["execution_order"],
            state_file=state_file,
            deploy_params=params,
            pipeline_module=pipeline_module,
        )
