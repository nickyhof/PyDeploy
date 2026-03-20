"""Deploy-phase executor: run the pipeline locally.

State is persisted to a state file so resource IDs survive across deploys.
Token attributes (team, resource_id) are resolved at deploy time.
Jobs in containers can hydrate from the state file using ``load_assets_from_state()``.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Any

from pydeploy.nodes import AssetNode, Table, JobNode, Token

STATE_FILE = "output/pydeploy.state.json"


@dataclass
class Context:
    """Execution context tracking deploy results.

    Attributes:
        assets: mapping of asset name → Asset instance.
        outputs: mapping of node name → output value from execution.
    """

    assets: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)


def _load_state(state_file: str) -> dict[str, Any]:
    """Load previously persisted deploy state, or return empty state."""
    if os.path.exists(state_file):
        with open(state_file) as f:
            return json.load(f)
    return {"resources": {}}


def _save_state(state: dict[str, Any], state_file: str) -> None:
    """Persist deploy state to disk."""
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


def _provision_asset(
    asset_node: AssetNode,
    ctx: Context,
    state: dict[str, Any],
    deploy_params: dict[str, dict[str, Any]],
) -> None:
    """Provision an asset: resolve tokens from state and deploy params."""
    asset = asset_node.asset
    name = asset_node.id
    resources = state.setdefault("resources", {})

    if isinstance(asset, Table):
        # Resolve team from deploy params
        asset_params = deploy_params.get(name, {})
        if "team" in asset_params:
            asset.team.resolve(asset_params["team"])
        else:
            raise ValueError(
                f"Missing 'team' for table '{name}'. "
                f"Provide it in the deploy params config."
            )

        # Resolve resource_id from state or generate new
        existing = resources.get(name)
        if existing:
            rid = existing["resource_id"]
            asset.resource_id.resolve(rid)
            print(f"  ✓ Table '{name}': reusing resource {rid}")
        else:
            rid = str(uuid.uuid4())
            asset.resource_id.resolve(rid)
            print(f"  ✓ Table '{name}': created resource {rid}")

        resources[name] = {
            "type": "table",
            "resource_id": rid,
            "schema": asset.schema,
            "team": asset.team.value,
        }
    else:
        print(f"  ✓ Asset '{name}': registered ({type(asset).__name__})")

    ctx.assets[name] = asset
    ctx.outputs[name] = asset


def _provision_job(
    job_node: JobNode,
    ctx: Context,
    state_file: str,
    pipeline_module: str,
) -> None:
    """Provision a job: generate a runnable Python script.

    The generated script adds the project root and pipeline directory
    to sys.path, hydrates asset dependencies from the state file,
    and invokes the job handler.
    """
    name = job_node.id
    dep_names = [dep.id for dep in job_node.depends_on]

    pipeline_stem = os.path.splitext(os.path.basename(pipeline_module))[0]

    # Compute relative paths from the jobs/ output dir
    jobs_dir = os.path.join(os.path.dirname(state_file), "jobs")
    os.makedirs(jobs_dir, exist_ok=True)
    script_path = os.path.join(jobs_dir, f"{name}.py")

    rel_state = os.path.relpath(state_file, jobs_dir)
    rel_pipeline_dir = os.path.relpath(
        os.path.dirname(os.path.abspath(pipeline_module)), jobs_dir
    )
    rel_project_root = os.path.relpath(os.getcwd(), jobs_dir)

    script_lines = [
        '#!/usr/bin/env python3',
        f'"""Auto-generated job script for \'{name}\'."""',
        f'',
        f'import os',
        f'import sys',
        f'',
        f'_dir = os.path.dirname(os.path.abspath(__file__))',
        f'sys.path.insert(0, os.path.join(_dir, "{rel_project_root}"))',
        f'sys.path.insert(0, os.path.join(_dir, "{rel_pipeline_dir}"))',
        f'',
        f'from pydeploy.deploy import load_assets_from_state',
        f'',
        f'_state = os.path.join(_dir, "{rel_state}")',
        f'assets = load_assets_from_state(_state)',
        f'',
    ]

    for dep_name in dep_names:
        script_lines.append(f'{dep_name} = assets["{dep_name}"]')

    script_lines.append(f'')
    script_lines.append(f'from {pipeline_stem} import {name}')
    script_lines.append(f'{name}({", ".join(dep_names)})')
    script_lines.append(f'')

    with open(script_path, "w") as f:
        f.write("\n".join(script_lines))

    os.chmod(script_path, 0o755)

    ctx.outputs[name] = script_path
    print(f"  ✓ Job '{name}': script → {script_path}")


def execute(
    nodes: list[AssetNode | JobNode],
    execution_order: list[str],
    state_file: str = STATE_FILE,
    deploy_params: dict[str, dict[str, Any]] | None = None,
    pipeline_module: str = "pipeline",
) -> Context:
    """Provision assets and jobs in topological order.

    Assets are provisioned (tokens resolved, state persisted).
    Jobs produce runnable scripts that hydrate from the state file.

    Args:
        deploy_params: mapping of asset name → {token: value} for
            resolving tokens at deploy time (e.g., team).
        pipeline_module: importable module name containing the job handlers.
    """
    if deploy_params is None:
        deploy_params = {}

    name_to_node: dict[str, AssetNode | JobNode] = {n.id: n for n in nodes}
    state = _load_state(state_file)
    ctx = Context()

    print("🚀 Deploying...\n")
    for name in execution_order:
        node = name_to_node[name]
        if isinstance(node, AssetNode):
            _provision_asset(node, ctx, state, deploy_params)
        elif isinstance(node, JobNode):
            _provision_job(node, ctx, state_file, pipeline_module)

    _save_state(state, state_file)
    print(f"\n✅ Deploy complete. State saved to '{state_file}'")
    return ctx


def load_assets_from_state(state_file: str) -> dict[str, Table]:
    """Hydrate Table instances (with resolved Tokens) from a state file.

    This is the entry point for containerized job execution::

        from pydeploy.deploy import load_assets_from_state

        assets = load_assets_from_state("/config/state.json")
        users = assets["raw_users"]
        users.team.value     # → "data-team"
        users.ingest(my_dataframe)

    Returns:
        A dict mapping asset name → hydrated Table (all tokens resolved).
    """
    state = _load_state(state_file)
    tables: dict[str, Table] = {}

    for name, info in state.get("resources", {}).items():
        if info.get("type") == "table":
            team = Token(f"${{{name}.team}}")
            team.resolve(info.get("team", ""))

            ds = Table(
                name=name,
                schema=info.get("schema", {}),
                team=team,
            )
            ds.resource_id = Token(f"${{{name}.resource_id}}")
            ds.resource_id.resolve(info["resource_id"])
            tables[name] = ds

    return tables
