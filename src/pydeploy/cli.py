"""CLI entrypoint for PyDeploy.

Usage:
    pydeploy plan <module>       — synthesize DAG → output/{dag_name}.manifest.json
    pydeploy deploy <module>     — run the pipeline locally
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


def _load_pipeline_module(module_path: str) -> Any:
    """Dynamically import a Python module from a file path."""
    path = Path(module_path).resolve()
    if not path.exists():
        print(f"Error: '{module_path}' not found", file=sys.stderr)
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("pipeline", path)
    if spec is None or spec.loader is None:
        print(f"Error: could not load '{module_path}'", file=sys.stderr)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _find_dag(module: Any) -> Any:
    """Find the DAG instance in a loaded module."""
    from pydeploy.dag import DAG

    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if isinstance(obj, DAG):
            return obj

    print("Error: no DAG instance found in module", file=sys.stderr)
    sys.exit(1)


def cmd_plan(args: argparse.Namespace) -> None:
    """Execute the plan (synth) phase."""
    module = _load_pipeline_module(args.module)
    dag = _find_dag(module)

    output_path = dag.synth_to_file(args.output)

    manifest = dag.synth()
    print(f"📋 Plan for DAG '{manifest['dag']}':\n")
    print(f"   Nodes ({len(manifest['nodes'])}):")
    for node in manifest["nodes"]:
        node_type = node["type"]
        deps = node.get("depends_on", [])
        dep_str = f" → depends on: {deps}" if deps else ""
        print(f"   • [{node_type}] {node['name']}{dep_str}")

    print(f"\n   Execution order: {' → '.join(manifest['execution_order'])}")
    print(f"\n✅ Manifest written to '{output_path}'")


def cmd_deploy(args: argparse.Namespace) -> None:
    """Execute the deploy phase."""
    if args.manifest:
        # Deploy from a saved manifest — limited to displaying the plan.
        path = Path(args.manifest)
        if not path.exists():
            print(f"Error: manifest '{args.manifest}' not found", file=sys.stderr)
            sys.exit(1)
        with open(path) as f:
            manifest = json.load(f)
        print(f"📋 Loaded manifest for DAG '{manifest['dag']}'")
        print("⚠  Deploying from manifest requires the original module for handlers.")
        print("   Use: pydeploy deploy <module> instead.")
        sys.exit(1)

    module = _load_pipeline_module(args.module)
    dag = _find_dag(module)

    # Resolve absolute pipeline path for job script imports
    pipeline_path = str(Path(args.module).resolve())

    # Load deploy params if provided
    deploy_params = None
    if args.params:
        params_path = Path(args.params)
        if not params_path.exists():
            print(f"Error: params file '{args.params}' not found", file=sys.stderr)
            sys.exit(1)
        with open(params_path) as f:
            deploy_params = json.load(f)

    dag.deploy(params=deploy_params, pipeline_module=pipeline_path)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="pydeploy",
        description="PyDeploy — plan and deploy DAG pipelines",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # plan
    plan_parser = subparsers.add_parser("plan", help="Synthesize DAG into a manifest")
    plan_parser.add_argument("module", help="Path to the pipeline module (.py file)")
    plan_parser.add_argument(
        "-o", "--output", help="Output path (default: output/{dag_name}.manifest.json)"
    )
    plan_parser.set_defaults(func=cmd_plan)

    # deploy
    deploy_parser = subparsers.add_parser("deploy", help="Execute the pipeline locally")
    deploy_parser.add_argument(
        "module", nargs="?", help="Path to the pipeline module (.py file)"
    )
    deploy_parser.add_argument(
        "--manifest", help="Deploy from a saved manifest JSON file"
    )
    deploy_parser.add_argument(
        "--params", help="Path to JSON file with deploy-time params"
    )
    deploy_parser.set_defaults(func=cmd_deploy)

    args = parser.parse_args()
    args.func(args)
