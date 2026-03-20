#!/usr/bin/env bash
set -euo pipefail

uv run pydeploy deploy examples/pipeline.py --params examples/params.json "$@"
