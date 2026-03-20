#!/usr/bin/env bash
set -euo pipefail

uv run pydeploy plan examples/pipeline.py "$@"
