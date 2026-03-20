#!/usr/bin/env python3
"""Auto-generated job script for 'ingest_users'."""

import os
import sys

_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_dir, "../.."))
sys.path.insert(0, os.path.join(_dir, "../../examples"))

from pydeploy.deploy import load_assets_from_state

_state = os.path.join(_dir, "../example_pipeline.state.json")
assets = load_assets_from_state(_state)

raw_users = assets["raw_users"]

from pipeline import ingest_users
ingest_users(raw_users)
