#!/usr/bin/env python3
"""Auto-generated job script for 'build_report'."""

import os
import sys

_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_dir, "../.."))
sys.path.insert(0, os.path.join(_dir, "../../examples"))

from pydeploy.deploy import load_assets_from_state

_state = os.path.join(_dir, "../example_pipeline.state.json")
assets = load_assets_from_state(_state)

user_orders = assets["user_orders"]

from pipeline import build_report
build_report(user_orders)
