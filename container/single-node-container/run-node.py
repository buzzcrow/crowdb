#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Existing Docker node entry point delegates to the shared OCI launcher."""
import runpy
import sys
from pathlib import Path

common = Path(__file__).resolve().parents[1] / "oci-image"
sys.path.insert(0, str(common))
runpy.run_path(str(common / "run-node.py"), run_name="__main__")
