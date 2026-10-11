#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Delegate managed browser acceptance to its release-container owner."""

import subprocess
import sys
from pathlib import Path

raise SystemExit(subprocess.call([
    "pixi", "run", "test-container-e2e", "--scope", "console.managed", *sys.argv[1:]
], cwd=Path(__file__).resolve().parents[2]))
