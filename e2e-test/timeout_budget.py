# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host acceptance budgets shared by local runs and CI."""

import math
import os


def scaled_timeout(seconds):
    scale = float(os.environ.get("CROWDB_E2E_TIMEOUT_SCALE", "1"))
    if not math.isfinite(scale) or scale < 1:
        raise ValueError("CROWDB_E2E_TIMEOUT_SCALE must be finite and at least 1")
    return seconds * scale
