# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Verify source/artifact binding and export a CI digest receipt."""

import os
import sys
from pathlib import Path

from artifact import receipt

verified = receipt(Path(sys.argv[1]))
if verified["revision"] != os.environ["GITHUB_SHA"]:
    raise ValueError("CI artifact source differs from checkout")
with open(os.environ["GITHUB_OUTPUT"], "a") as output:
    output.write("digest=" + verified["image_digest"] + "\n")
print("Verified OCI digest: " + verified["image_digest"])
