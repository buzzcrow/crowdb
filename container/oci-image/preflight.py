# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Check builder tools and privileges before compiling the runtime."""

import argparse

from builder import preflight

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--privileged", action="store_true")
    args = parser.parse_args()
    preflight(args.privileged)
