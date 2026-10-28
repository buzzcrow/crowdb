# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Check builder tools and privileges before compiling the runtime."""

import argparse
import subprocess

from builder import preflight, tool
from builder_host import apparmor_profile

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--privileged", action="store_true")
    mode.add_argument('--apparmor-profile', action='store_true',
                      help='print a per-executable RootlessKit profile for administrator installation')
    parser.add_argument('--setup-apparmor', action='store_true',
                        help='install a targeted profile through sudo if AppArmor blocks RootlessKit')
    args = parser.parse_args()
    try:
        if args.apparmor_profile:
            print(apparmor_profile(tool('rootlesskit')), end='')
        else:
            preflight(args.privileged, setup_host=args.setup_apparmor)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        parser.exit(1, str(error) + '\n')
