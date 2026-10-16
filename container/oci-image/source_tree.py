# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Record a working source tree without changing HEAD or the user's index."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile


def snapshot(root):
    root = Path(root).resolve()
    with tempfile.TemporaryDirectory(prefix="crowdb-source-index-") as directory:
        environment = dict(os.environ, GIT_INDEX_FILE=str(Path(directory) / "index"))
        command = ["git", "-C", str(root)]
        subprocess.run([*command, "read-tree", "HEAD"], env=environment, check=True)
        subprocess.run([*command, "add", "--all", "--", "."], env=environment, check=True)
        return subprocess.check_output([*command, "write-tree"], env=environment, text=True).strip()


if __name__ == "__main__":
    print(snapshot(sys.argv[1] if len(sys.argv) > 1 else Path.cwd()))
