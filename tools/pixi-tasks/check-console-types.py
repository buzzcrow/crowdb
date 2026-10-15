# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Type-check the same self-contained browser client layout as the native runner."""
import pathlib
import shutil
import subprocess
import tempfile

root = pathlib.Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix="crowdb-console-types-") as temporary:
    project = pathlib.Path(temporary) / "console-ui"
    shutil.copytree(root / "e2e-test/clients/console-ui", project,
                    ignore=shutil.ignore_patterns("node_modules", "test-results", "playwright-report"))
    for relative in ("chunk-kv/catalog.ts", "iceberg/json.ts", "iceberg/types.ts"):
        target = project / "src" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / "app/crowdb-web/ui/src" / relative, target)
    subprocess.run(["npm", "ci", "--ignore-scripts"], cwd=project, check=True)
    subprocess.run(["npx", "tsc", "--noEmit", "--project", "tsconfig.json"], cwd=project, check=True)
