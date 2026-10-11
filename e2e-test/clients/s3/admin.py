# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host invocation of the exact release credential CLI, with no secret output."""

import os
import subprocess
import sys


def invoke(command):
    return subprocess.run(["pixi", "run", sys.argv[1], command, "s3-cli-e2e"],
                          env=os.environ.copy(), capture_output=True, text=True, timeout=30)


def tokens(result):
    assert result.returncode == 0, "release credential CLI failed"
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    return values["AWS_ACCESS_KEY_ID"], values["AWS_SECRET_ACCESS_KEY"]


missing = invoke("lookup-user")
assert missing.returncode != 0, "absent credential lookup unexpectedly succeeded"
assert "AWS_ACCESS_KEY_ID=" not in missing.stdout, "absent lookup emitted credentials"
issued = tokens(invoke("issue-user"))
for command in ("ensure-user", "lookup-user", "ensure-user"):
    assert tokens(invoke(command)) == issued, "release credential CLI changed identity"
print("release credential CLI issue/ensure/lookup passed")
