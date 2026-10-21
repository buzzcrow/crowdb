#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Exercise entrypoint ordering/credentials in Docker, with a monitor boundary stub."""

import argparse
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import uuid


MONITOR = """#!/bin/sh
set -eu
case "$1" in
    validate) exit 0 ;;
    accept-policy)
        test "$CROWDB_TEST_REJECT" != 1 || exit 7
        umask 077
        printf '{"version":1}' > /opt/crowdb/data/node-policy.json
        printf '%s\\n' "$CROWDB_TEST_MODE"
        ;;
    run)
        test "$CROWDB_DEPLOYMENT_MODE" = "$CROWDB_TEST_MODE"
        test -s /opt/crowdb/data/ssh/id_ed25519
        test -s /opt/crowdb/data/ssh/ssh_host_ed25519_key
        touch /opt/crowdb/data/entrypoint-ran
        ;;
    *) exit 2 ;;
esac
"""

CHECK = """#!/bin/sh
set -eu
set +e
/bin/sh /entrypoint-under-test
result=$?
set -e
if test "$CROWDB_TEST_REJECT" = 1; then
    test "$result" != 0
    test ! -e /opt/crowdb/data/ssh
    test ! -e /opt/crowdb/data/disks
    test ! -e /opt/crowdb/data/node-policy.json
elif test "$CROWDB_TEST_MODE" = production && test "$CROWDB_TEST_PASSWORD" = 0; then
    test "$result" != 0
    test ! -e /opt/crowdb/data/ssh/id_ed25519
    test ! -e /opt/crowdb/data/ssh/local-admission-password
else
    test "$result" = 0
    test -e /opt/crowdb/data/entrypoint-ran
    test "$(stat -c %a /opt/crowdb/data/node-policy.json)" = 600
    if test "$CROWDB_TEST_MODE" = local; then
        test "$(stat -c %a /opt/crowdb/data/ssh/local-admission-password)" = 600
        test "$(stat -c %u /opt/crowdb/data/ssh/local-admission-password)" = 10001
        cmp /run/test-password /opt/crowdb/data/ssh/local-admission-password
    else
        test ! -e /opt/crowdb/data/ssh/local-admission-password
    fi
fi
"""


def invoke(*arguments):
    return subprocess.run(["docker", *arguments], text=True, capture_output=True, timeout=30)


def run_case(image, entrypoint, mode, supplied, reject):
    name = "crowdb-entrypoint-policy-" + uuid.uuid4().hex[:12]
    secret = secrets.token_urlsafe(24) if supplied else "crowdb"
    with tempfile.TemporaryDirectory(prefix="crowdb-entrypoint-policy-") as directory:
        directory = Path(directory)
        for filename, body in (("monitor", MONITOR), ("check", CHECK)):
            path = directory / filename
            path.write_text(body)
            path.chmod(0o755)
        password = directory / "password"
        password.write_text(secret)
        password.chmod(0o600)
        arguments = ["run", "--rm", "--name", name, "--network", "none",
                     "--label", "org.crowdb.test=entrypoint-policy",
                     "--tmpfs", "/opt/crowdb/data:uid=10001,gid=10001,mode=0755",
                     "--entrypoint", "/bin/sh"]
        for source, target in ((entrypoint, "/entrypoint-under-test"),
                               (directory / "monitor", "/opt/crowdb/bin/crowdb-monitor"),
                               (directory / "check", "/entrypoint-check"),
                               (password, "/run/test-password")):
            arguments += ["--mount", f"type=bind,source={source},target={target},readonly"]
        for key, value in (("CROWDB_TEST_MODE", mode), ("CROWDB_TEST_REJECT", int(reject)),
                           ("CROWDB_TEST_PASSWORD", int(supplied))):
            arguments += ["--env", f"{key}={value}"]
        if supplied:
            arguments += ["--env", "CROWDB_SSH_PASSWORD_FILE=/run/test-password"]
        try:
            result = invoke(*arguments, image, "/entrypoint-check")
            output = (result.stdout + result.stderr).replace(secret, "<redacted>")
            if result.returncode:
                raise RuntimeError(f"case mode={mode} supplied={supplied} reject={reject}:\n{output}")
            if secret != "crowdb" and secret in result.stdout + result.stderr:
                raise RuntimeError("entrypoint exposed a private SSH credential")
            print(f"PASS entrypoint mode={mode} supplied={supplied} reject={reject}", flush=True)
        finally:
            inspected = invoke("container", "inspect", name)
            if inspected.returncode == 0:
                removed = invoke("rm", "--force", name)
                if removed.returncode:
                    raise RuntimeError(removed.stdout + removed.stderr)
            elif "No such" not in inspected.stderr:
                raise RuntimeError(inspected.stdout + inspected.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=os.environ.get("CROWDB_CONTAINER_IMAGE", "crowdb-node:dev"))
    args = parser.parse_args()
    entrypoint = Path(__file__).resolve().parents[1] / "entrypoint.sh"
    for mode, supplied, reject in (("local", False, True), ("local", False, False),
                                   ("local", True, False), ("production", False, False),
                                   ("production", True, False)):
        run_case(args.image, entrypoint, mode, supplied, reject)
    print("Entrypoint boundary checks passed; this does not certify application startup or image content.", flush=True)


if __name__ == "__main__":
    main()
