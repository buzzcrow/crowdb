# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Pinned CLI recipes; credentials stay in the subprocess environment."""

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from datetime import datetime


def client_env(endpoint):
    env = os.environ.copy()
    env.update(AWS_ACCESS_KEY_ID=env["CROWDB_S3_E2E_ACCESS_KEY"],
               AWS_SECRET_ACCESS_KEY=env["CROWDB_S3_E2E_SECRET_KEY"],
               AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true",
               AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off")
    env.update(RCLONE_CONFIG_CROW_TYPE="s3", RCLONE_CONFIG_CROW_PROVIDER="Other",
               RCLONE_CONFIG_CROW_ENV_AUTH="true", RCLONE_CONFIG_CROW_ENDPOINT=endpoint,
               RCLONE_CONFIG_CROW_REGION="us-east-1", RCLONE_CONFIG_CROW_FORCE_PATH_STYLE="true",
               RCLONE_CONFIG_CROW_LIST_VERSION="2", RCLONE_CONFIG_CROW_NO_SYSTEM_METADATA="true")
    return env


def run(client, args, env):
    binary = Path(sys.executable).parent / client
    result = subprocess.run([str(binary), *args], env=env, capture_output=True, timeout=120)
    if result.returncode:
        diagnostic = result.stderr.decode(errors="replace")
        for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
            if env.get(name):
                diagnostic = diagnostic.replace(env[name], "<redacted>")
        diagnostic = re.sub(r"[?&]X-Amz-[^\s]+", "?<redacted>", diagnostic, flags=re.I)
        raise AssertionError(f"{client} {args[0]} failed ({result.returncode}): {diagnostic}")
    return result.stdout.decode()


def aws_workflow(endpoint, phase="all"):
    env = client_env(endpoint)
    bucket = "crowdb-aws-cli"
    with tempfile.TemporaryDirectory(prefix="crowdb-cli-") as directory:
        root = Path(directory)
        config = root / "config"
        concurrency = int(os.environ.get("CROWDB_S3_CLIENT_CONCURRENCY", "1"))
        config.write_text("[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n    preferred_transfer_client = classic\n    multipart_threshold = 8MB\n    multipart_chunksize = 5MB\n"
                          + f"    max_concurrent_requests = {concurrency}\n")
        env["AWS_CONFIG_FILE"] = str(config)
        def aws(*args):
            return run("aws", ["--endpoint-url", endpoint, "--no-cli-pager", *args], env)
        payload = bytes(range(256)) * (48 * 1024)
        source = root / "source"
        source.mkdir()
        (source / "large.bin").write_bytes(payload)
        (source / "small.txt").write_bytes(b"CLI exact bytes")
        if phase in ["all", "write"]:
            aws("s3api", "create-bucket", "--bucket", bucket)
            assert bucket in aws("s3", "ls")
            aws("s3", "cp", str(source / "large.bin"), f"s3://{bucket}/prefix/large.bin")
            aws("s3", "cp", str(source / "small.txt"), f"s3://{bucket}/prefix/small.txt")
            assert "large.bin" in aws("s3", "ls", f"s3://{bucket}/prefix/", "--recursive")
            aws("s3", "cp", f"s3://{bucket}/prefix/small.txt", f"s3://{bucket}/copied.txt", "--metadata-directive", "COPY")
            aws("s3", "cp", f"s3://{bucket}/prefix/large.bin", f"s3://{bucket}/copied-large.bin", "--metadata-directive", "COPY")
            aws("s3", "sync", str(source), f"s3://{bucket}/synced/", "--delete")
            (source / "small.txt").unlink()
            aws("s3", "sync", str(source), f"s3://{bucket}/synced/", "--delete")
            assert "small.txt" not in aws("s3", "ls", f"s3://{bucket}/synced/", "--recursive")
        if phase in ["all", "read"]:
            aws("s3", "cp", f"s3://{bucket}/prefix/large.bin", str(root / "download"))
            assert (root / "download").read_bytes() == payload
            aws("s3", "cp", f"s3://{bucket}/copied-large.bin", str(root / "large-copy-download"))
            assert (root / "large-copy-download").read_bytes() == payload
            aws("s3", "cp", f"s3://{bucket}/copied.txt", str(root / "copy-download"))
            assert (root / "copy-download").read_bytes() == b"CLI exact bytes"
        if phase == "all":
            aws("s3", "rm", f"s3://{bucket}", "--recursive")
            aws("s3api", "delete-bucket", "--bucket", bucket)
    print(f"AWS CLI recipe {phase}: discovery, multipart transfer, prefix, copy, sync/delete verified")


def rclone_workflow(endpoint, phase="all"):
    env = client_env(endpoint)
    bucket = "crowdb-rclone"
    with tempfile.TemporaryDirectory(prefix="crowdb-rclone-") as directory:
        root = Path(directory)
        env["RCLONE_CONFIG"] = str(root / "empty.conf")
        def rclone(*args):
            return run("rclone", [*args, "--retries", "1", "--low-level-retries", "1",
                                  "--s3-upload-cutoff", "8Mi", "--s3-chunk-size", "5Mi",
                                  "--transfers", "1", "--checkers", "1",
                                  "--s3-upload-concurrency", "1"], env)
        payload = bytes(range(256)) * (48 * 1024)
        source = root / "source"
        source.mkdir()
        (source / "large.bin").write_bytes(payload)
        (source / "small.txt").write_bytes(b"rclone exact bytes")
        for path in source.iterdir():
            os.utime(path, (1700000000, 1700000000))
        remote = f"crow:{bucket}"
        if phase in ["all", "write"]:
            rclone("mkdir", remote)
            assert bucket in rclone("lsd", "crow:")
            rclone("copy", str(source), remote + "/prefix")
            assert "large.bin" in rclone("lsf", remote + "/prefix", "--recursive")
            rclone("copyto", remote + "/prefix/small.txt", remote + "/copied.txt")
            rclone("sync", str(source), remote + "/synced", "--ignore-times")
            (source / "small.txt").unlink()
            rclone("sync", str(source), remote + "/synced", "--ignore-times")
            assert "small.txt" not in rclone("lsf", remote + "/synced", "--recursive")
        if phase in ["all", "read"]:
            rclone("copyto", remote + "/prefix/large.bin", str(root / "download"))
            assert (root / "download").read_bytes() == payload
            rclone("copyto", remote + "/copied.txt", str(root / "copy-download"))
            assert (root / "copy-download").read_bytes() == b"rclone exact bytes"
            for key in ["prefix/small.txt", "prefix/large.bin", "copied.txt"]:
                details = json.loads(rclone("lsjson", remote + "/" + key, "--stat"))
                assert datetime.fromisoformat(details["ModTime"].replace("Z", "+00:00")).timestamp() == 1700000000, details["ModTime"]
        if phase == "all":
            rclone("purge", remote)
    print(f"rclone recipe {phase}: discovery, multipart transfer, prefix, copy, sync/delete verified")


class CliClientCases:
    def test_aws_cli_workflow(self):
        aws_workflow(self.endpoint)
        status, _, metrics = self.signed_http("GET", "/_crowdb/metrics")
        self.assertEqual(status, 200)
        self.assertIn(b"crowdb_s3_native_retained_bytes 0\n", metrics)
        self.assertNotIn("crowdb-aws-cli", [bucket["Name"] for bucket in self.client.list_buckets()["Buckets"]])

    def test_rclone_workflow(self):
        rclone_workflow(self.endpoint)
if __name__ == "__main__":
    selected = sys.argv[1]
    phase = sys.argv[2] if len(sys.argv) > 2 else "all"
    endpoint = os.environ["CROWDB_S3_E2E_ENDPOINT"]
    {"aws": aws_workflow, "rclone": rclone_workflow}[selected](endpoint, phase)
