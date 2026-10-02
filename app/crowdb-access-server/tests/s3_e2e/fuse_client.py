# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Real s3fs mount gate. Only missing host prerequisites may skip."""

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import boto3
from botocore.config import Config


def is_mounted(mount):
    # stat() on a stalled FUSE request cannot honor the gate's deadline.
    return any(line.split()[4] == str(mount)
               for line in Path("/proc/self/mountinfo").read_text().splitlines())


def exercise_mount(mount, phase):
    target = Path(mount) / "directory" / "file.bin"
    renamed = target.with_name("renamed.bin")
    if phase == "write":
        target.parent.mkdir()
        target.write_bytes(b"first")
        assert target.read_bytes() == b"first"
        target.write_bytes(b"replaced exact bytes")
        target.rename(renamed)
        assert list(target.parent.iterdir()) == [renamed]
        assert renamed.read_bytes() == b"replaced exact bytes"
    else:
        assert renamed.read_bytes() == b"replaced exact bytes"
        renamed.unlink()
        target.parent.rmdir()


def prerequisite():
    if sys.platform != "linux":
        return "Linux required"
    try:
        fd = os.open("/dev/fuse", os.O_RDWR)
        os.close(fd)
    except OSError as error:
        return f"/dev/fuse unavailable: {error.strerror}"
    if not shutil.which("fusermount3"):
        return "fusermount3 unavailable"
    return None


def mounted_workflow(endpoint):
    unavailable = prerequisite()
    if unavailable:
        if os.environ.get("CROWDB_S3_REQUIRE_FUSE") == "1":
            raise AssertionError("required FUSE gate: " + unavailable)
        raise unittest.SkipTest("s3fs prerequisite: " + unavailable)
    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                          aws_access_key_id=os.environ["CROWDB_S3_E2E_ACCESS_KEY"],
                          aws_secret_access_key=os.environ["CROWDB_S3_E2E_SECRET_KEY"],
                          config=Config(s3={"addressing_style": "path"}))
    bucket = "crowdb-s3fs-mount"
    client.create_bucket(Bucket=bucket)
    try:
        with tempfile.TemporaryDirectory(prefix="crowdb-fuse-") as directory:
            root = Path(directory)
            mount = root / "mount"
            mount.mkdir()
            credentials = root / "credentials"
            credentials.write_text(os.environ["CROWDB_S3_E2E_ACCESS_KEY"] + ":" +
                                   os.environ["CROWDB_S3_E2E_SECRET_KEY"] + "\n")
            credentials.chmod(0o600)
            binary = Path(sys.executable).parent / "s3fs"
            args = [str(binary), bucket, str(mount), "-f", "-o", f"passwd_file={credentials}",
                    "-o", f"url={endpoint}", "-o", "use_path_request_style", "-o", "listobjectsv2",
                    "-o", "endpoint=us-east-1", "-o", "enable_content_md5"]
            for phase in ["write", "remount"]:
                with tempfile.TemporaryFile() as log:
                    process = subprocess.Popen(args, stdout=log, stderr=log)
                    try:
                        deadline = time.monotonic() + 30
                        while not is_mounted(mount):
                            if process.poll() is not None or time.monotonic() > deadline:
                                log.seek(0)
                                diagnostic = log.read().decode(errors="replace")
                                for name in ["CROWDB_S3_E2E_ACCESS_KEY", "CROWDB_S3_E2E_SECRET_KEY"]:
                                    diagnostic = diagnostic.replace(os.environ[name], "<redacted>")
                                raise AssertionError("s3fs mount failed: " + diagnostic)
                            time.sleep(0.1)
                        worker = subprocess.Popen([sys.executable, __file__, "exercise", str(mount), phase])
                        try:
                            if worker.wait(timeout=60) != 0:
                                raise AssertionError("s3fs mounted filesystem operation failed")
                        finally:
                            if worker.poll() is None:
                                try:
                                    subprocess.run(["fusermount3", "-uz", str(mount)], check=True, timeout=15)
                                finally:
                                    if process.poll() is None:
                                        process.kill()
                                    worker.kill()
                                    worker.wait(timeout=15)
                    finally:
                        try:
                            if is_mounted(mount):
                                subprocess.run(["fusermount3", "-uz", str(mount)], check=True, timeout=15)
                        finally:
                            if process.poll() is None:
                                process.terminate()
                            try:
                                process.wait(timeout=15)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=15)
    finally:
        objects = client.list_objects_v2(Bucket=bucket).get("Contents", [])
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": item["Key"]} for item in objects]})
        client.delete_bucket(Bucket=bucket)
    print("s3fs real mount/remount workflow verified")


class FuseClientCases:
    def test_s3fs_mounted_workflow(self):
        mounted_workflow(self.endpoint)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "exercise":
        exercise_mount(sys.argv[2], sys.argv[3])
        sys.exit(0)
    try:
        mounted_workflow(os.environ["CROWDB_S3_E2E_ENDPOINT"])
    except unittest.SkipTest as skipped:
        print("SKIP:", skipped)
