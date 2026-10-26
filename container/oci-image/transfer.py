# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Import or publish the verified OCI artifact without reconstruction."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from artifact import inspect, receipt
from builder import tool


def skopeo():
    return [tool("skopeo"), "--registries-conf", str(Path(__file__).with_name("registries.conf"))]


def remote_digest(image, authfile=None):
    command = [*skopeo(), "inspect", "--format", "{{.Digest}}"]
    if authfile:
        command += ["--authfile", str(authfile)]
    return subprocess.check_output([*command, "docker://" + image], text=True).strip()


def publish(archive, image, authfile):
    verified = receipt(archive)
    subprocess.run([*skopeo(), "copy", "--preserve-digests", "--all",
                    "--authfile", str(authfile), "oci-archive:" + str(archive),
                    "docker://" + image], check=True)
    observed = remote_digest(image, authfile)
    if observed != verified["image_digest"]:
        raise RuntimeError(f"registry digest changed: expected {verified['image_digest']}, received {observed}")
    return observed


def execution_config(config):
    # Docker 28 inspect serializes absent OCI fields as Go zero values.
    # Keep all non-default values and unknown fields in the comparison.
    defaults = {
        "Hostname": "", "Domainname": "", "User": "", "AttachStdin": False,
        "AttachStdout": False, "AttachStderr": False, "Tty": False,
        "OpenStdin": False, "StdinOnce": False, "Env": None, "Cmd": None,
        "Image": "", "Volumes": None, "WorkingDir": "", "Entrypoint": None,
        "OnBuild": None, "Labels": None,
    }
    return {key: value for key, value in config.items()
            if key not in defaults or value != defaults[key]}


def verify_import(image, artifact):
    # Local image stores may rewrite the config envelope and its image ID.
    # Verify the execution config and ordered filesystem content directly.
    if not isinstance(image.get("Config"), dict):
        raise RuntimeError("imported Docker image config differs from the OCI artifact")
    observed, expected = execution_config(image["Config"]), execution_config(artifact["config"])
    if observed != expected:
        missing = object()
        fields = sorted(key for key in observed.keys() | expected.keys()
                        if observed.get(key, missing) != expected.get(key, missing))
        raise RuntimeError("imported Docker image config differs from the OCI artifact: " + ", ".join(fields))
    rootfs = image.get("RootFS", {})
    if rootfs.get("Type") != artifact["rootfs"]["type"] or rootfs.get("Layers") != artifact["rootfs"]["diff_ids"]:
        raise RuntimeError("imported Docker image filesystem differs from the OCI artifact")
    if f"{image.get('Os')}/{image.get('Architecture')}" != artifact["platform"]:
        raise RuntimeError("imported Docker image platform differs from the OCI artifact")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["import", "publish"])
    parser.add_argument("archive", type=Path)
    parser.add_argument("--image", required=True)
    parser.add_argument("--authfile", type=Path)
    args = parser.parse_args()
    archive = args.archive.resolve()
    verified = receipt(archive)
    if args.operation == "publish":
        if args.authfile is None or not args.authfile.is_file() or args.authfile.stat().st_mode & 0o077:
            parser.error("publication requires a private registry authfile")
        print(publish(archive, args.image, args.authfile))
        return
    # Docker's classic image store needs a Docker archive. Convert only this
    # local transport representation; the published OCI artifact stays intact.
    with tempfile.TemporaryDirectory(prefix="crowdb-docker-import-") as directory:
        converted = Path(directory) / "docker.tar"
        subprocess.run([*skopeo(), "copy", "oci-archive:" + str(archive),
                        f"docker-archive:{converted}:{args.image}"], check=True)
        subprocess.run(["docker", "load", "--input", str(converted)], check=True)
        image = json.loads(subprocess.check_output(["docker", "image", "inspect", args.image], text=True))[0]
        verify_import(image, inspect(archive))


if __name__ == "__main__":
    main()
