# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Validate an OCI archive without extracting untrusted paths."""

import argparse
import hashlib
import json
import tarfile
from pathlib import Path

MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
LAYERS = {"application/vnd.oci.image.layer.v1.tar", "application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.oci.image.layer.v1.tar+zstd"}


def checksum(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def inspect(path):
    with tarfile.open(path, "r:*") as archive:
        members = {}
        for member in archive:
            name = member.name.removeprefix("./")
            if member.isdir():
                continue
            if not member.isfile() or name.startswith("/") or ".." in Path(name).parts or name in members:
                raise ValueError(f"invalid or duplicate OCI archive member: {name}")
            members[name] = member

        def read(name):
            member = members[name]
            if member.size > 16 * 1024 * 1024:
                raise ValueError(f"oversized OCI metadata: {name}")
            return archive.extractfile(member).read()

        def blob(descriptor, metadata=False):
            algorithm, digest = descriptor["digest"].split(":", 1)
            if algorithm != "sha256" or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("expected a SHA256 OCI descriptor")
            member = members[f"blobs/sha256/{digest}"]
            if member.size != descriptor["size"]:
                raise ValueError("OCI descriptor size mismatch")
            with archive.extractfile(member) as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                    raise ValueError("OCI blob digest mismatch")
            return json.loads(read(member.name.removeprefix("./"))) if metadata else None

        if json.loads(read("oci-layout")) != {"imageLayoutVersion": "1.0.0"}:
            raise ValueError("unsupported OCI layout")
        index = json.loads(read("index.json"))
        if index.get("schemaVersion") != 2 or len(index["manifests"]) != 1:
            raise ValueError("expected one platform manifest")
        descriptor = index["manifests"][0]
        if descriptor["mediaType"] != MANIFEST:
            raise ValueError("expected OCI image manifest media type")
        if "platform" in descriptor and (descriptor["platform"].get("os"), descriptor["platform"].get("architecture")) != ("linux", "amd64"):
            raise ValueError("OCI index platform differs from the supported image")
        manifest = blob(descriptor, True)
        if manifest.get("schemaVersion") != 2 or manifest.get("mediaType") != MANIFEST or manifest["config"]["mediaType"] != CONFIG:
            raise ValueError("invalid OCI manifest/config media type")
        config = blob(manifest["config"], True)
        if (config["os"], config["architecture"]) != ("linux", "amd64"):
            raise ValueError("only Linux amd64 images are supported")
        if config["rootfs"].get("type") != "layers" or len(config["rootfs"]["diff_ids"]) != len(manifest["layers"]):
            raise ValueError("OCI rootfs/layer count mismatch")
        for layer in manifest["layers"]:
            if layer["mediaType"] not in LAYERS:
                raise ValueError("invalid OCI layer media type")
            blob(layer)
        return {"image_digest": descriptor["digest"], "platform": "linux/amd64",
                "config_digest": manifest["config"]["digest"], "archive_sha256": checksum(path), "labels": config.get("config", {}).get("Labels", {}),
                "config": config.get("config", {})}


def receipt(path):
    result = inspect(path)
    saved = json.loads(Path(str(path) + ".json").read_text())
    for key in ("image_digest", "config_digest", "platform", "archive_sha256"):
        if result[key] != saved[key]:
            raise ValueError(f"artifact receipt mismatch: {key}")
    for key, label in (("revision", "org.opencontainers.image.revision"), ("version", "org.opencontainers.image.version"),
                       ("runtime_sha256", "org.crowdb.runtime.sha256")):
        if saved[key] != result["labels"][label]:
            raise ValueError(f"artifact source metadata mismatch: {key}")
    return saved


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--receipt", action="store_true")
    args = parser.parse_args()
    print(json.dumps(receipt(args.archive) if args.receipt else inspect(args.archive), indent=2))
