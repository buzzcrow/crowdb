# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Publish, attest and tag the exact image tested by both runtime gates."""

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

from artifact import receipt
from builder import tool
from transfer import publish, remote_digest, skopeo

REPOSITORIES = ("docker.io/crowdb/crowdb-node", "docker.io/crowdb/crowdb-iceberg", "docker.io/crowdb/crowdb-s3",
                "docker.io/crowdb/crowdb-dataset")


def check_source(verified, tag):
    revision = os.environ["GITHUB_SHA"]
    branch = os.environ["GITHUB_REF_NAME"]
    if branch not in (f"release/{tag}", f"release/v{tag}") or tag != verified["version"]:
        raise ValueError("release branch/version differs from artifact")
    if revision != verified["revision"]:
        raise ValueError("release source differs from verified artifact")
    head = subprocess.check_output(["git", "ls-remote", "origin", "refs/heads/" + branch], text=True).split()[0]
    if head != revision:
        raise ValueError("release branch moved after verification")
    for key in ("DOCKER_VERIFIED_DIGEST", "CONTAINERD_VERIFIED_DIGEST"):
        if os.environ.get(key) != verified["image_digest"]:
            raise ValueError(f"missing or different runtime gate digest: {key}")


def provenance(verified):
    return {"buildDefinition": {"buildType": "https://crowdb.dev/build/oci/v1",
            "externalParameters": {"revision": verified["revision"], "version": verified["version"], "platform": verified["platform"]},
            "internalParameters": {"tools": verified["tools"], "archive_sha256": verified["archive_sha256"]},
            "resolvedDependencies": [{"uri": verified["base_image"].split("@")[0],
                                      "digest": {"sha256": verified["base_image"].split("@sha256:")[1]}}]},
            "runDetails": {"builder": {"id": "https://github.com/buzzcrow/crowdb/actions/workflows/release-container.yml"},
                           "metadata": {"invocationId": os.environ.get("GITHUB_RUN_ID", "local")}}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    verified = receipt(args.archive)
    check_source(verified, args.tag)
    username, token = os.environ.get("DOCKERHUB_USERNAME"), os.environ.get("DOCKERHUB_TOKEN")
    if not username or not token:
        parser.error("registry publication credentials are required")
    with tempfile.TemporaryDirectory(prefix="crowdb-publication-") as directory:
        directory = Path(directory)
        auth = directory / "config.json"
        subprocess.run([*skopeo(), "login", "--authfile", str(auth), "--username", username,
                        "--password-stdin", "docker.io"], input=token, text=True, check=True)
        auth.chmod(0o600)
        # Cosign consumes the same private auth file using Docker's config lookup.
        environment = dict(os.environ, DOCKER_CONFIG=str(directory))
        sbom = directory / "sbom.spdx.json"
        subprocess.run([tool("syft"), "oci-archive:" + str(args.archive.resolve()), "-o", "spdx-json=" + str(sbom)], check=True)
        predicate = directory / "provenance.json"
        predicate.write_text(json.dumps(provenance(verified)) + "\n")
        check_source(verified, args.tag)
        for repository in REPOSITORIES:
            digest = publish(args.archive.resolve(), repository + ":" + args.tag, auth)
            reference = repository + "@" + digest
            subprocess.run([tool("cosign"), "sign", "--yes", reference], env=environment, check=True)
            for kind, path in (("spdxjson", sbom), ("https://slsa.dev/provenance/v1", predicate)):
                subprocess.run([tool("cosign"), "attest", "--yes", "--type", kind,
                                "--predicate", str(path), reference], env=environment, check=True)
        # Moving aliases only follow successful immutable-image verification and
        # attestations. Registry writes across repositories are not transactional.
        check_source(verified, args.tag)
        for repository in REPOSITORIES:
            subprocess.run([*skopeo(), "copy", "--preserve-digests", "--authfile", str(auth),
                            "docker://" + repository + ":" + args.tag, "docker://" + repository + ":latest"], check=True)
            if remote_digest(repository + ":latest", auth) != verified["image_digest"]:
                raise RuntimeError("moving tag differs from verified OCI digest")
        print(verified["image_digest"])


if __name__ == "__main__":
    main()
