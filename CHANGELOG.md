<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Changelog

CROWDB is preparing `0.2.2`. This is a development version, not a
production release or a compatibility promise.

CROWDB does not yet maintain compatibility for persisted data, WAL, metadata,
or other on-disk formats. A newer checkout may be unable to read data created by
an older checkout. Use disposable data only.

Changelog history begins with the first public Docker preview. Development work
before that baseline remains available in Git history and project requirements
but is intentionally not reconstructed as released change history.

The changelog will follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
from that baseline. Version tags alone do not imply Semantic Versioning
compatibility until the project explicitly adopts and documents a compatibility
policy.

## [Unreleased]

### 0.2.2 preparation

- Build service binaries before stream and console acceptance tests in CI.
- Wait for stopped KV children before deleting console workspaces during reset.

### 0.2.1 preparation

- S3 and Iceberg own separate chunk types, storage policies, and write pools
  behind one access-server process.
- Explicit single-node test and three-node production protection profiles,
  with strip-level mirror and EC I/O and repairable degraded EC placement.

## [0.1.0]

- Single-node Linux amd64 container with native Iceberg REST catalog and FileIO,
  backed by CROWDB metadata, chunk storage and disk services.
- Persistent bootstrap, generated client credentials, health checks, bounded
  service recovery and restart validation.
- S3 object access through an optional published endpoint.
- Host builds and runtime-only container packaging, with a manual Docker Hub
  publication workflow for version and commit tags, signatures, SBOM and provenance.

The preview image is `crowdb/crowdb-iceberg:0.1.0`; no published digest is
recorded here. Multi-node deployment,
production hardening and data-format upgrades are outside this release.

See the container deployment files for supported startup, persistence,
credentials and recovery behavior.
