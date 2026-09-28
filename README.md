<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

[![CI](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml/badge.svg)](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

CROWDB is a distributed storage platform for Iceberg tables, AI datasets, and
S3 objects. Each access model keeps its own semantics while sharing one storage
core for distributed state, placement, protection, streaming, and recovery.

- **Iceberg:** native catalog and FileIO, implemented.
- **S3:** core HTTP object operations, implemented.
- **Dataset:** native access and direct GPU delivery, in design.

## Why CROWDB?

Storage bottlenecks move—from disks to CPUs, networks, and data movement—but
system boundaries tend to stay. A change that crosses a metadata service, an
object gateway, and a separate storage engine can become a negotiation between
systems rather than an improvement to one data path.

CROWDB owns enough of that path to change it when workloads and hardware change.
S3 objects, Iceberg tables, and planned AI datasets are native access models
built over shared infrastructure, not conventions layered on top of one
another. The goal is not to claim novelty for Paxos, WALs, trees, or erasure
coding; it is to make their contracts agree on durability, placement, bounded
buffers, and recovery.

Read the full motivation in
[Why we’re building CROWDB](https://buzzcrow.github.io/blog/why-we-are-building-crowdb/).

## Development preview

Version `0.1.0-dev` is being prepared for public evaluation on Linux amd64.
Use disposable data. Production use and on-disk upgrade compatibility are not
supported, and Dataset and direct GPU delivery are not available yet.

- [Project homepage](https://crowdb.dev/)
- [Quick start](https://crowdb.dev/docs/quickstart/)
- [Documentation](https://crowdb.dev/docs/)
- [Architecture](https://crowdb.dev/docs/architecture/)
- [Demos](https://crowdb.dev/demo/)

## Repository

The repository contains the storage implementation, tests, and source design
documents. Start with:

- [Contributing](CONTRIBUTING.md) for the development environment and workflow.
- [Documentation index](doc/doc_index.md) for subsystem designs.
- [Backlog](doc/backlog/backlog.md) for current delivery scope.
- [Single-node container](container/single-node-container/README.md) for image
  development and packaging.

## AI-assisted development

The code in this project was written with AI assistance. The architecture,
naming, module boundaries, and trade-offs remain human choices. AI is the
compiler. The intent is mine.

## License

Licensed under the [Apache License 2.0](LICENSE).
