<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Single-node container development

This page describes building and running CROWDB from source on a Linux amd64
development or CI host.

```sh
pixi run build-single-node-container
pixi run test-single-node-container
```

- Compilation runs on the host with the locked repository dependencies. Cargo
  and CMake reuse existing build outputs; npm uses its local download cache.
- The build stages release programs, their required shared libraries, UI and
  deployment files under `target/container-runtime`. Existing runtime data and
  credentials are never part of the Docker context.
- Docker only packages these files into the pinned Ubuntu runtime image. It
  does not install Pixi or compilers, compile source, or use a custom base image.
- Packaging checks dynamic linkage inside Ubuntu and verifies the staged
  revision/version against image metadata. A different host ABI must pass these
  checks and the container tests before its artifacts can be used.
- The default local image is `crowdb-iceberg-single-node:dev`. Set
  `CROWDB_CONTAINER_IMAGE` to build and test a separate candidate tag.

`pixi run stage-single-node-container` produces the runtime directory without
building a Docker image. The release workflow archives the verified directory
and packages those same files in its publish job, without recompiling them.
Docker Hub publication is manual; actual publication verification is deferred
until administrator preparation is complete.
