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
building a Docker image. To prepare a release from a clean, current `main`
checkout, preview the patch bump and then run it explicitly:

```sh
pixi run -- python tools/release.py --dry-run
pixi run -- python tools/release.py --execute
```

`--bump minor` and `--bump major` select larger version changes. The script
updates every version manifest, commits and tags the release, atomically pushes
`main` and the tag, creates a draft GitHub Release, then dispatches the existing
verified DockerHub workflow. Add `--symbols` to either command to include the
large exact-build symbol archive; the default release skips it. Execution
requires authenticated `gh` and GitHub
permission to push `main`; the dry run changes no files or remote state. The
workflow archives the verified runtime, then packages those same files in its
publish job without recompiling them. With `--symbols`, it also archives
exact-build symbols from that build and attaches
`crowdb-symbols-<tag>-git-<revision>-linux-amd64.tar.zst` to the GitHub
Release. The workflow publishes the GitHub Release after the Docker image and
signature succeed. If the optional symbol upload fails, the published release
remains available and the workflow reports a warning.

## Crash collection boundary

The image does not configure the host's Linux core collector. Inspect
`/proc/sys/kernel/core_pattern` on the Docker host before expecting a dump in
the mounted data volume. A leading `|` sends a crash to a host-side collector;
relative file patterns write in the crashing process's working directory.
The container does not currently set a private core working directory, a
core size limit, or dump retention. Release debug symbols are available when
the release was run with `--symbols`. Do not assume `/opt/crowdb/data` contains
a core after a crash.

- On a systemd-coredump host, use `coredumpctl list` and `coredumpctl dump`
  on the host to locate and export a captured dump.
- On an Ubuntu Apport host, use the host's Apport report and core extraction
  workflow. A pipe pattern does not create a volume file.
- On Docker Desktop, inspect the Linux VM's collector. The desktop host's
  native crash directory is not the container's core directory.

Core dumps can contain credentials and user data. Store exports privately,
apply host retention policy, and match the exact image revision and binary
build when symbolizing. The required bounded volume collection and source-line
symbolization check remain tracked by R188.

Collector behavior follows the [Linux core pattern documentation](https://docs.kernel.org/admin-guide/sysctl/kernel.html),
[systemd-coredump manual](https://www.freedesktop.org/software/systemd/man/250/systemd-coredump.socket.html),
and [Ubuntu Apport documentation](https://ubuntu.com/project/docs/contributors/debugging/apport/).
