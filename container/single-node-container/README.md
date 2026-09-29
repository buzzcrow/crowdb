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
relative `core` or `core.*` patterns write in the crashing process's working
directory. The container runs the monitor and managed children from the private
`/opt/crowdb/data/crash` directory. On monitor startup and after a child is
reaped, it removes older regular `core` files and keeps the newest one.
The directory has mode `0700`; symlinks named `core.*` are not followed or
deleted. This is one-core retention, not a promise that the host creates a
volume file. Other relative filename patterns are outside this retention rule.

For a host with a relative `core` pattern, add a size bound to `docker run`:

```sh
--ulimit core=1073741824:1073741824
```

The example bounds each core to 1 GiB. A small bound may truncate a dump and
make some stack frames unavailable. Core collection can also be suppressed by
the host's dumpability policy, including for executables with file capabilities.
The container never changes `core_pattern` or the host's dumpability policy.

- On a systemd-coredump host, use `coredumpctl list crowdb-kv-server` to find
  the host report, then `coredumpctl --output=/private/core dump
  crowdb-kv-server` as an authorized host user to export it. Check that the
  result is readable and nonempty before symbolization.
- On an Ubuntu Apport host, find the matching report in `/var/crash` on the
  Docker host. Create a private directory, then run `sudo apport-unpack
  /var/crash/REPORT.crash /private/crowdb-core/unpacked`. The extracted
  `CoreDump` is the file to pass to the symbolizer. Apport reports may be
  readable only by the host administrator; preserve the private permissions
  when granting the debugging user access. A pipe pattern does not create a
  volume file. Apport may fail to resolve a CROWDB executable because its
  `/opt/crowdb/bin` path exists only inside the container; it may also ignore
  executables outside host distribution packages. Check the host's Apport log
  when no report appears.
  If the report or `CoreDump` is absent, collection is unavailable for that
  crash; do not substitute a log or an unrelated dump.
- On Docker Desktop, inspect the Linux VM's collector. The desktop host's
  native crash directory is not the container's core directory.

Core dumps can contain credentials and user data. Keep exports in a private
directory and do not attach them to ordinary logs or issues. If the release
included the optional symbol archive, use the exact image and matching archive
to show source-line stacks:

```sh
pixi run -- python tools/symbolize-container-core.py \
  --image 'docker.io/crowdb/crowdb-iceberg:<tag>' \
  --symbols '/private/path/crowdb-symbols-<tag>-git-<revision>-linux-amd64.tar.zst' \
  --binary crowdb-monitor --core /private/path/core
```

The tool copies binaries from a stopped container into a temporary private
directory, verifies source revision, version and SHA-256 hashes, then runs
`gdb` without printing frame arguments. Use the crashed child binary instead
of `crowdb-monitor` for a child core. The temporary binaries are removed after
the stack is shown; the core stays at the path supplied by the operator.
The exact-build source-line check on a supported file-based collector remains
tracked by R188.

Collector behavior follows the [Linux core pattern documentation](https://docs.kernel.org/admin-guide/sysctl/kernel.html),
[systemd-coredump manual](https://www.freedesktop.org/software/systemd/man/250/systemd-coredump.socket.html),
and [Ubuntu Apport documentation](https://ubuntu.com/project/docs/contributors/debugging/apport/).
