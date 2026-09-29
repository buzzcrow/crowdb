<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Debugging a CROWDB Crash

Use the core from the crashed process and the exact executable build that
produced it. Core files can contain credentials and user data. Keep them in a
private directory, do not attach them to ordinary logs or issues, and remove
them when the investigation is complete.

## 1. Find the collector

On the machine running Docker or a bare-metal service, inspect:

```sh
cat /proc/sys/kernel/core_pattern
cat /proc/sys/fs/suid_dumpable
```

- A relative name such as `core.%e.%p.%t` writes in the crashing process's
  working directory. The name must start with `core` for the single-node
  container's retention rule to recognize it.
- A leading `|` sends the dump to a host-side program. Ubuntu Apport usually
  writes a report under `/var/crash`; `apport-unpack REPORT.crash OUTPUT_DIR`
  extracts its `CoreDump`. `systemd-coredump` uses `coredumpctl list` and
  `coredumpctl --output=FILE dump`. A collector may reject a container crash.
  If there is no report, there is no core to extract.
- An absolute file pattern uses the crashing process's mount namespace and
  root. Check the actual destination and access policy on that host.

The container's data volume does not override a host collector. During the
2026-09-29 Ubuntu development-host check, Apport received the crash but could
not resolve `/opt/crowdb/bin/crowdb-kv-server` in the host filesystem.
The image contains the executable; the failed lookup happens in Apport. No
CROWDB core was saved by that test.

## 2. Configure a file-based collector on a dedicated development host

This is a **host-wide change**, not a Docker setting. It replaces Apport's
automatic crash reports for all processes on that host. Other programs with
a nonzero core limit may write private core files in their own working
directories. Do not apply it to a shared or production host without an
operator decision. Run these commands on the Docker host, never inside the
container. Record the original `core_pattern` and Apport service state first.

On an Ubuntu host where `apport.service` owns `core_pattern`, a persistent
file-based setup is:

```sh
cat /proc/sys/kernel/core_pattern
systemctl is-enabled apport.service
sudo systemctl disable --now apport.service
printf 'kernel.core_pattern=core.%%e.%%p.%%t\nfs.suid_dumpable=0\n' |
  sudo tee /etc/sysctl.d/99-crowdb-core.conf
sudo sysctl -w 'kernel.core_pattern=core.%e.%p.%t'
sudo sysctl -w fs.suid_dumpable=0
cat /proc/sys/kernel/core_pattern
```

`apport.service` sets the pipe pattern when it starts, so a sysctl file alone
does not keep the file pattern after a restart. Verify `core_pattern` again
after reboot. `fs.suid_dumpable=0` permits an ordinary process to write a
relative core; executables with file capabilities may still be excluded.
The container currently gives file capabilities to `crowdb-iceberg` and
`crowdb-access-server` for low ports, so do not assume those two will produce
cores under this setting. Verify the particular crashed service.

For a one-time investigation, stop Apport and apply the two `sysctl -w`
commands without creating the sysctl file or disabling the service. Restart
Apport after the investigation to restore its collector. For the persistent
setup above, restore the prior Ubuntu behavior with:

```sh
sudo unlink /etc/sysctl.d/99-crowdb-core.conf
sudo systemctl enable --now apport.service
cat /proc/sys/kernel/core_pattern
```

If this host used another collector originally, restore its recorded service
state and exact original pattern instead of starting Apport.

## 3. Bound and locate the core

For the single-node container, add a nonzero per-process bound when running
Docker:

```sh
--ulimit core=1073741824:1073741824
```

The monitor and managed children run from the private mounted directory
`/opt/crowdb/data/crash` (mode `0700`). With a relative `core.*` pattern, a
dump lands there. The monitor keeps the newest regular `core` file after
startup or child recovery. Inspect the directory with `docker exec`; export
the selected core to a private host directory with `docker cp` for GDB. A
1 GiB limit can truncate a larger dump. A piped collector ignores this
`RLIMIT_CORE` bound and uses its own policy.

For a bare-metal process, set a writable private working directory and a
nonzero core limit in the launcher. A shell launch can use
`ulimit -c 1048576` (KiB); a systemd unit can use `WorkingDirectory=` and
`LimitCORE=1G`. Verify the actual process limit in `/proc/PID/limits`.
There is no container monitor retention rule for bare-metal cores.

## 4. Open a container core with exact-build symbols

Use the image that ran the crashed process and its matching optional symbol
archive. From the CROWDB checkout:

```sh
pixi run -- python tools/symbolize-container-core.py \
  --image 'docker.io/crowdb/crowdb-iceberg:<tag>' \
  --symbols '/private/crowdb-symbols-<tag>-git-<revision>-linux-amd64.tar.zst' \
  --binary crowdb-kv-server \
  --core /private/core.crowdb-kv-ser.PID.TIME
```

The tool checks the image revision, version and binary SHA-256 hashes against
the archive, places the `.debug` files beside the matching stripped ELF files,
and starts GDB with the image's shared libraries. GNU debuglinks let GDB load
the separate symbols. Replace `--binary` with the actual crashed CROWDB
binary. If the archive was not released, build the exact Git tag with
`CROWDB_PACKAGE_SYMBOLS=1 pixi run build-single-node-container` and package
its local `target/container-symbols` directory for the helper:

```sh
pixi run -- bash -c 'tar -C target/container-symbols -cf - . | zstd -q -o /private/crowdb-symbols-local.tar.zst'
```

Use that archive with the image produced by the same build. A different
commit or build is not a safe substitute.

For an interactive session, keep private copies of the same image's `bin/`
and `lib/`, put each matching `.debug` file beside its stripped binary or
library, and run:

```sh
pixi run -- gdb -q /private/runtime/bin/crowdb-kv-server /private/core
(gdb) set solib-search-path /private/runtime/lib
(gdb) sharedlibrary
(gdb) set print frame-arguments none
(gdb) thread apply all bt
```

Do not use a newly built binary against an older core just because its version
string is unchanged.

## 5. Open a bare-metal core

Bare-metal deployment keeps the binary's debug information. Use the exact
binary and shared libraries that were running when the core was made; no
separate container symbol archive is needed:

```sh
pixi run -- gdb -q /path/to/exact/crowdb-kv-server /private/core
(gdb) set solib-search-path /path/to/exact/lib
(gdb) sharedlibrary
(gdb) set print frame-arguments none
(gdb) thread apply all bt
```

If the executable or a shared library has been replaced since the crash,
recover the original build before trusting the stack. A core from one build
must not be interpreted with symbols from another.

References: [Linux core dump rules](https://man7.org/linux/man-pages/man5/core.5.html),
[kernel `core_pattern`](https://docs.kernel.org/admin-guide/sysctl/kernel.html),
and [Ubuntu Apport](https://documentation.ubuntu.com/project/contributors/debugging/apport/).
