# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Stage real ELF dependencies and dynamically loaded provider symlinks."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PIXI = ["pixi", "run", "--as-is", "--manifest-path", str(ROOT / "pixi.toml")]
BINARIES = ("crowdb-monitor", "crowdb-paxos-kv-server", "crowdb-diskdb", "crowdb-diskio",
            "crowdb-chunkdb", "crowdb-chunk-kv-server", "crowdb-access-server", "crowdb-web")


class StagingTest(unittest.TestCase):
    def compile(self, source, output, *arguments):
        path = output.with_suffix(".c")
        path.write_text(source)
        compiler, = (ROOT / ".pixi/envs/default/bin").glob("*-conda-linux-gnu-cc")
        subprocess.run([*PIXI, str(compiler), str(path),
                        "-o", str(output), *arguments], check=True)
        subprocess.run([*PIXI, "patchelf", "--remove-rpath", str(output)], check=True)

    def test_core_libraries_and_provider_dependency_closure_survive_staging(self):
        for aliased in (False, True):
            with self.subTest(aliased=aliased), tempfile.TemporaryDirectory() as temporary:
                self.stage_fixture(Path(temporary), aliased)

    def stage_fixture(self, directory, aliased):
        root = directory / "repo"
        prefix = root / ".pixi/envs/default"
        prefix.mkdir(parents=True)
        (root / "target").mkdir()
        libraries = prefix / "lib"
        release = root / "target/release"
        if aliased:
            libraries.symlink_to(directory / "libraries", target_is_directory=True)
            release.symlink_to(directory / "release", target_is_directory=True)
        libraries.resolve().mkdir()
        release.resolve().mkdir()
        providers = libraries / "libibverbs"
        providers.mkdir()
        config = prefix / "etc/libibverbs.d"
        config.mkdir(parents=True)
        (config / "fixture.driver").write_text("driver fixture\n")
        definitions = {"libibverbs.so.1": "verbs", "librdmacm.so.1": "rdma",
                       "libcrypto.so.3": "crypto", "libprovider-dependency.so": "provider_dependency"}
        for name, function in definitions.items():
            self.compile(f"int {function}(void) {{ return 0; }}", libraries / name,
                         "-shared", "-fPIC", "-Wl,-soname," + name)
        self.compile("int ffi(void) { return 0; }", release / "libcrowdb_paxos_kv_client.so",
                     "-shared", "-fPIC", "-Wl,-soname,libcrowdb_paxos_kv_client.so")
        self.compile("int provider_dependency(void); int provider(void) { return provider_dependency(); }",
                     libraries / "libfixture.so.1", "-shared", "-fPIC", "-L" + str(libraries),
                     "-lprovider-dependency", "-Wl,-rpath," + str(libraries))
        (providers / "libfixture-rdmav99.so").symlink_to("../libfixture.so.1")
        source = "int verbs(void), rdma(void), crypto(void), ffi(void);\n" \
                 "int main(void) { return verbs() + rdma() + crypto() + ffi(); }"
        executable = directory / "node"
        self.compile(source, executable, "-L" + str(libraries), "-L" + str(release),
                     "-Wl,-l:libibverbs.so.1", "-Wl,-l:librdmacm.so.1", "-Wl,-l:libcrypto.so.3",
                     "-lcrowdb_paxos_kv_client")
        for name in BINARIES:
            target = root / "app/crowdb-diskio/build" if name == "crowdb-diskio" else release
            target.mkdir(parents=True, exist_ok=True)
            (target / name).write_bytes(executable.read_bytes())
            (target / name).chmod(0o755)
        tools = directory / "tools"
        tools.mkdir()
        git = tools / "git"
        git.write_text('#!/bin/sh\nprintf "%s\\n" "$CROWDB_STAGING_TEST_ROOT"\n')
        git.chmod(0o755)
        staged = directory / "staged"
        command = [*PIXI, "bash", "-c", 'PATH="$1:$PATH"; exec bash "$2" "$3"', "_",
                   str(tools), str(ROOT / "container/oci-image/collect-libs.sh"), str(staged)]
        subprocess.run(command, check=True, env=dict(os.environ, CROWDB_STAGING_TEST_ROOT=str(root)))
        for name in (*definitions, "libcrowdb_paxos_kv_client.so"):
            self.assertTrue((staged / "lib" / name).is_file(), name)
        provider = staged / "lib/libibverbs/libfixture-rdmav99.so"
        self.assertTrue(provider.is_file())
        self.assertFalse(provider.is_symlink())
        self.assertEqual((staged / "etc/libibverbs.d/fixture.driver").read_text(), "driver fixture\n")


if __name__ == "__main__":
    unittest.main()
