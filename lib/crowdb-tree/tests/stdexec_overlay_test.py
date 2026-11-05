# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
ENGINE = Path(sys.argv.pop(1)).resolve()
VENDOR = Path(sys.argv.pop(1)).resolve()
SPEC = importlib.util.spec_from_file_location("prepare_stdexec", ENGINE / "cmake/prepare_stdexec.py")
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)


class StdexecOverlay(unittest.TestCase):
    def test_generation_is_idempotent_and_does_not_mutate_vendor(self):
        original = (VENDOR / "exec/static_thread_pool.hpp").read_bytes()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "overlay"
            PREPARE.prepare(VENDOR, output)
            patched = output / "include/exec/static_thread_pool.hpp"
            self.assertIn(b"pool_thread_factory", patched.read_bytes())
            self.assertNotEqual(original, patched.read_bytes())
            first = patched.stat().st_mtime_ns
            PREPARE.prepare(VENDOR, output)
            self.assertEqual(first, patched.stat().st_mtime_ns)
            self.assertEqual(original, (VENDOR / "exec/static_thread_pool.hpp").read_bytes())

    def test_changed_baseline_preserves_previous_valid_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            vendor = root / "vendor"
            output = root / "overlay"
            shutil.copytree(VENDOR, vendor)
            PREPARE.prepare(vendor, output)
            stamp = (output / "stamp").read_bytes()
            target = vendor / "exec/detail/bwos_lifo_queue.hpp"
            target.write_bytes(target.read_bytes() + b"\n// changed baseline\n")
            with self.assertRaisesRegex(RuntimeError, "baseline changed"):
                PREPARE.prepare(vendor, output)
            self.assertEqual(stamp, (output / "stamp").read_bytes())


if __name__ == "__main__":
    unittest.main()
