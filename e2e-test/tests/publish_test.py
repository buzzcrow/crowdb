# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Failure exports retain evidence within a bounded upload budget."""

import gzip
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from publish import publish


class PublishTest(unittest.TestCase):
    def test_failed_browser_keeps_raw_trace_and_context_but_success_keeps_only_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            for name, status in (("failed", "failed"), ("passed", "pass")):
                case = source / name
                (case / "browser").mkdir(parents=True)
                (case / "case.json").write_text(json.dumps({"case": name}))
                (case / "timing.json").write_text(json.dumps({"status": status}))
                (case / "browser/trace.zip").write_bytes(b"complete binary trace")
                (case / "browser/error-context.md").write_text('password="test-value"')
                (case / "client-browser.log").write_text("original assertion")
            publish(source, root / "public")
            output = root / "public"
            self.assertEqual((output / "failed/browser/trace.zip").read_bytes(), b"complete binary trace")
            self.assertEqual((output / "failed/browser/error-context.md").read_text(), 'password="test-value"')
            self.assertTrue((output / "passed/timing.json").is_file())
            self.assertFalse((output / "passed/browser").exists())
            self.assertFalse((output / "passed/client-browser.log").exists())

    def test_service_logs_are_kept_without_mounted_data_builds_or_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            logs = source / "run/case/nodes/node0/data/log/kv"
            logs.mkdir(parents=True)
            (logs / "server.log").write_text("startup failure")
            with gzip.open(logs / "previous.log.gz", "wt") as stream:
                stream.write("previous failure")
            (logs.parent.parent / "disk.dat").write_bytes(b"mounted data")
            (source / "client").mkdir()
            (source / "client/build.log").write_text("build output")
            (source / "copied.log").symlink_to(logs / "server.log")
            self.assertEqual(publish(source, root / "public"), 2)
            output = root / "public/run/case/nodes/node0/data/log/kv"
            self.assertEqual((output / "server.log").read_text(), "startup failure")
            self.assertEqual((output / "previous.log").read_text(), "previous failure")
            self.assertFalse((output.parent.parent / "disk.dat").exists())
            self.assertFalse((root / "public/client").exists())
            self.assertFalse((root / "public/copied.log").exists())

    def test_large_logs_keep_the_tail_and_oversized_trace_is_not_corrupted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            (source / "browser").mkdir(parents=True)
            (source / "client-browser.log").write_bytes(b"irrelevant prefix LAST ERROR")
            (source / "browser/trace.zip").write_bytes(b"x" * 20)
            console = io.StringIO()
            with contextlib.redirect_stdout(console):
                publish(source, root / "public", text_limit=10, trace_limit=10)
            self.assertEqual((root / "public/client-browser.log").read_bytes(), b"LAST ERROR")
            self.assertFalse((root / "public/browser/trace.zip").exists())
            summary = json.loads((root / "public/export-summary.json").read_text())
            statuses = {row["path"]: row["status"] for row in summary["files"]}
            self.assertEqual(statuses["client-browser.log"], "truncated")
            self.assertEqual(statuses["browser/trace.zip"], "skipped")
            self.assertEqual(summary["counts"]["truncated"], 1)
            self.assertIn("truncated=1 skipped=1 read_failed=0", console.getvalue())

    def test_job_budget_prioritizes_the_client_error_before_service_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            source.mkdir()
            (source / "aaa-service.log").write_bytes(b"s" * 40)
            (source / "client-failure.log").write_bytes(b"important assertion")
            publish(source, root / "public", total_limit=24)
            self.assertEqual((root / "public/client-failure.log").read_bytes(), b"important assertion")
            summary = json.loads((root / "public/export-summary.json").read_text())
            self.assertLessEqual(summary["exported_bytes"], 24)
            self.assertEqual(summary["exported_bytes"], sum(
                path.stat().st_size for path in (root / "public").rglob("*")
                if path.is_file() and path.name != "export-summary.json"))

    def test_unreadable_log_does_not_drop_other_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            source.mkdir()
            (source / "blocked.log").write_text("unreadable")
            (source / "results.json").write_text('{"status":"failed"}')
            original = Path.open
            def opened(path, *args, **kwargs):
                if path.name == "blocked.log":
                    raise PermissionError("Permission denied")
                return original(path, *args, **kwargs)
            with patch.object(Path, "open", opened):
                self.assertEqual(publish(source, root / "public"), 1)
            summary = json.loads((root / "public/export-summary.json").read_text())
            self.assertIn("read_failed", [row["status"] for row in summary["files"]])
            self.assertTrue((root / "public/results.json").is_file())

    def test_export_cannot_rewrite_originals_or_reuse_a_stale_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "original"
            source.mkdir()
            for output in (source, source / "public", root):
                with self.assertRaises(ValueError):
                    publish(source, output)
            output = root / "public"
            output.mkdir()
            (output / "stale.log").write_text("old run")
            with self.assertRaises(ValueError):
                publish(source, output)
