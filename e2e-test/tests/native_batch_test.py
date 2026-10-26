# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from scenarios import Case, run_cases


class NativeBatchTest(TestCase):
    def test_native_client_shares_browser_batch_after_cli_finishes_and_outlives_its_peer(self):
        native = Case("console.native", "native-client", native_client=True)
        browser = Case("console.browser", "browser-client", shell_browser="literal browser")
        cli = Case("console.cli.container", "cli-client", cli_replay="bound")
        active, events = set(), []
        with TemporaryDirectory() as directory:
            root = Path(directory)

            class Fixture:
                def __init__(self, case):
                    self.name = case.name
                    self.artifacts = root / case.name
                    self.artifacts.mkdir()

                def __enter__(self):
                    events.append(("start", self.name))
                    active.add(self.name)
                    return self

                def __exit__(self, *_):
                    events.append(("close", self.name))
                    active.remove(self.name)

                def client(self, *_):
                    self.browser()

                def browser(self, *_):
                    self_owner.assertEqual(active, {native.name, browser.name})
                    events.append(("client", self.name))

                def replay(self, *_):
                    self_owner.assertEqual(active, {cli.name})
                    events.append(("client", self.name))

            self_owner = self
            with patch("scenarios.select_cases", return_value=[native, browser, cli]), \
                 patch("scenarios.allocate_cluster", side_effect=lambda case, *_: Fixture(case)):
                run_cases("image", root, root, 3)
        self.assertEqual(events[:3], [("start", cli.name), ("client", cli.name), ("close", cli.name)])
        self.assertEqual(events[3:5], [("start", native.name), ("start", browser.name)])
        self.assertLess(events.index(("client", native.name)), events.index(("close", browser.name)))
        self.assertLess(events.index(("client", browser.name)), events.index(("close", native.name)))
        self.assertLess(events.index(("close", cli.name)), events.index(("start", native.name)))
        self.assertEqual(active, set())
