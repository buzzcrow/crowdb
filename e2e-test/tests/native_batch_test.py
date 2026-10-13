# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from scenarios import Case, run_cases


class NativeBatchTest(TestCase):
    def test_native_client_joins_browser_batch_before_any_client_and_outlives_its_peer(self):
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
                    self_owner.assertEqual(active, {native.name, browser.name, cli.name})
                    events.append(("client", self.name))

                def replay(self, *_):
                    self.browser()

            self_owner = self
            with patch("scenarios.select_cases", return_value=[native, browser, cli]), \
                 patch("scenarios.allocate_cluster", side_effect=lambda case, *_: Fixture(case)):
                run_cases("image", root, root, 3)
        self.assertEqual(events[:3], [("start", native.name), ("start", browser.name), ("start", cli.name)])
        self.assertLess(events.index(("client", native.name)), events.index(("close", browser.name)))
        self.assertLess(events.index(("client", browser.name)), events.index(("close", native.name)))
        self.assertLess(events.index(("client", cli.name)), events.index(("close", native.name)))
        self.assertEqual(active, set())
