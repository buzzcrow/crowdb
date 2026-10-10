# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import unittest

from kv_topology import ready_nodes


class TopologyTest(unittest.TestCase):
    def test_bare_store_omits_groups_but_grouped_store_must_have_exact_groups(self):
        profiles = {
            "existing": {"stores": [{"store_id": 0, "groups": [{"group_id": 1}]}]},
            "joining": {"stores": [{"store_id": 2}]},
        }

        def wait(action, predicate, _description):
            actual = action()
            self.assertTrue(predicate(actual))
            wrong = {"stores": [{"store_id": actual["stores"][0]["store_id"],
                                  "groups": [{"group_id": 9}]}]}
            self.assertFalse(predicate(wrong))

        ready_nodes(("existing", "joining"), ((0,), (2,)), ((1,), ()),
                    lambda origin, _path: profiles[origin], wait)
