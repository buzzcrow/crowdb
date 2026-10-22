# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fixture import LABEL
import oci_reference_runtime as runtime
from scenarios import case_nodes, case_resources, select_cases


class OciReferenceOwnershipTest(unittest.TestCase):
    def cluster(self, implementation="docker"):
        return SimpleNamespace(implementation=implementation, run_id="owned", image_id="expected",
                               expected_names=["owned-node1"], node_count=1,
                               runtime_accepted=False, command=Mock())

    def test_preexisting_scope_is_rejected_without_adoption_or_effects(self):
        cluster = self.cluster()
        with patch.object(runtime, "inventory", return_value=[{"Name": "foreign"}]):
            with self.assertRaisesRegex(ValueError, "already contains"):
                runtime.start(cluster)
        self.assertFalse(cluster.runtime_accepted)
        cluster.command.assert_not_called()

    def test_unaccepted_start_does_not_authorize_cleanup(self):
        cluster = self.cluster()
        runtime.close(cluster)
        cluster.command.assert_not_called()

    def test_changed_image_is_rejected_before_any_cleanup_effect(self):
        cluster = self.cluster()
        cluster.runtime_accepted = True
        foreign = {"Name": "/owned-node1", "Image": "replaced",
                   "Config": {"Labels": {LABEL: "owned"}}}
        with patch.object(runtime, "inventory", return_value=[foreign]):
            with self.assertRaisesRegex(ValueError, "ownership or image"):
                runtime.close(cluster)
        cluster.command.assert_not_called()

    def test_unrelated_network_attachment_prevents_container_and_network_removal(self):
        cluster = self.cluster()
        cluster.runtime_accepted = True
        container = {"Id": "node-id", "Name": "/owned-node1", "Image": "expected",
                     "Config": {"Labels": {LABEL: "owned"}}}
        network = {"Id": "bridge-id", "Containers": {"foreign-id": {}}}
        with patch.object(runtime, "inventory", side_effect=lambda _, kind: {
                "container": [container], "network": [network]}[kind]):
            with self.assertRaisesRegex(ValueError, "unrelated attachment"):
                runtime.close(cluster)
        cluster.command.assert_not_called()

    def test_compose_project_label_alone_does_not_authorize_an_unrelated_service(self):
        cluster = self.cluster("compose")
        container = {"Image": "expected", "Config": {"Labels": {
            runtime.PROJECT: "owned", "com.docker.compose.service": "foreign"}}}
        with self.assertRaisesRegex(ValueError, "unrelated service"):
            runtime.validate_container(cluster, container)

    def test_compose_service_label_does_not_authorize_an_unexpected_container(self):
        cluster = self.cluster("compose")
        container = {"Name": "/foreign", "Image": "expected", "Config": {"Labels": {
            runtime.PROJECT: "owned", "com.docker.compose.service": "node"}}}
        with self.assertRaisesRegex(ValueError, "unexpected reference container name"):
            runtime.validate_container(cluster, container)

    def test_missing_owned_storage_rejects_recreation_before_container_removal(self):
        cluster = self.cluster()
        container = {"Id": "node-id", "Name": "/owned-node1", "Image": "expected",
                     "Config": {"Labels": {LABEL: "owned"}}}
        with patch.object(runtime, "inventory", side_effect=lambda _, kind: {
                "container": [container], "network": [], "volume": []}[kind]):
            with self.assertRaisesRegex(ValueError, "retained volumes"):
                runtime.recreate(cluster)
        cluster.command.assert_not_called()

    def test_unexpected_owned_volume_rejects_cleanup_before_any_removal(self):
        cluster = self.cluster()
        cluster.runtime_accepted = True
        container = {"Id": "node-id", "Name": "/owned-node1", "Image": "expected",
                     "Config": {"Labels": {LABEL: "owned"}}}
        with patch.object(runtime, "inventory", side_effect=lambda _, kind: {
                "container": [container], "network": [], "volume": [{"Name": "unrelated"}]}[kind]):
            with self.assertRaisesRegex(ValueError, "unrelated volume"):
                runtime.close(cluster)
        cluster.command.assert_not_called()

    def test_all_reference_examples_have_registered_owners_and_resource_reservations(self):
        cases = select_cases(scope="deployment.reference")
        self.assertEqual({case.oci_reference for case in cases},
                         {"docker-one", "docker-three", "compose-one", "compose-three"})
        self.assertEqual({case.oci_reference for case in cases if case.oci_editable_ui},
                         {"docker-one", "docker-three"})
        self.assertEqual(len(cases), 6)
        for case in cases:
            count = 1 if case.oci_reference.endswith("one") else 3
            self.assertEqual(case_nodes(case), 4 if case.oci_address_recovery else count)
            cpu, memory = case_resources(case, 0.25)
            self.assertGreaterEqual(cpu, count)
            self.assertGreaterEqual(memory, count * 1536 + 512)

    def test_pending_free_regression_has_one_owned_normal_node_case(self):
        cases = select_cases(scope="console.cli.container",
                             case_name="console::cli_container::pending_free_accounting")
        self.assertEqual(len(cases), 1)
        self.assertTrue(cases[0].cli_workload)
        self.assertEqual(cases[0].oci_reference, "docker-one")
        self.assertEqual(case_nodes(cases[0]), 1)

    def test_cold_recovery_reserves_the_later_nonvoter_without_reducing_existing_nodes(self):
        case = select_cases(case_name='deployment::reference::direct_docker_three_nodes')[0]
        self.assertTrue(case.oci_address_recovery)
        self.assertEqual(case_nodes(case), 4)
        self.assertEqual(case_resources(case, .25), (3.75, 3 * 1536 + 768 + 512))

    def test_recreated_names_reuse_exact_retained_volumes_on_the_accepted_new_network(self):
        cluster = self.cluster()
        cluster.expected_names = ['owned-node1-cold']
        cluster.reference_volumes = ['owned-node1-data']
        cluster.reference_network = 'owned-recovered'
        cluster.port_base, cluster.image, cluster.containers = 20000, 'expected', []
        runtime.create_docker_nodes(cluster, [0])
        self.assertEqual(runtime.expected_volumes(cluster), {'owned-node1-data'})
        create = cluster.command.call_args.args
        self.assertIn('type=volume,source=owned-node1-data,target=/opt/crowdb/data', create)
        self.assertEqual(create[create.index('--network') + 1], 'owned-recovered')
        self.assertNotIn('owned-node1-cold-data', create)

    def test_cold_network_scope_still_rejects_foreign_names_and_attachments(self):
        cluster = self.cluster()
        cluster.reference_networks = {'owned', 'owned-recovered'}
        for name, peers in [('foreign', {}), ('owned-recovered', {'foreign-id':{}})]:
            with patch.object(runtime, 'inventory', return_value=[{'Name':name,'Containers':peers}]):
                with self.assertRaises(ValueError):
                    runtime.validate_networks(cluster, [{'Id':'owned-id'}])
        cluster.command.assert_not_called()
