// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { diskBranch } from '../fixtures/capacity';
import type { Page, APIRequestContext } from '@playwright/test';

async function verifyOwnerProjection(page: Page, request: APIRequestContext, hardware: any) {
  const servicesResponse = await request.get('/api/servers');
  expect(servicesResponse.ok(), await servicesResponse.text()).toBe(true);
  const services = (await servicesResponse.json()).filter((row: { service_type: string }) => row.service_type === 'diskdb');
  const instancesResponse = await request.get('/api/diskdb/instances');
  expect(instancesResponse.ok(), await instancesResponse.text()).toBe(true);
  const instances = await instancesResponse.json();
  expect(services).toHaveLength(3);
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const server of services) {
    const row = sidebar.getByTestId(`tree-node-DDB-${server.node_id}`).locator(':scope > [role="treeitem"]');
    await expect(row.getByTitle('Healthy', { exact: true })).toBeVisible();
    await row.getByRole('button', { name: 'Expand', exact: true }).click();
  }
  for (const server of services) {
    const instance = instances.find((row: { rpc_endpoint: string }) => row.rpc_endpoint === (server.endpoint || server.rpc_url));
    expect(instance).toBeDefined();
    const tree = sidebar.getByTestId(`tree-node-DDB-${server.node_id}`);
    await expect(tree.getByRole('treeitem')).toHaveCount(instance.owned_dg_ids.length + 1);
    for (const id of instance.owned_dg_ids) {
      const physical = hardware.disk_groups.find((row: { disk_group_id: number }) => row.disk_group_id === id);
      expect(physical).toBeDefined();
      await expect(tree.getByTestId(`tree-node-CL-DG-${physical.node_id}-${id}`)).toBeVisible();
    }
  }
  for (const physical of hardware.disk_groups.filter((row: { node_id: number }) => row.node_id === 1)) {
    const group = sidebar.getByTestId(`tree-node-CL-DG-1-${physical.disk_group_id}`).locator(':scope > [role="treeitem"]');
    await group.getByRole('button', { name: 'Expand', exact: true }).click();
    for (const disk of physical.disks) await expect(sidebar.getByTestId(`tree-node-CL-D-1-${physical.disk_group_id}-${disk.disk_id}`)).toBeVisible();
  }
  const expand = page.getByRole('button', { name: /^N-1 Expand children\b/ });
  if (await expand.count()) await expand.click();
  else await expect(page.getByRole('button', { name: /^N-1 Collapse children\b/ })).toBeVisible();
  for (const physical of hardware.disk_groups.filter((row: { node_id: number }) => row.node_id === 1)) {
    const graph = page.locator(`.react-flow__node[data-id="CL-DG-1-${physical.disk_group_id}"]`);
    await expect(graph).toBeVisible();
    for (const disk of physical.disks) await expect(graph.getByTestId('compact-disk-stack')).toContainText(disk.disk_id.slice(0, 12));
  }
  await expect(sidebar.getByTestId('tree-node-S-1-1')).toHaveCount(0);
  await expect(page.locator('.react-flow__node[data-id="S-1-1"]')).toHaveCount(0);
}

// Baseline: 2.6s (2026-10-12).
test('native diagnostics: disk Inspector and Capacity selection retain two actual groups', async ({ page, request }) => {
  test.setTimeout(60_000);
  const response = await request.get('/api/hardware/capacity');
  expect(response.ok(), await response.text()).toBe(true);
  const hardware = await response.json();
  const groups = hardware.disk_groups.filter((group: { node_id: number }) => group.node_id === 1);
  expect(groups.map((group: { disk_group_id: number }) => group.disk_group_id).sort()).toEqual([1, 581]);
  expect(groups.every((group: { capacity_bytes: number; disk_count: number }) =>
    group.capacity_bytes === 4 * 1024 ** 4 && group.disk_count === 1)).toBe(true);
  const actual = groups.find((group: { disk_group_id: number }) => group.disk_group_id === 1).disks[0];
  await verifyOwnerProjection(page, request, hardware);
  await page.goto('/?domain=Capacity');
  const panel = page.getByTestId('capacity-summary').locator('..');
  await expect(page.getByRole('heading', { name: 'Capacity — Cluster', exact: true })).toBeVisible();
  await expect(page.getByTestId('capacity-summary').locator(':scope > div')
    .filter({ hasText: 'Total Capacity' }).getByText('8.0 TB', { exact: true })).toBeVisible();
  await expect(panel.getByText('Racks (1)', { exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: /R-1 / })).toBeVisible();
  await expect(panel.getByText('4 DG(s) · 3 node(s)', { exact: true })).toBeVisible();
  await expect(panel.getByText('Scanner', { exact: true })).toBeVisible();
  const first = await diskBranch(page, 1, 1);
  await diskBranch(page, 1, 581);
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const node of [1, 2, 3]) await expect(sidebar.getByTestId(`tree-node-DDB-${node}`)).toHaveCount(0);
  await sidebar.getByTestId('tree-node-R-1').locator(':scope > [role="treeitem"]')
    .getByRole('button', { name: /^R-1 / }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — Rack 1', exact: true })).toBeVisible();
  await expect(panel.getByText('Nodes in R-1 (3)', { exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: /^N-1 / })).toBeVisible();
  await expect(panel.getByText('2 DG(s)', { exact: true })).toBeVisible();
  await expect(panel.getByText('Scanner', { exact: true })).toHaveCount(0);
  await sidebar.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]')
    .getByRole('button', { name: 'N-1', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — Node 1', exact: true })).toBeVisible();
  await expect(panel.getByText('Disk-groups on N-1 (2)', { exact: true })).toBeVisible();
  for (const group of [1, 581]) await expect(panel.getByRole('button', { name: new RegExp(`DG-${group} `) })).toBeVisible();
  await expect(panel.getByText('1 disk(s)', { exact: true })).toHaveCount(2);
  await first.group.getByRole('button', { name: 'storage (DG-1)', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — DG-1', exact: true })).toBeVisible();
  await expect(panel.getByText('Disks in DG-1 (1)', { exact: true })).toBeVisible();
  const label = first.id.slice(0, 8) + '…';
  await expect(panel.getByText(label, { exact: true })).toBeVisible();
  await expect(panel.getByRole('button').filter({ hasText: label })).not.toHaveAttribute('title', /Usage unknown/);
  await first.disk.getByRole('button', { name: first.id.slice(0, 12) + '…', exact: true }).click();
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
  await expect(panel.getByText(actual.disk_id, { exact: true })).toBeVisible();
  await expect(page.getByTestId('disk-geometry')).toContainText(
    `${['BlockHdd', 'BlockSsd', 'ZoneSsd', 'SmrHdd'][actual.disk_type]} · Up · ${actual.zone_count} zones`);
  for (const name of ['Scan', 'Recalc', 'Compact', 'Rebuild', 'Down', 'Up']) {
    await expect(panel.getByRole('button', { name, exact: true })).toBeVisible();
  }
  await expect(panel.getByText('Recalc (DG-1)', { exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Run Recalc', exact: true })).toBeVisible();
  await expect(panel.getByText(`Zone grid (${actual.zone_count} zones)`, { exact: true })).toBeVisible();
  const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
  for (const name of ['Type', 'ID', 'Parent: rack_id', 'Parent: node_id', 'Parent: disk_group_id']) {
    await expect(inspector.getByText(name, { exact: true })).toBeVisible();
  }
  await expect(inspector.getByText('Name', { exact: true })).toHaveCount(0);
  await expect(inspector.getByText('Parent: disk_id', { exact: true })).toHaveCount(0);
});
