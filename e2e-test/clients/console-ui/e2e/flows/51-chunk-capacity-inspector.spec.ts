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
    const owner = instances.find((instance: { rpc_endpoint: string }) => instance.rpc_endpoint === (server.endpoint || server.rpc_url));
    if (owner?.owned_dg_ids.length) { await expect(row).toHaveAttribute('aria-expanded', 'false'); await row.getByRole('button', { name: 'Expand', exact: true }).click(); }
  }
  for (const server of services) {
    const instance = instances.find((row: { rpc_endpoint: string }) => row.rpc_endpoint === (server.endpoint || server.rpc_url));
    expect(instance).toBeDefined();
    for (const id of instance.owned_dg_ids) {
      const physical = hardware.disk_groups.find((row: { disk_group_id: number }) => row.disk_group_id === id);
      expect(physical).toBeDefined();
      await expect(sidebar.getByTestId(`tree-node-OWNER-${instance.instance_id}-CH-DG-${physical.node_id}-${id}`)).toBeVisible();
    }
  }
  for (const node of new Set<number>(hardware.disk_groups.map((row: { node_id: number }) => row.node_id))) {
    const storage = sidebar.getByTestId(`tree-node-STORAGE-${node}`);
    await storage.getByRole('button', { name: 'Expand', exact: true }).click();
    for (const physical of hardware.disk_groups.filter((row: { node_id: number }) => row.node_id === node)) {
      const group = storage.getByTestId(`tree-node-CH-DG-${node}-${physical.disk_group_id}`);
      await expect(group).toBeVisible();
      await group.getByRole('button', { name: 'Expand', exact: true }).click();
      for (const disk of physical.disks) await expect(group.getByTestId(`tree-node-CH-D-${node}-${physical.disk_group_id}-${disk.disk_id}`)).toBeVisible();
    }
  }
  const expand = page.getByRole('button', { name: 'Expand N-1 children', exact: true });
  if (await expand.count()) await expand.click();
  else await expect(page.getByRole('button', { name: 'Collapse N-1 children', exact: true })).toBeVisible();
  for (const physical of hardware.disk_groups.filter((row: { node_id: number }) => row.node_id === 1)) {
    const graph = page.locator(`.react-flow__node[data-id="CL-DG-1-${physical.disk_group_id}"]`);
    await expect(graph).toBeVisible();
    for (const disk of physical.disks) await expect(graph.getByTestId('compact-disk-stack')).toContainText(disk.disk_id.slice(0, 12));
  }
  await expect(sidebar.getByTestId('tree-node-S-1-1')).toHaveCount(0);
  await expect(page.locator('.react-flow__node[data-id="S-1-1"]')).toHaveCount(0);
}

// Baseline: 6.6s (2026-10-26).
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
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'datacenter', exact: true }).click();
  const panel = page.getByTestId('capacity-summary').locator('..');
  await expect(page.getByRole('heading', { name: 'Capacity — Cluster', exact: true })).toBeVisible();
  await expect(page.getByTestId('capacity-summary').locator(':scope > div')
    .filter({ hasText: 'Total Capacity' }).getByText('8.0 TB', { exact: true })).toBeVisible();
  await expect(panel).toContainText('Racks (1)');
  await expect(panel).toContainText('4 DG(s) · 3 node(s)');
  await expect(panel.getByRole('button', { name: /R-1 / })).toBeVisible();
  await expect(panel.getByText('Scanner', { exact: true })).toBeVisible();
  const first = await diskBranch(page, 1, 1);
  await diskBranch(page, 1, 581);
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const node of [1, 2, 3]) await expect(sidebar.getByTestId(`tree-node-DDB-${node}`)).toBeVisible();
  await sidebar.getByTestId('tree-node-R-1').locator(':scope > [role="treeitem"]')
    .getByRole('button', { name: /^R-1 / }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — Rack 1', exact: true })).toBeVisible();
  await expect(panel).toContainText('Nodes in R-1 (3)');
  const node = panel.getByRole('article', { name: 'N-1', exact: true });
  await expect(node).toContainText('8.0 TB');
  await node.getByRole('button', { name: 'Inspect N-1', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — Rack 1', exact: true })).toBeVisible();
  await expect(page.getByRole('complementary', { name: 'Entity inspector' })).toContainText('N-1');
  await page.getByRole('button', { name: 'Close inspector', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — Rack 1', exact: true })).toBeVisible();
  await expect(panel.getByText('Scanner', { exact: true })).toHaveCount(0);
  await sidebar.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]')
    .getByRole('button', { name: 'N-1', exact: true }).click();
  await sidebar.getByTestId('tree-node-STORAGE-1').getByRole('button', { name: 'Storage', exact: true }).click();
  const workspace = page.getByLabel('Node 1 storage', { exact: true });
  await expect(workspace).toBeVisible();
  await expect(page.getByRole('region', { name: 'Topology', exact: true })).toBeHidden();
  for (const group of [1, 581]) {
    const section = workspace.getByRole('region', { name: `Disk group ${group}`, exact: true });
    await expect(section).toBeVisible();
    await expect(section.getByRole('article', { name: /^Registered disk / })).toHaveCount(1);
  }
  await first.group.getByRole('button', { name: 'DG-1', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — DG-1', exact: true })).toBeVisible();
  await expect(panel.getByText('Disks in DG-1 (1)', { exact: true })).toBeVisible();
  const label = first.id.slice(0, 8) + '…';
  await expect(panel.getByText(label, { exact: true })).toBeVisible();
  await expect(panel.getByRole('article', { name: `Disk ${actual.disk_id}`, exact: true })).not.toHaveAttribute('title', /Usage unknown/);
  await expect(page.getByRole('region', { name: 'Topology', exact: true })).toBeHidden();
  await panel.getByRole('button', { name: `Open disk ${actual.disk_id}`, exact: true }).click();
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — DG-1', exact: true })).toBeVisible();
  await panel.getByRole('button', { name: label, exact: true }).click();
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
  await expect(panel.getByText(actual.disk_id, { exact: true })).toBeVisible();
  await expect(page.getByTestId('disk-geometry')).toContainText(
    `${['BlockHdd', 'BlockSsd', 'ZoneSsd', 'SmrHdd'][actual.disk_type]} · Up · ${actual.zone_count} zones`);
  for (const name of ['Scan', 'Recalc', 'Compact', 'Rebuild', 'Manage disk']) {
    await expect(panel.getByRole('button', { name, exact: true })).toBeVisible();
  }
  for (const name of ['Down', 'Up']) await expect(panel.getByRole('button', { name, exact: true })).toHaveCount(0);
  await expect(panel.getByText('Recalc (DG-1)', { exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Run Recalc', exact: true })).toBeVisible();
  await expect(panel.getByText(`Zone grid (${actual.zone_count} zones)`, { exact: true })).toBeVisible();
  expect(actual.zone_count).toBeGreaterThan(32);
  const zones = page.getByRole('group', { name: 'Disk zones', exact: true });
  await expect(zones.getByRole('button', { name: /^Zone \d+$/ })).toHaveCount(32);
  await zones.getByRole('button', { name: 'Next zones', exact: true }).click();
  await zones.getByRole('button', { name: 'Zone 32', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Zone 32 detail', exact: true })).toContainText('free blocks');
  await expect(sidebar.getByRole('button', { name: 'Zone 32', exact: true })).toHaveCount(0);
  const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
  for (const name of ['Type', 'ID', 'Parent: rack_id', 'Parent: node_id', 'Parent: disk_group_id']) {
    await expect(inspector.getByText(name, { exact: true })).toBeVisible();
  }
  await expect(inspector.getByText('Name', { exact: true })).toHaveCount(0);
  await expect(inspector.getByText('Parent: disk_id', { exact: true })).toHaveCount(0);
  const workspaceScroll = page.getByTestId('cluster-workspace');
  const savedScroll = await workspaceScroll.evaluate(element => { element.scrollTop = element.scrollHeight; return element.scrollTop; });
  expect(savedScroll).toBeGreaterThan(0);
  await page.getByRole('navigation', { name: 'Cluster scope' }).getByRole('button', { name: 'DG-1', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — DG-1', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
  await expect(zones.getByRole('button', { name: 'Zone 32', exact: true })).toHaveAttribute('aria-pressed', 'true');
  await expect(zones.getByRole('button', { name: 'Zone 0', exact: true })).toHaveCount(0);
  await expect(page.getByRole('region', { name: 'Zone 32 detail', exact: true })).toBeVisible();
  await expect.poll(() => workspaceScroll.evaluate(element => element.scrollTop), { intervals: [100] }).toBe(savedScroll);
});
