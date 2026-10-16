// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: Capacity retains hardware and marks interrupted DDB usage unknown', async ({ page, request }) => {
  await page.goto('/?domain=Capacity');
  const totals = page.getByTestId('capacity-summary');
  const panel = totals.locator('..');
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(0);
  expect((await request.post('/api/nodes/1/diskdb/stop', { data: {} })).ok()).toBe(true);
  try {
    await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await expect(page.getByRole('alert').filter({ hasText: 'Capacity observation unavailable' })).toBeVisible();
    await panel.getByRole('button', { name: /R-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await panel.getByRole('table', { name: 'Node capacity' }).getByRole('button', { name: 'N-1', exact: true }).click();
    const workspace = page.getByLabel('Node 1 workspace', { exact: true });
    const group = workspace.getByRole('region', { name: 'Disk group 1', exact: true });
    await expect(group.getByRole('article', { name: /^Registered disk / })).toContainText('Usage unknown');
    await expect(group).toContainText('80.0 GB');
    await group.getByRole('button', { name: /DG-1$/ }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    const disk = panel.getByRole('button', { name: /00000000…/ });
    await expect(panel.getByRole('article', { name: /^Disk / })).toHaveAttribute('title', /Usage unknown/);
    await disk.click();
    await expect(page.getByTestId('disk-geometry')).toContainText(/80 zones.*Usage unknown/);
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await expect(totals).not.toContainText('100% free');
  } finally {
    expect((await request.post('/api/nodes/1/diskdb/restart', { data: {} })).ok()).toBe(true);
  }
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(0);
  await expect(page.getByTestId('disk-geometry')).not.toContainText('Usage unknown');
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
});

// Baseline: 2.3s (2026-10-12).
test('native diagnostics: scanner and scoped recalc show actual completed observations', async ({ page, request }) => {
  await page.goto('/?domain=Capacity');
  const panel = page.getByTestId('capacity-summary').locator('..');
  await expect(page.getByRole('heading', { name: 'Capacity — Cluster', exact: true })).toBeVisible();
  await expect(panel.getByText('3 instance(s)', { exact: true })).toBeVisible();
  for (const label of ['Total Capacity', 'Busy', 'Free']) {
    await expect(page.getByTestId('capacity-summary').getByText(label, { exact: true })).toBeVisible();
  }
  await expect(panel.getByRole('button', { name: 'Refresh', exact: true })).toBeVisible();
  await expect(panel.getByText('Scan status unavailable.', { exact: true })).toBeVisible();
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  const datacenter = sidebar.getByTestId('tree-node-DC-datacenter').locator(':scope > [role="treeitem"]');
  await expect(datacenter).toBeVisible();
  await expect(datacenter).toHaveText(/datacenter/);
  await datacenter.getByText('datacenter', { exact: true }).click();
  const clusterInspector = page.getByRole('complementary', { name: 'Entity inspector' });
  const field = (name: string) => clusterInspector.locator('dl > div')
    .filter({ has: page.locator('dt', { hasText: new RegExp(`^${name}$`) }) }).locator('dd');
  await expect(field('Type')).toHaveText('Datacenter');
  await expect(field('Rack Count')).toHaveText('1');
  for (const label of ['Total Capacity', 'Used', 'Free']) {
    await expect(clusterInspector.getByText(label, { exact: true })).toBeVisible();
  }
  const response = await request.get('/api/hardware/capacity');
  expect(response.ok(), await response.text()).toBe(true);
  const hardware = await response.json();
  expect(hardware.disk_groups.map((group: { disk_group_id: number }) => group.disk_group_id).sort()).toEqual([1, 2, 3]);
  const bytes = hardware.datacenter_capacity_bytes;
  expect(bytes).toBeGreaterThan(0);
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const unit = Math.floor(Math.log(bytes) / Math.log(1024));
  await expect(field('Total Capacity')).toHaveText(`${(bytes / 1024 ** unit).toFixed(1)} ${units[unit]}`);
  await expect(panel.getByText('No scan has been run yet.', { exact: true })).toBeVisible();
  const scanResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/diskdb/scan' && response.request().method() === 'POST');
  await panel.getByRole('button', { name: 'Run Scan', exact: true }).click();
  const scan = await scanResponse;
  expect(scan.ok(), await scan.text()).toBe(true);
  await expect(panel.getByText('Zones scanned', { exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Run Scan', exact: true })).toBeEnabled();
  await panel.getByRole('button', { name: /R-1 / }).click();
  await expect(panel.getByText('Scanner', { exact: true })).toHaveCount(0);
  await panel.getByRole('table', { name: 'Node capacity' }).getByRole('button', { name: 'N-1', exact: true }).click();
  await page.getByLabel('Node 1 workspace', { exact: true }).getByRole('region', { name: 'Disk group 1', exact: true }).getByRole('button', { name: /DG-1$/ }).click();
  await expect(page.getByRole('heading', { name: 'Capacity — DG-1', exact: true })).toBeVisible();
  await expect(panel.getByRole('button', { name: /00000000…/ })).toBeVisible();
  await panel.getByRole('button', { name: /00000000…/ }).click();
  await expect(page.getByTestId('disk-geometry')).toContainText('80 zones');
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
  await expect(panel.getByText('Recalc (DG-1)', { exact: true })).toBeVisible();
  const recalcResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/diskdb/recalc' && response.request().method() === 'POST');
  await panel.getByRole('button', { name: 'Run Recalc', exact: true }).click();
  const recalc = await recalcResponse;
  expect(recalc.ok(), await recalc.text()).toBe(true);
  const observation = await recalc.json();
  expect(observation.results.map((row: { disk_group_id: number }) => row.disk_group_id)).toEqual([1]);
  expect(observation.results.flatMap((row: { zones: { drift_detected: boolean }[] }) => row.zones).every((zone: { drift_detected: boolean }) => !zone.drift_detected)).toBe(true);
  await expect(panel.getByText('No drift detected. All zones match.', { exact: true })).toBeVisible();
  const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
  await expect(inspector.getByText('Name', { exact: true })).toHaveCount(0);
  await expect(inspector.getByText('Parent: disk_id', { exact: true })).toHaveCount(0);
  for (const parent of ['rack_id', 'node_id', 'disk_group_id']) await expect(inspector.getByText(`Parent: ${parent}`, { exact: true })).toBeVisible();
});
