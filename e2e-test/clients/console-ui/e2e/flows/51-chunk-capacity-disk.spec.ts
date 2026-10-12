// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: Capacity navigation fetches only opened storage branches', async ({ page }) => {
  const groups: number[] = []; const disks: string[] = []; const usage: string[] = [];
  page.on('request', request => {
    const url = new URL(request.url());
    const node = url.pathname.match(/^\/api\/nodes\/(\d+)\/disk-groups$/);
    if (node) groups.push(Number(node[1]));
    if (/^\/api\/nodes\/\d+\/disk-groups\/\d+\/disks$/.test(url.pathname)) disks.push(url.pathname);
    if (url.pathname === '/api/diskdb/usage') usage.push(url.search);
  });
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(sidebar.getByRole('button', { name: 'N-1', exact: true })).toBeVisible();
  expect(groups).toEqual([]); expect(disks).toEqual([]); expect(usage).toEqual([]);
  await page.getByTestId('domain-capacity').click();
  await expect(page.getByTestId('capacity-summary')).toBeVisible();
  expect(groups).toEqual([]); expect(disks).toEqual([]);
  await sidebar.getByTestId('tree-node-N-1').getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(sidebar.getByRole('button', { name: 'storage (DG-1)', exact: true })).toBeVisible();
  expect(groups).toEqual([1]); expect(disks).toEqual([]);
  await sidebar.getByRole('button', { name: 'storage (DG-1)', exact: true }).click();
  await expect.poll(() => usage.some(query => query.includes('dg=1')), { intervals: [100] }).toBe(true);
  await expect.poll(() => disks.length, { intervals: [100] }).toBe(1);
  expect(disks[0]).toBe('/api/nodes/1/disk-groups/1/disks');
  expect(groups.every(node => node === 1)).toBe(true);
});

// Baseline: 1.2s (2026-10-12).
test('native diagnostics: hardware capacity aggregates disks and dashed status updates', async ({ page, request }) => {
  const group = await request.post('/api/nodes/1/disk-groups', {
    data: { id: 591, name: 'test-dg-hwcap', store_id: 1, group_id: 1 },
  });
  expect(group.status(), await group.text()).toBe(201);
  const ids = ['0000000000000591-0000000000000001', '0000000000000591-0000000000000002'];
  const diskCapacity = 4096 * 1000;
  const created = await request.post('/api/nodes/1/disk-groups/591/disks/batch', {
    data: { disks: ids.map(disk_id => ({
      disk_id, disk_type: 'Hdd', capacity_bytes: diskCapacity,
      unit_size_bytes: 4096, zone_size_bytes: 1024 * 1024,
    })) },
  });
  expect(created.status(), await created.text()).toBe(201);
  const response = await request.get('/api/hardware/capacity');
  expect(response.ok(), await response.text()).toBe(true);
  const summary = await response.json();
  const dg = summary.disk_groups.find((row: { disk_group_id: number }) => row.disk_group_id === 591);
  expect(dg).toMatchObject({ capacity_bytes: diskCapacity * 2, disk_count: 2, rack_id: 1, node_id: 1 });
  expect(dg.disks).toHaveLength(2);
  for (const disk of dg.disks) {
    expect(disk).toMatchObject({ capacity_bytes: diskCapacity, unit_size_bytes: 4096 });
  }
  const rack = summary.racks.find((row: { rack_id: number }) => row.rack_id === 1);
  const node = summary.nodes.find((row: { node_id: number }) => row.node_id === 1);
  expect(summary.datacenter_capacity_bytes).toBeGreaterThanOrEqual(diskCapacity * 2);
  expect(rack.capacity_bytes).toBeGreaterThanOrEqual(diskCapacity * 2);
  expect(node.capacity_bytes).toBeGreaterThanOrEqual(diskCapacity * 2);
  const sum = (rows: { capacity_bytes: number }[]) => rows.reduce((total, row) => total + row.capacity_bytes, 0);
  expect(sum(summary.racks)).toBe(summary.datacenter_capacity_bytes);
  expect(sum(summary.nodes.filter((row: { rack_id: number }) => row.rack_id === 1))).toBe(rack.capacity_bytes);
  expect(sum(summary.disk_groups.filter((row: { node_id: number }) => row.node_id === 1))).toBe(node.capacity_bytes);

  await page.goto('/?domain=Capacity');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await sidebar.getByTestId('tree-node-N-1').getByRole('button', { name: 'Expand', exact: true }).click();
  const item = sidebar.getByRole('button', { name: 'test-dg-hwcap (DG-591)', exact: true });
  await expect(item).toBeVisible();
  await item.click();
  const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
  await expect(inspector.getByText('Total Capacity', { exact: true })).toBeVisible();
  await expect(inspector.getByText('Disks (2)', { exact: true })).toBeVisible();
  for (const status of ['Suspect', 'Up', 'Offline']) {
    const updated = await request.put(`/api/disks/${ids[0]}/status`, { data: { status } });
    expect(updated.status(), await updated.text()).toBe(204);
  }
});
