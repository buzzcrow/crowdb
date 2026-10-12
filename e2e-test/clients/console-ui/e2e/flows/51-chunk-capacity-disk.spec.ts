// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import type { Page, APIRequestContext } from '@playwright/test';
import { diskBranch, diskMenu } from '../fixtures/capacity';

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

// Baseline: 6.8s (2026-10-12).
test('native diagnostics: disk maintenance menus route to owners and retain every hardware badge', async ({ page, request }) => {
  await page.goto('/?domain=Capacity');
  const first = await diskBranch(page, 1, 1);
  for (const [name, button] of [['Compact Zones', 'Compact'], ['Rebuild Bitmap', 'Rebuild']]) {
    await diskMenu(page, first.disk, name);
    const dialog = page.getByRole('dialog', { name, exact: true });
    const zones = dialog.getByLabel(/zones/i);
    const submit = dialog.getByRole('button', { name: button, exact: true });
    await expect(zones).toHaveValue('all');
    await expect(submit).toBeEnabled();
    if (name === 'Compact Zones') {
      for (const [value, valid] of [['1-5,10', true], ['3', true], ['abc', false], ['1-5,abc', false], ['', true]] as const) {
        await zones.fill(value);
        if (valid) await expect(submit).toBeEnabled();
        else await expect(submit).toBeDisabled();
      }
    }
    await page.keyboard.press('Escape');
    await expect(dialog).toHaveCount(0);
  }

  const recalc = page.waitForResponse(response => new URL(response.url()).pathname === '/api/diskdb/recalc'
    && response.request().method() === 'POST');
  await diskMenu(page, first.disk, 'Recalc Usage');
  const recalculated = await recalc;
  expect(recalculated.ok(), await recalculated.text()).toBe(true);
  expect((await recalculated.json()).results.map((row: { disk_group_id: number }) => row.disk_group_id)).toEqual([1]);
  const second = await diskBranch(page, 2, 2);
  const scan = page.waitForResponse(response => new URL(response.url()).pathname === '/api/diskdb/scan'
    && response.request().method() === 'POST');
  await diskMenu(page, second.disk, 'Trigger Consistency Scan');
  const scanned = await scan;
  expect(scanned.ok(), await scanned.text()).toBe(true);

  const groupOffline = page.waitForResponse(response => new URL(response.url()).pathname === '/api/disk-groups/1/2/2/status'
    && response.request().method() === 'PUT');
  await diskMenu(page, second.group, 'Change Status');
  await page.getByRole('menuitem', { name: 'Offline', exact: true }).click();
  expect((await groupOffline).status()).toBe(204);
  const diskOffline = page.waitForResponse(response => new URL(response.url()).pathname === `/api/disks/${second.id}/status`
    && response.request().method() === 'PUT');
  await diskMenu(page, second.disk, 'Change Status');
  await page.getByRole('menuitem', { name: 'Offline', exact: true }).click();
  expect((await diskOffline).status()).toBe(204);

  const paused: string[] = [];
  try {
    // Another allocator can take over after an owner stops. Keep every
    // allocator paused while observing deliberately selected hardware states.
    for (const node of [1, 2, 3]) {
      const id = `diskdb-${node}`;
      const response = await request.post(`/api/services/${id}/stop`, { data: {} });
      expect(response.ok(), await response.text()).toBe(true);
      paused.push(id);
    }
    await hardwareBadges(page, request, first.id);
  } finally {
    for (const id of paused) {
      const resumed = await request.post(`/api/services/${id}/restart`, { data: {} });
      expect(resumed.ok(), await resumed.text()).toBe(true);
    }
  }
});


async function hardwareBadges(page: Page, request: APIRequestContext, firstId: string) {
  const statuses = ['Init', 'Maintenance', 'Suspect', 'Missing', 'Bad', 'Offline'];
  const records = [{ group: 1, node: 1, disk: firstId, status: 'Up' }];
  for (const [index, status] of statuses.entries()) {
    const group = 554 + index;
    const id = `${group.toString(16).padStart(16, '0')}-0000000000000001`;
    const createdGroup = await request.post('/api/nodes/1/disk-groups', {
      data: { id: group, store_id: 1, group_id: 1 },
    });
    expect(createdGroup.status(), await createdGroup.text()).toBe(201);
    const createdDisk = await request.post(`/api/nodes/1/disk-groups/${group}/disks`, {
      data: { disk_id: id, disk_type: 'Hdd', capacity_bytes: 1024 ** 3,
        zone_size_bytes: 1024 ** 2, unit_size_bytes: 128 * 1024 },
    });
    expect(createdDisk.status(), await createdDisk.text()).toBe(201);
    const changedGroup = await request.put(`/api/disk-groups/1/1/${group}/status`, { data: { status } });
    expect(changedGroup.status(), await changedGroup.text()).toBe(204);
    const changedDisk = await request.put(`/api/disks/${id}/status`, { data: { status } });
    expect(changedDisk.status(), await changedDisk.text()).toBe(204);
    records.push({ group, node: 1, disk: id, status });
  }
  const empty = await request.post('/api/nodes/1/disk-groups', {
    data: { id: 548, store_id: 1, group_id: 1 },
  });
  expect(empty.status(), await empty.text()).toBe(201);
  await page.reload();
  for (const record of records) {
    const branch = await diskBranch(page, record.node, record.group);
    expect(branch.id).toBe(record.disk);
    await expect(branch.group.getByTitle(record.status, { exact: true })).toBeVisible();
    await expect(branch.disk.getByTitle(record.status, { exact: true })).toBeVisible();
  }
  const emptyRow = page.getByRole('complementary', { name: 'Cluster tree sidebar' })
    .getByTestId('tree-node-CH-DG-1-548').locator(':scope > [role="treeitem"]');
  await expect(emptyRow).toBeVisible();
  const changed = page.waitForResponse(response => new URL(response.url()).pathname === '/api/disk-groups/1/1/548/status'
    && response.request().method() === 'PUT');
  await diskMenu(page, emptyRow, 'Change Status');
  await page.getByRole('menuitem', { name: 'Offline', exact: true }).click();
  expect((await changed).status()).toBe(204);
}
