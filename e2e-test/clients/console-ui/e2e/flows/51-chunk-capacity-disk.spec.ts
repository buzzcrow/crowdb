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
